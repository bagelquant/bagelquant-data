"""DataItem management on the shared Raw version and persistence engine."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from contextlib import closing, contextmanager, nullcontext
from concurrent.futures import CancelledError, ThreadPoolExecutor
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any, cast, overload
from threading import Lock, get_ident
from uuid import uuid4

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.types import DateLike
from bagelquant_data.execution import ExecutionOptions
from bagelquant_data.pipeline.versions import VERSION_FIELDS, commit_versions
from bagelquant_data.query.raw import RawQueryService, _date_value
from bagelquant_data.transforms import Cast, Filter, MapValues, Select, apply_transforms, scalar_dtype

from .types import BuildContext, DataItemSpec, ItemBuildReport, ItemCompletionAttestation, ItemInput, ItemPublication, Producer, ProducerUpdatePolicy, RawInput, input_payload, spec_from_payload, spec_payload
from .pit import _comparison_frame, _same_ordered_rows, general_input_snapshot, iter_computed_versions
from .completion import output_check_bound, pending_dates, verified_completion_digest, save_completion as _save_completion

if TYPE_CHECKING:
    from bagelquant_data.management.lake import DataLake
    from bagelquant_data.inputs import FrozenInputReceipt


def _json(value: Any) -> str:
    from bagelquant_data.storage.immutable_metadata import canonical
    return canonical(value)


def _definition_hash(spec: DataItemSpec) -> str:
    """Bind all numerical declaration fields, retaining description as prose."""
    definition = {key: value for key, value in spec_payload(spec).items() if key != "description"}
    return hashlib.sha256(_json(definition).encode()).hexdigest()


def _pointwise_scope_extension(previous, requests, *, policy, completed_start, completed_end,
                               requested_start, requested_end) -> bool:
    """Prove a widened physical scope preserves every completed pointwise input."""
    if policy is None or policy.historical_impact != "same_date" or set(previous) != set(requests):
        return False
    for alias, before in previous.items():
        after = requests[alias]
        old, new = input_payload(before), input_payload(after)
        if {key: value for key, value in old.items() if key not in {"start", "end"}} != {
                key: value for key, value in new.items() if key not in {"start", "end"}}:
            return False
        if old == new:
            continue
        if (before.start is not None and _date_value(before.start) > completed_start
                or before.end is not None and _date_value(before.end) < completed_end
                or after.start is not None and _date_value(after.start) > requested_start
                or after.end is not None and _date_value(after.end) < requested_end):
            return False
        if (after.start is not None and (before.start is None or _date_value(after.start) > _date_value(before.start))
                or after.end is not None and (before.end is None or _date_value(after.end) < _date_value(before.end))):
            return False
    return True


def _baseline_publication_groups(frame: pl.DataFrame, rows: int, byte_limit: int):
    """Keep independent baseline coordinates in causal, bounded batches."""
    for group in frame.sort("version_available_date", "time", "asset_id").iter_slices(rows):
        pending = [group]
        while pending:
            part = pending.pop()
            if part.height > 1 and part.estimated_size() > byte_limit:
                middle = part.height // 2
                pending.extend((part.slice(middle), part.head(middle)))
                continue
            yield _date_value(cast(DateLike, part["version_available_date"].max())), part


@dataclass(slots=True)
class _PublicationVerification:
    owner: ItemAPI
    receipt_id: str
    digest: str
    active: bool = True
    thread_id: int = field(default_factory=get_ident)
    timing_cache_entries: int = 0
    timing_buffer_bytes: int | None = None
    baseline_dates: dict[date, bool] = field(default_factory=dict)
    sparse_group: _SparseGroup | None = None


@dataclass(slots=True)
class _SparseGroup:
    owner: ItemAPI
    connection: sqlite3.Connection
    receipt_id: str
    digest: str
    leases: dict[str, str]
    thread_id: int = field(default_factory=get_ident)
    active: bool = True


class ItemPublisher:
    """Serial output groups in one verified immutable-input operation."""

    def __init__(self, owner: ItemAPI, receipt: FrozenInputReceipt | None,
                 verification: _PublicationVerification,
                 cancelled: Callable[[], bool] | None) -> None:
        self._owner, self._receipt = owner, receipt
        self._verification, self._cancelled = verification, cancelled
        self._lock = Lock()

    @property
    def lake(self) -> DataLake:
        return self._owner._lake

    @property
    def input_receipt(self) -> FrozenInputReceipt | None:
        return self._receipt

    @property
    def active(self) -> bool:
        return self._verification.active

    def publish(self, publications: Sequence[ItemPublication]) -> dict[str, ItemBuildReport | None]:
        if not self._verification.active:
            raise RuntimeError("DataItem publication operation has closed")
        if get_ident() != self._verification.thread_id:
            raise RuntimeError("DataItem publication requires its owning thread")
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("DataItem publication groups must be serial")
        try:
            return self._owner._publish(publications, input_receipt=self._receipt,
                cancelled=self._cancelled, _verification=self._verification)
        except BaseException:
            self._verification.active = False
            self._verification.baseline_dates.clear()
            raise
        finally:
            self._lock.release()


class ItemAPI:
    """Typed item definitions, dependency builds and causal long-table reads."""

    def _completed_rows(self, where: str, parameters) -> list[dict[str, Any]]:
        """Read source-bound complete publications, excluding row-only ingests."""
        with self._store.connect() as db:
            if not db.execute("select 1 from sqlite_master where name='item_completion_proofs'").fetchone():
                return []
            rows = db.execute("select b.*,p.digest as completion_digest from item_builds b "
                "join item_completion_proofs p on p.build_id=b.id where " + where, parameters).fetchall()
            result = []
            for row in rows:
                body = {key: value for key, value in dict(row).items() if key != "completion_digest"}
                verified_completion_digest(db, row["id"])
                result.append(body)
        return result

    def index_output_reuse(self, name: str, *, end: DateLike, config: ExecutionOptions | None = None,
                           check_canceled: Callable[[], None] = lambda: None,
                           progress: Callable[[dict[str, Any]], None] = lambda _: None) -> dict[str, Any]:
        """Explicitly index later output impact against the original completed result.

        This maintenance audits bounded original output batches and compares
        all numerical/availability channels, excluding creator provenance.
        Ordinary update plans only read the resulting batch-bound records.
        It never runs a producer, alters completion or overrides input impact.
        """
        from .output_reuse import index_output_reuse
        from bagelquant_data.inputs import current_input_boundary, current_input_check_boundary
        self._store.ensure_writable()
        if (self._max_commit is not None or current_input_boundary(self._store.data_meta_path) != (None, None)
                or current_input_check_boundary(self._store.data_meta_path) is not None):
            raise RuntimeError("Output reuse maintenance requires an unfrozen writer")
        run_id = uuid4().hex
        with self._store.dataset_writer("items", name, run_id):
            return index_output_reuse(self, name, end=end, config=config or ExecutionOptions(),
                check_canceled=check_canceled, progress=progress, run_id=run_id)

    def index_completion(self, attestation: ItemCompletionAttestation) -> dict[str, Any]:
        """Explicitly admit an original completion report without recalculation.

        The caller owns the report reference and whole-range/pending declarations.
        This is maintenance, never an automatic currentness or update fallback.
        Original build rows, result boundaries and frozen receipts remain intact;
        later publications still participate in ordinary stale-result checks.
        Sparse no-op reuse additionally requires the original output receipt,
        whose captured check ceiling remains the authority's immutable fence.
        """
        self._store.ensure_writable()
        if not isinstance(attestation, ItemCompletionAttestation):
            raise TypeError("Completion maintenance requires ItemCompletionAttestation")
        receipt = self._lake.inputs.get(attestation.frozen_receipt_id)
        if (receipt.digest != attestation.frozen_receipt_digest
                or receipt.dependency_digest != attestation.dependency_digest
                or receipt.max_commit != attestation.input_commit
                or receipt.information_cutoff != attestation.information_cutoff):
            raise ValueError("Completion attestation differs from original frozen input evidence")
        declaration = asdict(attestation)
        output_binding = None
        if attestation.output_receipt_id is not None:
            output = self._lake.inputs.get(attestation.output_receipt_id)
            if output.digest != attestation.output_receipt_digest:
                raise ValueError("Completion original output receipt digest differs")
            if output.max_check_id is None:
                raise ValueError("Completion original output receipt check boundary is unknown")
            aliases = [key for key, value in output.requests.items() if isinstance(value, ItemInput)
                       and value.name == attestation.name and str(value.start) == str(attestation.start)
                       and str(value.end) == str(attestation.end) and value.view == "snapshot"]
            if len(aliases) != 1:
                raise ValueError("Completion original output receipt scope differs")
            captured = output.evidence[aliases[0]]["empty_item_build"]
            expected_output = {"definition_hash": attestation.definition_hash,
                "dependency_digest": attestation.dependency_digest, "start_date": str(attestation.start),
                "end_date": str(attestation.end), "frozen_receipt_id": attestation.frozen_receipt_id,
                "input_commit": attestation.input_commit, "result_commit": attestation.result_commit}
            if captured != expected_output:
                raise ValueError("Completion original output receipt closure differs")
            output_binding = {"id": output.receipt_id, "digest": output.digest,
                              "max_check_id": output.max_check_id}
        else:
            # An absent output binding remains unknown for sparse reuse. Keep
            # the original attestation declaration unchanged; never absorb a
            # later timing check during explicit legacy maintenance.
            declaration.pop("output_receipt_id")
            declaration.pop("output_receipt_digest")
        payload = _json(declaration)
        digest = hashlib.sha256(payload.encode()).hexdigest()
        with self._store.connect() as db:
            db.execute("begin immediate")
            original = db.execute("select * from item_builds where id=?", (attestation.build_id,)).fetchone()
            expected = {"name": attestation.name, "definition_hash": attestation.definition_hash,
                "start_date": str(attestation.start), "end_date": str(attestation.end),
                "input_commit": attestation.input_commit, "result_commit": attestation.result_commit,
                "dependency_digest": attestation.dependency_digest, "frozen_receipt_id": attestation.frozen_receipt_id,
                "status": "success"}
            if original is None or any(original[name] != value for name, value in expected.items()):
                raise ValueError("Completion attestation differs from original build record")
            if attestation.result_commit is not None and db.execute("select 1 from version_commits "
                    "where seq=? and source='items' and dataset=? and status='committed'",
                    (attestation.result_commit, attestation.name)).fetchone() is None:
                raise ValueError("Completion result commit is not an original committed item publication")
            db.execute("create table if not exists item_completion_attestations("
                "build_id integer primary key,payload_json text not null,digest text not null)")
            saved = db.execute("select digest from item_completion_attestations where build_id=?",
                (attestation.build_id,)).fetchone()
            if saved is not None:
                if saved[0] != digest:
                    raise ValueError("Original completion attestation is immutable")
                pending_dates(db, attestation.build_id)
            else:
                if db.execute("select 1 from sqlite_master where name='item_completion_proofs'").fetchone() and db.execute(
                        "select 1 from item_completion_proofs where build_id=?", (attestation.build_id,)).fetchone():
                    raise ValueError("Original build already has native completion authority")
                _save_completion(db, attestation.build_id,
                    pending=[(str(day), str(available)) for day, available in attestation.pending_coordinates],
                    authority={"kind": "attested", "attestation_digest": digest,
                        "output_check_bound": receipt.max_check_id if output_binding is None else output_binding["max_check_id"],
                        **({} if output_binding is None else {"output_receipt": output_binding})})
                db.execute("insert into item_completion_attestations values(?,?,?)", (attestation.build_id, payload, digest))
        return {"build_id": attestation.build_id, "attestation_digest": digest, "report_id": attestation.report_id}

    def index_completion_output(self, build_id: int, *, output_receipt_id: str,
                                output_receipt_digest: str, report_id: str, report_digest: str) -> dict[str, Any]:
        """Explicitly bind a legacy native completion to its original output.

        The caller owns the original publication report. Existing native build,
        authority and pending records remain immutable; this records only the
        original frozen output/check ceiling. Ordinary checks never invoke it.
        """
        self._store.ensure_writable()
        if (type(build_id) is not int or build_id < 1 or not isinstance(report_id, str) or not 0 < len(report_id) <= 2048
                or not isinstance(report_digest, str) or len(report_digest) != 64
                or any(char not in "0123456789abcdef" for char in report_digest)):
            raise ValueError("Original completion output needs an explicit report reference and digest")
        output = self._lake.inputs.get(output_receipt_id)
        if output.digest != output_receipt_digest or output.max_check_id is None:
            raise ValueError("Original completion output receipt/check identity differs")
        with self._store.connect() as db:
            db.execute("begin immediate")
            digest = verified_completion_digest(db, build_id)
            build = db.execute("select * from item_builds where id=?", (build_id,)).fetchone()
            native = db.execute("select payload_json,digest from item_completion_authorities where build_id=?",
                (build_id,)).fetchone()
            authority = json.loads(native[0])
            if authority["kind"] != "native" or "output_check_bound" in authority:
                raise ValueError("Completion output maintenance requires an original native authority without a check ceiling")
            aliases = [alias for alias, request in output.requests.items() if isinstance(request, ItemInput)
                and request.name == build["name"] and request.view == "snapshot"
                and request.start is not None and request.end is not None
                and str(request.start) <= build["start_date"] <= build["end_date"] <= str(request.end)]
            if len(aliases) != 1 or output.max_commit < (build["result_commit"] or 0):
                raise ValueError("Original completion output does not cover the native range")
            evidence = output.evidence[aliases[0]]
            if evidence["definition_hash"] != build["definition_hash"]:
                raise ValueError("Original completion output definition differs")
            source = self._lake.inputs.get(build["frozen_receipt_id"])
            if (source.information_cutoff is None or output.information_cutoff is None
                    or source.information_cutoff > output.information_cutoff
                    or source.max_check_id is None or source.max_check_id > output.max_check_id
                    or source.max_commit > output.max_commit
                    or source.max_commit != build["input_commit"]
                    or source.dependency_digest != build["dependency_digest"]):
                raise ValueError("Original completion output source boundary differs")
            batches = []
            for batch in evidence["batches"]:
                if not batch["row_count"]:
                    continue
                lower, upper = batch.get("min_observation"), batch.get("max_observation")
                if lower is None or upper is None:
                    raise ValueError("Original completion output batch range is unknown")
                if str(upper) < build["start_date"] or str(lower) > build["end_date"]:
                    continue
                if (not build["start_date"] <= str(lower) <= str(upper) <= build["end_date"]
                        or batch["commit_seq"] > (build["result_commit"] or 0)
                        or batch["input_receipt_id"] != source.receipt_id
                        or evidence["parent_receipts"].get(source.receipt_id) != source.digest):
                    raise ValueError("Original completion output batch source/range differs")
                batches.append(batch)
            empty_ids = {check["id"] for check in evidence["empty_checks"] if check["row_count"] == 0}
            for check in [*evidence["audit_checks"], *evidence["empty_checks"]]:
                queries = json.loads(check["request_json"])
                scopes = [query["item_range"] for query in queries if "item_range" in query]
                if not scopes or len(scopes) != len(queries):
                    raise ValueError("Original completion output check scope is unknown")
                for scope in scopes:
                    if scope["end"] < build["start_date"] or scope["start"] > build["end_date"]:
                        continue
                    if (not build["start_date"] <= scope["start"] <= scope["end"] <= build["end_date"]
                            or check["input_receipt_id"] != source.receipt_id
                            or evidence["parent_receipts"].get(source.receipt_id) != source.digest
                            or check["visible_commit"] is None and check["id"] not in empty_ids
                            or check["visible_commit"] is not None
                            and check["visible_commit"] > (build["result_commit"] or 0)):
                        raise ValueError("Original completion output check source/range differs")
            proof = evidence["empty_item_build"]
            exact_empty = proof is not None and all(build[key] == value for key, value in proof.items())
            if not exact_empty and not any(batch["commit_seq"] == build["result_commit"] for batch in batches):
                raise ValueError("Original completion output lacks captured native batch/range evidence")
            pending_dates(db, build_id)
            body = {"build_id": build_id, "completion_digest": digest,
                "native_authority_digest": native[1], "pending_digest": authority["pending_digest"],
                "output_receipt_id": output.receipt_id, "output_receipt_digest": output.digest,
                "output_check_bound": output.max_check_id, "report_id": report_id, "report_digest": report_digest}
            payload = _json(body)
            db.execute("create table if not exists item_completion_output_receipts("
                "build_id integer primary key,payload_json text not null,digest text not null)")
            saved = db.execute("select payload_json from item_completion_output_receipts where build_id=?", (build_id,)).fetchone()
            if saved is not None and saved[0] != payload:
                raise ValueError("Original completion output index is immutable")
            if saved is None:
                db.execute("insert into item_completion_output_receipts values(?,?,?)",
                    (build_id, payload, hashlib.sha256(payload.encode()).hexdigest()))
            assert output_check_bound(db, build_id, authority) == output.max_check_id
        return body

    def __init__(self, lake: DataLake) -> None:
        self._lake = lake
        self._store = lake._data_meta
        self._parquet = lake._parquet
        self._query = RawQueryService(self._parquet, self._store)
        from bagelquant_data.inputs import current_input_boundary, current_input_check_boundary
        self._max_commit, self._as_of = current_input_boundary(self._store.data_meta_path)
        self._max_check_id = current_input_check_boundary(self._store.data_meta_path)
        self._producers: dict[tuple[str, str], Producer] = {}
        self._update_policies: dict[tuple[str, str], ProducerUpdatePolicy] = {}
        if not self._store.read_only:
            with self._store.connect() as db:
                db.executescript("""
                    create table if not exists item_definitions (
                        name text primary key, spec_json text not null,
                        spec_hash text not null, active integer not null default 1
                    );
                    create table if not exists item_dependencies (
                        item_name text not null, kind text not null,
                        source text not null, dataset text not null,
                        primary key(item_name,kind,source,dataset)
                    );
                    create table if not exists item_builds (
                        id integer primary key, name text not null,
                        definition_hash text not null, dependency_digest text not null,
                        start_date text not null, end_date text not null,
                        input_commit integer not null, result_commit integer,
                        status text not null, created_at text not null,
                        frozen_receipt_id text not null
                    );
                    create table if not exists item_initializations (
                        name text primary key, definition_hash text not null,
                        start_date text not null, end_date text not null,
                        status text not null
                    );
                """)

    def register(self, spec: DataItemSpec, *, category_id: str | None = None,
                 expected_definition_hash: str | None = None) -> None:
        """Register a declaration, optionally replacing only the captured active hash.

        The conditional form fences the check and declaration in one transaction
        under the item writer lease. It never substitutes for original-bound
        recovery evidence and does not make multiple item declarations atomic.
        """
        if expected_definition_hash is not None and (
                not isinstance(expected_definition_hash, str) or not expected_definition_hash):
            raise ValueError("expected_definition_hash must be a nonempty string")
        with self._store.dataset_writer("items", spec.name, uuid4().hex):
            if expected_definition_hash is None:
                self._register(spec, category_id=category_id)
            else:
                with self._store.atomic_transaction() as db:
                    current = db.execute("select spec_hash from item_definitions where name=? and active=1",
                        (spec.name,)).fetchone()
                    if current is None or current[0] != expected_definition_hash:
                        raise RuntimeError("DataItem definition changed before conditional registration")
                    self._register(spec, category_id=category_id)

    def _register(self, spec: DataItemSpec, *, category_id: str | None = None) -> None:
        self._store.ensure_writable()
        if not isinstance(spec, DataItemSpec):
            raise TypeError("spec must be a DataItemSpec")
        previous = self._store._rows("select spec_json from item_definitions where name=?", (spec.name,))
        if previous and self._store.manifest("items", spec.name) and spec_from_payload(json.loads(previous[0]["spec_json"])).value_dtype != spec.value_dtype:
            raise ValueError("A committed DataItem's scalar type is immutable; register a new item identity")
        payload = _json(spec_payload(spec))
        digest = _definition_hash(spec)
        physical = self._physical_spec(spec)
        self._store.upsert_source("items", "data_items", configured=True)
        with self._store.connect() as db:
            if not db.in_transaction:
                db.execute("begin immediate")
            # Verify dependencies and cycles against the prospective graph before
            # changing either the item definition or the physical dataset contract.
            for dependency in spec.inputs:
                if isinstance(dependency, ItemInput):
                    if dependency.name == spec.name or self._reaches(dependency.name, spec.name):
                        raise ValueError("DataItem dependency cycle")
                    self.get(dependency.name)
                elif self._store.get_dataset(dependency.source, dependency.dataset) is None:
                    raise ValueError(f"Unknown Raw dependency: {dependency.source}/{dependency.dataset}")
            db.execute(
                "insert into item_definitions values(?,?,?,1) "
                "on conflict(name) do update set spec_json=excluded.spec_json,spec_hash=excluded.spec_hash,active=1",
                (spec.name, payload, digest),
            )
            db.execute("delete from item_dependencies where item_name=?", (spec.name,))
            db.executemany(
                "insert into item_dependencies values(?,?,?,?)",
                [(spec.name, "raw", value.source, value.dataset) if isinstance(value, RawInput)
                 else (spec.name, "item", "items", value.name) for value in spec.inputs],
            )
            self._store._write_dataset(db, physical)
        if category_id is not None:
            self._lake.catalog.item_categories.assign(spec.name, category_id)

    def register_producer(self, key: str, revision: str, producer: Producer, *,
                          update_policy: ProducerUpdatePolicy | None = None) -> None:
        """Register runtime code matching an explicit immutable declaration."""
        self._store.ensure_writable()
        if not key or not revision or not callable(producer):
            raise ValueError("producer requires a non-empty key/revision and a callable")
        if update_policy is not None and not isinstance(update_policy, ProducerUpdatePolicy):
            raise TypeError("update_policy must be a ProducerUpdatePolicy")
        if (key, revision) in self._update_policies and self._update_policies[(key, revision)] != update_policy:
            raise ValueError("Update policy conflicts with the registered immutable producer revision")
        self._producers[(key, revision)] = producer
        if update_policy is not None:
            self._update_policies[(key, revision)] = update_policy
        else:
            self._update_policies.pop((key, revision), None)

    def list(self, *, include_inactive: bool = False) -> list[DataItemSpec]:
        rows = self._store._rows("select spec_json from item_definitions " + ("" if include_inactive else "where active=1 ") + "order by name")
        return [spec_from_payload(json.loads(row["spec_json"])) for row in rows]

    def get(self, name: str, *, include_inactive: bool = False) -> DataItemSpec:
        rows = self._store._rows("select spec_json,active from item_definitions where name=?", (name,))
        if not rows or (not include_inactive and not rows[0]["active"]):
            raise KeyError(f"Unknown active DataItem: {name}")
        return spec_from_payload(json.loads(rows[0]["spec_json"]))

    def remove(self, name: str) -> None:
        """Archive an item while retaining committed and frozen evidence."""
        self._store.ensure_writable()
        self.get(name)
        with self._store.dataset_writer("items", name, uuid4().hex), self._store.connect() as db:
            db.execute("begin immediate")
            dependants = self._store._rows(
                "select d.item_name from item_dependencies d join item_definitions i on i.name=d.item_name "
                "where d.kind='item' and d.dataset=? and i.active=1 order by d.item_name", (name,),
            )
            if dependants:
                raise ValueError(f"DataItem has active dependencies: {[value['item_name'] for value in dependants]}")
            db.execute("update item_definitions set active=0 where name=?", (name,))
            db.execute("delete from catalog_assignments where kind='item' and object_key=?", (name,))
            db.execute("update datasets set active=0,enabled=0 where source='items' and name=?", (name,))

    def plan_update(self, name: str, *, end: DateLike, start: DateLike = "2000-01-01",
                    update_policy: ProducerUpdatePolicy | None = None,
                    input_windows: Mapping[str, tuple[DateLike, DateLike]] | None = None) -> list[dict[str, str]]:
        """Plan from committed records, optionally binding a caller's producer policy.

        Read-only callers may supply the immutable producer's causal contract
        without registering executable code or changing the catalog. A supplied
        contract must agree with any already registered runtime policy.
        """
        spec = self.get(name)
        policy = self._policy(spec)
        if update_policy is not None:
            if not isinstance(update_policy, ProducerUpdatePolicy) or not spec.producer_key:
                raise ValueError("An explicit update policy requires a declared external producer")
            if policy is not None and policy != update_policy:
                raise ValueError("Update policy conflicts with the registered producer contract")
            policy = update_policy
        return self._plan_update(name, spec=spec, policy=policy, start=start, end=end,
            input_windows=input_windows)

    def _policy(self, spec: DataItemSpec) -> ProducerUpdatePolicy | None:
        if spec.producer_key:
            return self._update_policies.get((spec.producer_key, str(spec.producer_revision)))
        if spec.time_column not in {"time", "source_time"}:
            return None
        for operation in spec.transforms:
            if not isinstance(operation, (Select, Filter, MapValues, Cast)):
                return None
            if isinstance(operation, Select) and any(target in {"time", "source_time"}
                    and source != target for source, target in operation.rename.items()):
                return None
            if isinstance(operation, MapValues) and (operation.output_column or operation.column) in {"time", "source_time"}:
                return None
        return ProducerUpdatePolicy("same_date")

    def recertify(self, name: str, *, end: DateLike, start: DateLike = "2000-01-01",
                  update_policy: ProducerUpdatePolicy | None = None,
                  expected_definition: DataItemSpec | None = None,
                  config: ExecutionOptions | None = None,
                  cancelled: Callable[[], bool] | None = None) -> ItemBuildReport:
        """Certify proven unchanged output against current recorded inputs only.

        Never initialize, read numerical values, invoke producers or update
        dependencies. Recertify dependencies first through their own contracts.
        New/changed/unknown impact fails before any numerical work; the shared
        no-op publication branch repeats source/output/definition fences.
        A caller may supply the same immutable policy accepted by plan_update.
        """
        self._store.ensure_writable()
        spec = self.get(name)
        definition_hash = _definition_hash(spec)
        if expected_definition is not None and (
                not isinstance(expected_definition, DataItemSpec)
                or _definition_hash(expected_definition) != definition_hash):
            raise ValueError("DataItem definition differs from the recertification declaration")
        policy = self._policy(spec)
        if update_policy is not None:
            if not isinstance(update_policy, ProducerUpdatePolicy) or not spec.producer_key:
                raise ValueError("An explicit update policy requires a declared external producer")
            if policy is not None and policy != update_policy:
                raise ValueError("Update policy conflicts with the registered producer contract")
            policy = update_policy
        try:
            return self._incremental_update(name, first=_date_value(start), last=_date_value(end),
                config=config, progress=None, cancelled=cancelled, force=False, input_windows=None,
                reuse_only=True, update_policy=policy, expected_definition_hash=definition_hash)
        except CancelledError:
            return ItemBuildReport(name, "cancelled", 0, None, 0, _date_value(start), _date_value(end), "")

    def _plan_update(self, name: str, *, spec: DataItemSpec, policy: ProducerUpdatePolicy | None,
                     end: DateLike, start: DateLike,
                     input_windows: Mapping[str, tuple[DateLike, DateLike]] | None = None,
                     config: ExecutionOptions | None = None) -> list[dict[str, str]]:
        self._validate_output_anchors(spec, policy)
        requests = self._input_requests(spec, input_windows)
        from bagelquant_data.pipeline.planning import initialization_actions
        definition_hash = _definition_hash(spec)
        rows = self._store._rows("select * from item_initializations where name=?", (name,))
        row = rows[0] if rows else None
        initialization = None if row is None else {"status": row["status"], "start": row["start_date"],
            "end": row["end_date"], "definition_hash": row["definition_hash"]}
        history = bool(self._store._rows("select 1 from version_commits where source='items' and dataset=? and status='committed' limit 1", (name,)))
        history = history or bool(self._store._rows("select 1 from item_builds where name=? and status='success' limit 1", (name,)))
        first, last = _date_value(start), _date_value(end)
        if last < first:
            raise ValueError("end precedes start")
        if row is not None and row["status"] == "running":
            return initialization_actions(start=first, end=last, definition_hash=definition_hash,
                initialization=initialization, has_history=history)
        completed = self._completed_rows("name=? "
            "and definition_hash=? and status='success' and start_date<=? and end_date>=? "
            "order by end_date desc,id desc limit 1", (name, definition_hash, first.isoformat(), first.isoformat()))
        if completed:
            context = (self._lake.inputs.read_context(completed[0]["frozen_receipt_id"], config=config)
                if config is not None and policy is not None and policy.general_fields else nullcontext())
            with context:
                problem = self._input_scope_problem(completed[0], requests, policy=policy,
                    first=first, last=last)
                if problem is not None:
                    return [{"mode": "blocked", "start": first.isoformat(), "end": last.isoformat(),
                        "reason": problem}]
                from .incremental import completed_actions
                return completed_actions(self, completed[0], start=first, end=last, policy=policy)
        overlap = self._store._rows("select 1 from item_builds where name=? and definition_hash=? "
            "and status='success' and start_date<=? and end_date>=? limit 1",
            (name, definition_hash, last.isoformat(), first.isoformat()))
        if overlap:
            return [{"mode": "blocked", "start": first.isoformat(), "end": last.isoformat(),
                     "reason": "completed coverage has a prefix hole; explicit completion proof is required"}]
        return initialization_actions(start=first, end=last,
            definition_hash=definition_hash, initialization=initialization, has_history=history)

    def _validate_output_anchors(self, spec, policy) -> None:
        if policy is None:
            return
        inputs = {value.key: value for value in spec.inputs}
        for alias, _fields in policy.general_fields:
            request = inputs.get(alias)
            definition = (self._store.get_dataset(request.source, request.dataset)
                if isinstance(request, RawInput) else None)
            if (not isinstance(request, RawInput) or definition is None
                    or json.loads(definition["spec_json"])["update_type"] != "general"
                    or any(value is not None for value in (request.start, request.end,
                        request.observation_start, request.observation_end))):
                raise ValueError("general_fields requires a declared whole General Raw input")
        for alias in policy.output_anchors:
            request = inputs.get(alias)
            definition = (self._store.get_dataset(request.source, request.dataset)
                if isinstance(request, RawInput) else None)
            if definition is None or json.loads(definition["spec_json"])["update_type"] != "by_date":
                raise ValueError("Output anchor requires a declared by-date Raw input")

    def _input_scope_problem(self, previous, requests, *, policy, first, last) -> str | None:
        """Use the same record-only timing preflight for planning and execution."""
        old_receipt = self._lake.inputs.get(previous["frozen_receipt_id"])
        old_requests = old_receipt.requests
        if _json({key: input_payload(value) for key, value in old_requests.items()}) == _json(
                {key: input_payload(value) for key, value in requests.items()}):
            return None
        if not _pointwise_scope_extension(old_requests, requests, policy=policy,
                completed_start=_date_value(previous["start_date"]),
                completed_end=_date_value(previous["end_date"]), requested_start=first, requested_end=last):
            return "DataItem input windows changed; explicit selected-input impact proof is required"
        with self._store.connect() as db:
            if not db.in_transaction:
                db.execute("begin")
            captured = self._lake.inputs._capture(db, requests, cutoff=old_receipt.information_cutoff,
                max_commit=old_receipt.max_commit, retained_bounds_aliases=set(requests))
        wider = self._lake.inputs._receipt("scope-extension", captured,
            hashlib.sha256(_json(captured).encode()).hexdigest())
        for alias, old in old_requests.items():
            if input_payload(old) == input_payload(requests[alias]):
                continue
            evidence = old_receipt.evidence[alias]
            expanded = requests[alias]
            current_evidence = wider.evidence[alias]
            # A pointwise Raw upper extension cannot change an old selected
            # row: canonical by-date versions and first-known events are never
            # available before their observation. Its newly admitted dates
            # must therefore be strictly later than every old cutoff. Empty
            # responses have no row on which to establish that floor, so retain
            # their complete timing authority at the original check ceiling.
            future_raw_extension = (isinstance(old, RawInput)
                and isinstance(expanded, RawInput)
                and evidence["definition"].get("update_type") == "by_date"
                and current_evidence["definition"].get("update_type") == "by_date"
                and evidence["definition_hash"] == current_evidence["definition_hash"]
                and old_receipt.information_cutoff is not None
                and old.end is not None and expanded.end is not None
                and _date_value(old.end) >= old_receipt.information_cutoff
                and _date_value(expanded.end) > _date_value(old.end)
                and (old.start is None and expanded.start is None
                    or old.start is not None and expanded.start is not None
                    and _date_value(old.start) == _date_value(expanded.start))
                and wider.max_commit == old_receipt.max_commit
                and wider.max_check_id >= old_receipt.max_check_id)
            if future_raw_extension and _json(evidence["empty_checks"]) == _json([
                    check for check in current_evidence["empty_checks"]
                    if check["id"] <= old_receipt.max_check_id]):
                continue
            verified = (not self._lake.inputs._could_have_baseline(old_receipt, alias)
                and not self._lake.inputs._could_have_baseline(wider, alias))
            uniform_baseline = (isinstance(old, RawInput)
                and all(batch["baseline"] for batch in evidence["batches"])
                and all(check["baseline"] for check in evidence["audit_checks"])
                and (self._lake.inputs._proven_baseline_at(old_receipt, alias,
                    _date_value(previous["start_date"])) is True
                    or self._lake.inputs._indexed_baseline_proof(old_receipt, alias,
                        _date_value(previous["start_date"]), max_commit=None, max_check_id=None) is True))
            if not verified and not uniform_baseline:
                return "DataItem input windows changed baseline timing; explicit timing-impact proof is required"
        return None

    def initialize(self, name: str, *, end: DateLike, start: DateLike = "2000-01-01", config: ExecutionOptions | None = None,
                   progress: Callable[[ItemBuildReport], None] | None = None, cancelled: Callable[[], bool] | None = None) -> ItemBuildReport:
        self._store.ensure_writable()
        first, last = _date_value(start), _date_value(end)
        if last < first:
            raise ValueError("end precedes start")
        spec = self.get(name)
        self._validate_output_anchors(spec, self._policy(spec))
        definition_hash = _definition_hash(spec)
        with self._store.connect() as db:
            row = db.execute("select * from item_initializations where name=?", (name,)).fetchone()
            if row and (row["definition_hash"] != definition_hash or row["start_date"] != first.isoformat() or row["end_date"] != last.isoformat()):
                raise ValueError("Initialization range and definition are fixed; use update after completion")
            if row is None:
                if db.execute("select 1 from version_commits where source='items' and dataset=? and status='committed' limit 1", (name,)).fetchone():
                    raise ValueError("Historical initialization requires a new DataItem")
                db.execute("insert into item_initializations values(?,?,?,?,'running')", (name, definition_hash, first.isoformat(), last.isoformat()))
            elif row["status"] == "complete":
                previous = db.execute("select * from item_builds where name=? and definition_hash=? and start_date=? and end_date=? and status='success' order by id desc limit 1", (name, definition_hash, first.isoformat(), last.isoformat())).fetchone()
                if previous is None:
                    raise RuntimeError("Completed initialization has no committed build evidence")
                return ItemBuildReport(name, "unchanged", 0, previous["result_commit"], previous["input_commit"], first, last, previous["dependency_digest"], previous["frozen_receipt_id"])
        report = self._build(name, start=first, end=last, initialize=True, config=config, stack=(), progress=progress, cancelled=cancelled)
        if report.status in {"success", "unchanged"}:
            with self._store.connect() as db:
                db.execute("update item_initializations set status='complete' where name=?", (name,))
        return report

    def update(self, name: str, *, end: DateLike, start: DateLike | None = None, config: ExecutionOptions | None = None,
               progress: Callable[[ItemBuildReport], None] | None = None, cancelled: Callable[[], bool] | None = None,
               force: bool = False,
               input_windows: Mapping[str, tuple[DateLike, DateLike]] | None = None) -> ItemBuildReport:
        """Update declared causal producers over proven new/affected ranges.

        Successful whole-range completion follows all bounded publications.
        Unknown historical impact is explicit, never an implicit full replay.
        Explicit input_windows are caller-declared dependency observation
        windows, pinned in the actual frozen requests without changing the
        stored DataItem definition. Omitted dependencies retain their scope.
        """
        rows = self._store._rows("select min(start_date) as start_date from item_builds where name=? and status='success'", (name,))
        first = start or (rows[0]["start_date"] if rows and rows[0]["start_date"] else "2000-01-01")
        try:
            return self._incremental_update(name, first=_date_value(first), last=_date_value(end),
                config=config, progress=progress, cancelled=cancelled, force=force, input_windows=input_windows)
        except CancelledError:
            return ItemBuildReport(name, "cancelled", 0, None, 0, _date_value(first), _date_value(end), "")

    def _incremental_update(self, name: str, *, first: date, last: date,
                            config: ExecutionOptions | None,
                            progress: Callable[[ItemBuildReport], None] | None,
                            cancelled: Callable[[], bool] | None, force: bool,
                            input_windows: Mapping[str, tuple[DateLike, DateLike]] | None,
                            reuse_only: bool = False,
                            update_policy: ProducerUpdatePolicy | None = None,
                            expected_definition_hash: str | None = None) -> ItemBuildReport:
        if last < first:
            raise ValueError("end precedes start")
        if cancelled and cancelled():
            return ItemBuildReport(name, "cancelled", 0, None, 0, first, last, "")
        spec = self.get(name)
        definition_hash = _definition_hash(spec)
        if expected_definition_hash is not None and definition_hash != expected_definition_hash:
            raise RuntimeError("DataItem definition changed during recertification admission")
        pinned_producer = (self._producers.get((spec.producer_key, str(spec.producer_revision)))
                           if spec.producer_key else None)
        pinned_policy = update_policy if reuse_only else self._policy(spec)
        if reuse_only:
            runtime_policy = self._policy(spec)
            if runtime_policy is not None and runtime_policy != pinned_policy:
                raise RuntimeError("DataItem producer policy changed during recertification admission")
        self._validate_output_anchors(spec, pinned_policy)
        requests = self._input_requests(spec, input_windows)
        with self._store.connect() as db:
            db.execute("begin")
            output_commit_before = db.execute("select coalesce(max(seq),0) from version_commits "
                "where source='items' and dataset=? and status='committed'", (name,)).fetchone()[0]
            output_check_before = db.execute("select coalesce(max(id),0) from version_checks "
                "where source='items' and dataset=?", (name,)).fetchone()[0]
        predecessor = self._completed_rows("name=? and definition_hash=? "
            "and status='success' and start_date<=? and end_date>=? order by end_date desc,id desc limit 1",
            (name, definition_hash, first.isoformat(), first.isoformat()))
        if predecessor:
            context = (self._lake.inputs.read_context(predecessor[0]["frozen_receipt_id"], config=config)
                if config is not None and pinned_policy is not None and pinned_policy.general_fields else nullcontext())
            with context:
                problem = self._input_scope_problem(predecessor[0], requests, policy=pinned_policy,
                    first=first, last=last)
                if problem is not None:
                    raise RuntimeError(problem)
        for dependency in sorted((value for value in spec.inputs if isinstance(value, ItemInput)),
                                 key=lambda value: value.name):
            if not reuse_only and self.get(dependency.name).inputs:
                parent = self.update(dependency.name, start=first, end=last, config=config,
                    progress=progress, cancelled=cancelled)
                if parent.status == "cancelled":
                    return ItemBuildReport(name, "cancelled", 0, None, parent.max_input_commit, first, last, "")
        actions = self._plan_update(name, spec=spec, policy=pinned_policy, start=first, end=last,
            input_windows=input_windows, config=config)
        blocked = next((action for action in actions if action["mode"] == "blocked"), None)
        if blocked is not None:
            raise RuntimeError(f"Incremental DataItem plan blocked for {name}: {blocked['reason']}")
        if reuse_only and actions:
            raise RuntimeError(f"DataItem {name} has new or changed output impact; recertification requires unchanged completed coverage")
        if any(action["mode"] == "initialize" for action in actions):
            initializing = self._store._rows("select status from item_initializations where name=?", (name,))
            if initializing and initializing[0]["status"] == "running":
                raise ValueError("Resume the frozen initialization before incremental update")
        if not actions:
            previous = self._completed_rows("name=? and status='success' "
                "and definition_hash=? and start_date<=? and end_date>=? order by end_date desc,id desc limit 1",
                (name, definition_hash, first.isoformat(), last.isoformat()))[0]
            original = self._lake.inputs.get(previous["frozen_receipt_id"])
            # An unchanged causal plan may consume a narrower projection than
            # whole Source freshness (for example a timing-neutral calendar).
            # Publish that completed reuse against current inputs, preserving
            # every old build and creator receipt. Already-current no-ops need
            # neither a new capture nor a new completion record.
            if pinned_policy is not None and pinned_policy.general_fields:
                # A changed whole General snapshot can be consumed-equivalent.
                # Do not decode old row indexes to discover that it needs a new
                # provenance receipt. Unknown full metadata also captures fresh
                # current authority; the unchanged plan independently proves use.
                with self._lake.inputs.read_context(original, config=config):
                    current = self._lake.inputs._current_selection_receipt(original,
                        cutoff=original.information_cutoff)
                    rebind = current.dependency_digest != original.dependency_digest and not all(
                        self._lake.inputs._selection_equivalent(original, current, alias, records_only=True) is True
                        for alias in original.requests)
            else:
                rebind = not self._lake.inputs.is_current(original, config=config)
            receipt = (self._lake.inputs.freeze(requests, information_cutoff=last, retain_batch_bounds=True)
                if rebind else original)
            def check_noop_canceled():
                if cancelled and cancelled():
                    raise CancelledError("DataItem no-op certification cancelled")
            with self._lake.inputs.publication_guard(receipt, config=config, check_canceled=check_noop_canceled), self._store.connect() as db:
                if cancelled and cancelled():
                    return ItemBuildReport(name, "cancelled", 0, None, receipt.max_commit, first, last, "")
                if not self._lake.inputs.is_current(receipt):
                    raise RuntimeError("DataItem inputs changed during no-op planning; retry the update")
                if rebind and self._plan_update(name, spec=spec, policy=pinned_policy, start=first, end=last,
                        input_windows=input_windows, config=config):
                    raise RuntimeError("DataItem input impact changed during no-op planning; retry the update")
                if (db.execute("select 1 from version_commits where source='items' and dataset=? "
                        "and status='committed' and seq>? limit 1", (name, output_commit_before)).fetchone()
                        or db.execute("select 1 from version_checks where source='items' and dataset=? "
                            "and id>? limit 1", (name, output_check_before)).fetchone()):
                    raise RuntimeError("DataItem output changed during no-op planning; retry the update")
                if _definition_hash(self.get(name)) != definition_hash:
                    raise RuntimeError("DataItem definition changed during no-op planning")
                if reuse_only:
                    runtime_policy = self._policy(spec)
                    if runtime_policy is not None and runtime_policy != pinned_policy:
                        raise RuntimeError("DataItem producer policy changed before recertification publication")
                if cancelled and cancelled():
                    return ItemBuildReport(name, "cancelled", 0, None, receipt.max_commit, first, last, "")
                if rebind:
                    cursor = db.execute("insert into item_builds(name,definition_hash,dependency_digest,start_date,end_date,"
                        "input_commit,result_commit,status,created_at,frozen_receipt_id) "
                        "values(?,?,?,?,?,?,?,'success',?,?)", (name, definition_hash, receipt.dependency_digest,
                            first.isoformat(), last.isoformat(), receipt.max_commit, output_commit_before or None,
                            datetime.now(UTC).isoformat(), receipt.receipt_id))
                    _save_completion(db, cursor.lastrowid, pending=[(day, available)
                        for day, available in pending_dates(db, previous["id"])
                        if first.isoformat() <= day <= last.isoformat()], authority={"kind": "native", "reuse": {
                            "policy": asdict(pinned_policy) if pinned_policy is not None else None,
                            "completed_build": previous["id"], "completion_digest": verified_completion_digest(db, previous["id"]),
                            "original_source": {"id": original.receipt_id, "digest": original.digest},
                            "current_source": {"id": receipt.receipt_id, "digest": receipt.digest},
                            "requests": {alias: input_payload(request) for alias, request in requests.items()},
                            "information_cutoff": str(receipt.information_cutoff), "impact": "none",
                        }})
                check_noop_canceled()
                return ItemBuildReport(name, "unchanged", 0,
                    (output_commit_before or None) if rebind else previous["result_commit"], receipt.max_commit,
                    first, last, receipt.dependency_digest, receipt.receipt_id)
        receipt = self._lake.inputs.freeze(requests, information_cutoff=last, retain_batch_bounds=True)
        # Validate the plan again after capture; an intervening input commit/check
        # must not be certified as an unaffected prefix. Freeze remains immutable
        # even if a producer publishes new inputs while this operation executes.
        if actions != self._plan_update(name, spec=spec, policy=pinned_policy, start=first, end=last,
                input_windows=input_windows, config=config):
            raise RuntimeError("DataItem input impact changed during planning; retry the update")
        reports: list[ItemBuildReport] = []
        prior_completion = self._completed_rows("name=? and definition_hash=? and status='success' and start_date<=? "
            "order by end_date desc,id desc limit 1", (name, definition_hash, first.isoformat()))
        completed_through = _date_value(prior_completion[0]["end_date"]) if prior_completion else None
        for action in actions:
            report = self._build(name, start=_date_value(action["start"]), end=_date_value(action["end"]),
                initialize=False, config=config, stack=(), progress=progress, cancelled=cancelled,
                force=force, frozen_receipt=receipt, completed_through=completed_through,
                frozen_spec=spec, pinned_producer=pinned_producer, pinned_policy=pinned_policy)
            reports.append(report)
            if report.status == "cancelled":
                return ItemBuildReport(name, "cancelled", sum(value.rows_committed for value in reports),
                    report.commit_seq, receipt.max_commit, reports[0].start, report.end,
                    receipt.dependency_digest, receipt.receipt_id)
        result_commit = max((value.commit_seq for value in reports if value.commit_seq is not None),
            default=predecessor[0]["result_commit"] if predecessor else None)
        if cancelled and cancelled():
            return ItemBuildReport(name, "cancelled", sum(value.rows_committed for value in reports),
                result_commit, receipt.max_commit, reports[0].start, reports[-1].end,
                receipt.dependency_digest, receipt.receipt_id)
        with self._store.connect() as db:
            db.execute("begin immediate")
            current = db.execute("select spec_hash from item_definitions where name=?", (name,)).fetchone()
            if current is None or current[0] != definition_hash:
                raise RuntimeError("DataItem definition changed before completed-range publication")
            if db.execute("select 1 from version_commits where source='items' and dataset=? "
                    "and status='committed' and seq>? and (input_receipt_id is null or input_receipt_id<>?) limit 1",
                    (name, output_commit_before, receipt.receipt_id)).fetchone():
                raise RuntimeError("DataItem output changed during incremental publication; retry the update")
            if db.execute("select 1 from version_checks where source='items' and dataset=? and id>? "
                    "and (input_receipt_id is null or input_receipt_id<>?) limit 1",
                    (name, output_check_before, receipt.receipt_id)).fetchone():
                raise RuntimeError("DataItem output timing changed during incremental publication; retry the update")
            cursor = db.execute("insert into item_builds(name,definition_hash,dependency_digest,start_date,end_date,"
                "input_commit,result_commit,status,created_at,frozen_receipt_id) "
                "values(?,?,?,?,?,?,?,'success',?,?)", (name, definition_hash, receipt.dependency_digest,
                    first.isoformat(), last.isoformat(), receipt.max_commit, result_commit,
                    datetime.now(UTC).isoformat(), receipt.receipt_id))
            pending = []
            if prior_completion:
                pending.extend((day, available) for day, available in pending_dates(db, prior_completion[0]["id"])
                    if not any(action["start"] <= day <= action["end"] for action in actions))
            for action in actions:
                unit = db.execute("select id from item_builds where name=? and frozen_receipt_id=? "
                    "and start_date=? and end_date=? and id<>? order by id desc limit 1",
                    (name, receipt.receipt_id, action["start"], action["end"], cursor.lastrowid)).fetchone()
                if unit is not None:
                    pending.extend(pending_dates(db, unit[0]))
            _save_completion(db, cursor.lastrowid, pending=pending)
        return ItemBuildReport(name, "success", sum(value.rows_committed for value in reports), result_commit,
            receipt.max_commit, reports[0].start, reports[-1].end, receipt.dependency_digest, receipt.receipt_id)

    def ingest(
        self, name: str, frame: pl.DataFrame, *, available_date: DateLike | None = None,
        historical_baseline: bool = False, ingested_at: datetime | None = None,
        input_receipt: FrozenInputReceipt | str | None = None,
        expected_definition_hash: str | None = None,
    ) -> ItemBuildReport:
        """Publish neutral producer output through the shared immutable engine."""
        run_id = uuid4().hex
        with self._store.dataset_writer("items", name, run_id):
            return self._ingest(name, frame, available_date=available_date,
                historical_baseline=historical_baseline, ingested_at=ingested_at,
                input_receipt=input_receipt, expected_definition_hash=expected_definition_hash,
                run_id=run_id)

    def _ingest(self, name: str, frame: pl.DataFrame, *, available_date: DateLike | None,
                historical_baseline: bool, ingested_at: datetime | None,
                input_receipt: FrozenInputReceipt | str | None,
                expected_definition_hash: str | None, run_id: str,
                _baseline_proven: bool = False,
                _semantic_timing: bool | None = None,
                _declared_range: tuple[date, date] | None = None,
                writer_executor: ThreadPoolExecutor | None = None,
                partition_workers: int = 1,
                writer_buffer_bytes: int = 0,
                commit_batch_rows: int | None = None,
                commit_batch_bytes: int | None = None,
                cancelled: Callable[[], bool] | None = None,
                on_commit: Callable[[int, int | None], None] | None = None,
                _verification: _PublicationVerification | None = None) -> ItemBuildReport:
        self._store.ensure_writable()
        semantic_timing = "version_available_date" in frame.columns if _semantic_timing is None else _semantic_timing
        spec = self.get(name)
        explicit_availability = "available_date" in frame.columns or "version_available_date" in frame.columns
        frame = self._normalize(spec, frame)
        definition_hash = _definition_hash(spec)
        if expected_definition_hash is not None and definition_hash != expected_definition_hash:
            raise RuntimeError("DataItem definition changed while computing producer output")
        receipt = None if input_receipt is None else self._lake.inputs.get(input_receipt)
        if spec.inputs and receipt is None:
            raise ValueError("DataItem dependency publication requires an input_receipt")
        if receipt is not None:
            def identity(value):
                return ("raw", value.source, value.dataset) if isinstance(value, RawInput) else ("item", value.name)
            if {identity(value) for value in spec.inputs} != {identity(value) for value in receipt.requests.values()}:
                raise ValueError("Frozen input receipt does not match declared DataItem dependencies")
            if _verification is not None and not (_verification.active and _verification.owner is self
                      and _verification.receipt_id == receipt.receipt_id
                      and _verification.digest == receipt.digest):
                raise RuntimeError("Publication input verification no longer matches this operation")
        now = ingested_at or datetime.now(UTC)
        if now.tzinfo is None:
            raise ValueError("ingested_at must be timezone-aware")
        frame = frame.with_columns(
            pl.lit(definition_hash).alias("_item_definition_hash"),
            pl.lit(spec.producer_key, dtype=pl.String).alias("_producer_key"),
            pl.lit(spec.producer_revision, dtype=pl.String).alias("_producer_revision"),
            pl.lit(None if receipt is None else receipt.dependency_digest, dtype=pl.String).alias("_dependency_digest"),
        )
        if available_date is not None:
            frame = frame.with_columns(pl.max_horizontal("time", pl.lit(_date_value(available_date))).alias("version_available_date"))
        elif "version_available_date" not in frame.columns:
            frame = frame.with_columns(pl.max_horizontal("time", "available_date", *([] if historical_baseline or explicit_availability else [pl.lit(now.astimezone(UTC).date())])).alias("version_available_date"))
        if frame.select((pl.col("version_available_date") < pl.max_horizontal("time", "available_date")).any()).item():
            raise ValueError("version availability precedes the observation or declared availability")
        publication_dates = sorted(frame["version_available_date"].unique().to_list())
        if not publication_dates:
            publication_dates = [_date_value(available_date) if available_date is not None else now.astimezone(UTC).date()]
        baseline_by_date = {cutoff: historical_baseline for cutoff in publication_dates}
        if receipt is not None and not historical_baseline and not _baseline_proven:
            cutoffs = {day: min(day, receipt.information_cutoff) if receipt.information_cutoff is not None else day
                       for day in publication_dates}
            cache = {} if _verification is None else _verification.baseline_dates
            unresolved = [day for day in publication_dates if cutoffs[day] not in cache]
            for day in publication_dates:
                if cutoffs[day] in cache:
                    baseline_by_date[day] = cache[cutoffs[day]]
            # A caller cannot promote unknown input timing by omitting the
            # output baseline flag. Retained original baselines cease to
            # taint a later version once exact input attestations are visible.
            # Baseline propagation is an OR over all inputs. Finish every cheap
            # positive proof before decoding any optional timing projection:
            # one known surviving baseline settles that date even when another
            # input's historical timing index is unavailable.
            for alias in receipt.requests:
                for day in unresolved:
                    if not baseline_by_date[day] and self._lake.inputs._proven_baseline_at(
                            receipt, alias, cutoffs[day]) is True:
                        baseline_by_date[day] = True
            for alias in receipt.requests:
                pending = [day for day in unresolved if not baseline_by_date[day]]
                if not pending:
                    break
                # Every immutable commit records the timing flag written on
                # all its rows; empty general identities carry the same proof.
                # Selection and attestations can only remove a baseline, never
                # create one. Verified inputs need no repeated prefix replay.
                if not self._lake.inputs._could_have_baseline(receipt, alias):
                    continue
                for publication_date in pending:
                    cutoff = cutoffs[publication_date]
                    if self._lake.inputs._baseline_at(receipt, alias, cutoff,
                            max_buffer_bytes=None if _verification is None else _verification.timing_buffer_bytes):
                        baseline_by_date[publication_date] = True
            # Only complete OR results enter this operation's fixed-receipt memo.
            # Dates are independent: later attestations can change True to False.
            if _verification is not None and _verification.timing_cache_entries:
                for day in unresolved:
                    key = cutoffs[day]
                    if key not in cache and len(cache) >= _verification.timing_cache_entries:
                        cache.pop(next(iter(cache)))
                    cache[key] = baseline_by_date[day]
        if frame.select("time", "asset_id", "version_available_date").is_duplicated().any():
            raise ValueError("DataItem contains duplicate version keys")
        first = _date_value(cast(DateLike, frame["time"].min())) if frame.height else date(2000, 1, 1)
        last = _date_value(cast(DateLike, frame["time"].max())) if frame.height else first
        if _declared_range is not None:
            first, last = _declared_range
        physical = self._physical_spec(spec)
        # Generic Raw storage uses source_time for the stable observation key and
        # time for version availability. Public DataItem reads restore the axis.
        # A first panel with distinct coordinates has no revisions or attestations
        # to sequence. One atomic commit preserves its row availability dates and
        # writes each month once. Existing content, repeated coordinates and mixed
        # timing proof retain chronological publications below.
        has_committed = bool(self._store._rows(
            "select 1 from version_commits where source='items' and dataset=? "
            "and status='committed' limit 1", (name,),
        ))
        initial_bulk = (
            frame.height > 0 and len(publication_dates) > 1
            and len(set(baseline_by_date.values())) == 1
            and not frame.select("time", "asset_id").is_duplicated().any()
            and not has_committed
        )
        if initial_bulk:
            publication_dates = [publication_dates[-1]]
        # Resuming an internally evaluated unverified baseline need not rewrite
        # a month for every day. Each coordinate has one causal version, and
        # baseline checks cannot promote strict visibility. Verified no-op
        # witnesses retain date-by-date checks (their check date is causal).
        resume_bulk = (frame.height > 0 and has_committed and _baseline_proven and historical_baseline
                       and commit_batch_rows is not None and commit_batch_bytes is not None
                       and len(set(baseline_by_date.values())) == 1
                       and not frame.select("time", "asset_id").is_duplicated().any())
        # Only first-publication deltas have bounded old-partition memory (zero).
        # Use the still-unused previous-revision reserve for preparation, with
        # a conservative allowance for sort/hash/Arrow/compressed copies.
        admitted_writers = 1
        if initial_bulk and partition_workers > 1 and writer_buffer_bytes:
            parts = frame.with_columns(pl.col("version_available_date").dt.strftime("%Y-%m").alias("_writer_month")).partition_by("_writer_month")
            largest = max(int(part.estimated_size()) for part in parts)
            admitted_writers = max(1, min(partition_workers, len(parts), writer_buffer_bytes // max(1, 6 * largest)))
            del parts
        rows_committed = 0
        groups = _baseline_publication_groups(frame, cast(int, commit_batch_rows), cast(int, commit_batch_bytes)) if resume_bulk else (
                      (day, frame if initial_bulk else frame.filter(pl.col("version_available_date") == day))
                      for day in publication_dates)
        for publication_date, group in groups:
            if cancelled and cancelled():
                raise CancelledError("DataItem publication cancelled")
            delta = group.rename({"time": "source_time", "version_available_date": "time"})
            result = commit_versions(
                physical, delta, self._parquet, run_id=run_id,
                mode="incremental", ingested_at=ingested_at,
                available_date=publication_date, preserve_available=True,
                preserve_semantic_timing=semantic_timing,
                historical_baseline=baseline_by_date[publication_date],
                input_receipt_id=None if receipt is None else receipt.receipt_id,
                requests=None if _declared_range is None else [{"item_range": {"start": first.isoformat(), "end": last.isoformat()}}],
                writer_executor=writer_executor if admitted_writers > 1 else None,
                partition_workers=admitted_writers,
            )
            rows_committed += result.rows_committed
            if on_commit:
                latest = self._store._rows("select max(seq) as seq from version_commits where source='items' and dataset=? and status='committed'", (name,))[0]["seq"]
                on_commit(rows_committed, latest)
        if cancelled and cancelled():
            raise CancelledError("DataItem publication cancelled")
        commits = self._store._rows("select max(seq) as seq from version_commits where source='items' and dataset=? and status='committed'", (name,))
        seq = commits[0]["seq"] if commits else None
        if receipt is not None:
            with self._store.connect() as db:
                cursor = db.execute("insert into item_builds(name,definition_hash,dependency_digest,start_date,end_date,input_commit,result_commit,status,created_at,frozen_receipt_id) values(?,?,?,?,?,?,?,'success',?,?)", (name, definition_hash, receipt.dependency_digest, first.isoformat(), last.isoformat(), receipt.max_commit, seq, now.isoformat(), receipt.receipt_id))
                if _declared_range is not None:
                    _save_completion(db, cursor.lastrowid,
                        checked_target=None if available_date is None else _date_value(available_date).isoformat())
        return ItemBuildReport(name, "success", rows_committed, seq, 0 if receipt is None else receipt.max_commit, first, last, "" if receipt is None else receipt.dependency_digest, "" if receipt is None else receipt.receipt_id)

    def _publish_proven(self, name: str, frame: pl.DataFrame, *, historical_baseline: bool,
                        input_receipt: FrozenInputReceipt,
                        expected_definition_hash: str,
                        writer_executor: ThreadPoolExecutor | None = None,
                        partition_workers: int = 1,
                        writer_buffer_bytes: int = 0,
                        commit_batch_rows: int | None = None,
                        commit_batch_bytes: int | None = None,
                        cancelled: Callable[[], bool] | None = None,
                        on_commit: Callable[[int, int | None], None] | None = None) -> ItemBuildReport:
        """Publish causal output; generated acquisition dates are not value deltas."""
        run_id = uuid4().hex
        with self._store.dataset_writer("items", name, run_id):
            return self._ingest(name, frame, available_date=None,
                historical_baseline=historical_baseline, ingested_at=None,
                input_receipt=input_receipt, expected_definition_hash=expected_definition_hash,
                run_id=run_id, _baseline_proven=True,
                _semantic_timing=False,
                writer_executor=writer_executor, partition_workers=partition_workers,
                writer_buffer_bytes=writer_buffer_bytes, commit_batch_rows=commit_batch_rows,
                commit_batch_bytes=commit_batch_bytes, cancelled=cancelled, on_commit=on_commit)

    def replace_range(self, name: str, frame: pl.DataFrame, *, start: DateLike, end: DateLike,
                      available_date: DateLike, input_receipt: FrozenInputReceipt | str | None = None,
                      expected_definition_hash: str | None = None,
                      historical_baseline: bool = False) -> ItemBuildReport:
        """Verify complete coverage at available_date, publishing withdrawals.

        Equal content retains its original publication history. Explicit
        historical versions use ingest or the frame of an ItemPublication.
        """
        run_id = uuid4().hex
        with self._store.dataset_writer("items", name, run_id):
            return self._replace_range(name, frame, start=start, end=end, available_date=available_date,
                input_receipt=input_receipt, expected_definition_hash=expected_definition_hash,
                historical_baseline=historical_baseline, run_id=run_id)

    def _replace_range(self, name: str, frame: pl.DataFrame, *, start: DateLike, end: DateLike,
                       available_date: DateLike, input_receipt: FrozenInputReceipt | str | None,
                       expected_definition_hash: str | None, historical_baseline: bool,
                       run_id: str, _verification: _PublicationVerification | None = None,
                       cancelled: Callable[[], bool] | None = None) -> ItemBuildReport:
        spec = self.get(name)
        first, last, cutoff = _date_value(start), _date_value(end), _date_value(available_date)
        if last < first:
            raise ValueError("end precedes start")
        normalized = self._normalize(spec, frame)
        if normalized.filter(~pl.col("time").is_between(first, last)).height:
            raise ValueError("Replacement output lies outside its declared range")
        existing = self.read(name, start=first, end=last, view="snapshot", as_of=cutoff).collect()
        removed = existing.join(normalized.select("time", "asset_id"), on=["time", "asset_id"], how="anti")
        tombstones = removed.select("time", "asset_id").with_columns(pl.lit(None, dtype=scalar_dtype(spec.value_dtype)).alias("value"))
        from bagelquant_data.core.schema import concat_compatible_frames
        combined = concat_compatible_frames([normalized, tombstones]) if tombstones.height else normalized
        return self._ingest(name, combined, available_date=cutoff,
            historical_baseline=historical_baseline, ingested_at=None,
            input_receipt=input_receipt, expected_definition_hash=expected_definition_hash,
            run_id=run_id, _declared_range=(first, last), _verification=_verification, cancelled=cancelled,
            _semantic_timing=False)

    @contextmanager
    def publication(self, *, input_receipt: FrozenInputReceipt | str | None = None,
                    config: ExecutionOptions | None = None,
                    cancelled: Callable[[], bool] | None = None) -> Iterator[ItemPublisher]:
        """Bind published input metadata for one serial publication operation."""
        if cancelled and cancelled():
            raise CancelledError("DataItem publication cancelled")
        receipt = None if input_receipt is None else self._lake.inputs.get(input_receipt)
        verification = _PublicationVerification(self, "" if receipt is None else receipt.receipt_id,
            "" if receipt is None else receipt.digest,
            # A conservative 1KiB entry reserve; never retain producer frames.
            timing_cache_entries=min(1024, (config or ExecutionOptions()).max_buffer_bytes // 4 // 1024),
            timing_buffer_bytes=(config or ExecutionOptions()).max_buffer_bytes)
        try:
            if cancelled and cancelled():
                raise CancelledError("DataItem publication cancelled")
            yield ItemPublisher(self, receipt, verification, cancelled)
            if get_ident() != verification.thread_id:
                raise RuntimeError("DataItem publication requires its owning thread")
            if not verification.active:
                raise RuntimeError("DataItem publication operation failed")
            if cancelled and cancelled():
                raise CancelledError("DataItem publication cancelled")
        finally:
            verification.active = False
            verification.baseline_dates.clear()

    def _publish(self, publications: Sequence[ItemPublication], *,
                input_receipt: FrozenInputReceipt | str | None = None,
                _verification: _PublicationVerification,
                cancelled: Callable[[], bool] | None = None) -> dict[str, ItemBuildReport | None]:
        """Publish a preflighted group through the ordinary writer mechanics.

        Keep historical versions, complete-range certificates and per-item
        writer transactions. Earlier committed numerical outputs survive later
        failures. Disjoint pure no-op groups share one fenced Source check and
        publish completion metadata atomically, without numerical writes.
        """
        values = tuple(publications)
        if any(not isinstance(value, ItemPublication) for value in values):
            raise TypeError("publish requires ItemPublication values")
        if len({value.name for value in values}) != len(values):
            raise ValueError("Publication item names must be unique")
        receipt = None if input_receipt is None else self._lake.inputs.get(input_receipt)
        def identity(value):
            return ("raw", value.source, value.dataset) if isinstance(value, RawInput) else ("item", value.name)
        for value in values:
            spec = self.get(value.name)
            definition_hash = _definition_hash(spec)
            if value.expected_definition_hash is not None and value.expected_definition_hash != definition_hash:
                raise RuntimeError("DataItem definition changed while computing producer output")
            if spec.inputs and receipt is None:
                raise ValueError("DataItem dependency publication requires an input_receipt")
            if receipt is not None and {identity(v) for v in spec.inputs} != {identity(v) for v in receipt.requests.values()}:
                raise ValueError("Frozen input receipt does not match declared DataItem dependencies")
            if value.start is not None and value.end is not None and _date_value(value.end) < _date_value(value.start):
                raise ValueError("end precedes start")
            if not value.frame.is_empty() or value.start is not None:
                normalized = self._normalize(spec, value.frame)
                if value.start is not None and value.end is not None and normalized.filter(
                        ~pl.col("time").is_between(_date_value(value.start), _date_value(value.end))).height:
                    raise ValueError("Replacement output lies outside its declared range")
        if cancelled and cancelled():
            raise CancelledError("DataItem publication cancelled")
        verification = _verification
        if receipt is not None and len(values) > 1 and all(
                value.frame.is_empty() and value.affected_ranges == ()
                and value.reuse_build_id is not None and value.start is not None
                and value.end is not None and value.complete_at is not None for value in values):
            grouped = self._publish_noop_group(values, receipt=receipt,
                verification=verification, cancelled=cancelled)
            if grouped is not None:
                return grouped
        results: dict[str, ItemBuildReport | None] = {}
        for value in values:
            if cancelled and cancelled():
                raise CancelledError("DataItem publication cancelled")
            run_id = uuid4().hex
            result = None
            if value.reuse_build_id is not None:
                result = self._publish_sparse(value, receipt=receipt, verification=verification, cancelled=cancelled)
                results[value.name] = result
                continue
            with self._store.dataset_writer("items", value.name, run_id):
                if not value.frame.is_empty():
                    result = self._ingest(value.name, value.frame, available_date=None,
                        historical_baseline=False, ingested_at=None, input_receipt=receipt,
                        expected_definition_hash=value.expected_definition_hash, run_id=run_id,
                        cancelled=cancelled, _verification=verification)
                if value.start is not None and value.end is not None and value.complete_at is not None:
                    if cancelled and cancelled():
                        raise CancelledError("DataItem publication cancelled")
                    latest = value.frame
                    spec = self.get(value.name)
                    if "version_available_date" in latest.columns:
                        latest = latest.sort("version_available_date").unique([spec.time_column, spec.asset_column], keep="last")
                    result = self._replace_range(value.name, latest, start=value.start, end=value.end,
                        available_date=value.complete_at, input_receipt=receipt,
                        expected_definition_hash=value.expected_definition_hash,
                        historical_baseline=False, run_id=run_id, _verification=verification,
                        cancelled=cancelled)
            if cancelled and cancelled():
                raise CancelledError("DataItem publication cancelled")
            results[value.name] = result
        if cancelled and cancelled():
            raise CancelledError("DataItem publication cancelled")
        return results

    def _publish_noop_group(self, values, *, receipt, verification, cancelled):
        """Share currentness only while disjoint no-op metadata is fenced."""
        budget = verification.timing_buffer_bytes or ExecutionOptions().max_buffer_bytes
        if sum(8192 + len(value.name.encode()) * 4 for value in values) > budget // 4:
            raise ValueError("No-op publication group exceeds its metadata buffer budget")
        def check_canceled():
            if cancelled and cancelled():
                raise CancelledError("Sparse DataItem completion cancelled")
        check_canceled()
        leases = {value.name: uuid4().hex for value in sorted(values, key=lambda value: value.name)}
        self._store.acquire_update_leases(("items", name, run_id) for name, run_id in leases.items())
        group = None
        try:
            with self._lake.inputs.publication_guard(receipt,
                    config=ExecutionOptions(max_buffer_bytes=budget), check_canceled=check_canceled), self._store.connect() as db:
                check_canceled()
                if not self._lake.inputs.is_current(receipt):
                    raise RuntimeError("Sparse DataItem inputs changed before completion")
                # A completion can alter a Source's creator proof without changing
                # values. Exclude every captured/resolved parent and registered
                # transitive Item dependency before borrowing this fixed check.
                context = self._lake.inputs._read_context()
                assert context is not None
                pending = list(context.receipts.values())
                seen, inputs = set(), set()
                reserve = 0
                while pending:
                    check_canceled()
                    source = pending.pop()
                    if source.receipt_id in seen:
                        continue
                    seen.add(source.receipt_id)
                    if len(seen) > 2048:
                        raise ValueError("No-op publication Source graph exceeds its metadata budget")
                    for request in source.requests.values():
                        if isinstance(request, ItemInput):
                            inputs.add(request.name)
                    for evidence in source.evidence.values():
                        for dependency in (evidence.get("definition") or {}).get("inputs", ()):
                            if dependency["kind"] == "ItemInput":
                                inputs.add(dependency["name"])
                        for key, digest in evidence["parent_receipts"].items():
                            original = self._lake.inputs.get(key)
                            if original.digest != digest:
                                raise RuntimeError("Frozen upstream input receipt checksum mismatch")
                            pending.append(original)
                    reserve += 2048
                    if reserve > budget // 4:
                        raise ValueError("No-op publication Source graph exceeds its metadata budget")
                frontier = list(inputs)
                while frontier:
                    check_canceled()
                    for row in db.execute("select dataset from item_dependencies where item_name=? and kind='item'",
                            (frontier.pop(),)):
                        if row[0] not in inputs:
                            inputs.add(row[0])
                            frontier.append(row[0])
                            reserve += 2048 + len(row[0].encode()) * 4
                            if reserve > budget // 4:
                                raise ValueError("No-op publication dependency graph exceeds its metadata budget")
                if inputs.intersection(leases):
                    return None
                group = _SparseGroup(self, db, receipt.receipt_id, receipt.digest, leases)
                verification.sparse_group = group
                results = {}
                for value in values:
                    check_canceled()
                    # Admit individual stored proof bodies before their canonical
                    # resolvers decode them; never retain all group authorities.
                    self._admit_noop_completion(db, value.reuse_build_id, budget)
                    results[value.name] = self._publish_sparse(value, receipt=receipt,
                        verification=verification, cancelled=cancelled)
                check_canceled()
                group.active = False
                verification.sparse_group = None
                return results
        finally:
            if group is not None:
                group.active = False
            verification.sparse_group = None
            self._store.release_update_leases(leases.values())

    @staticmethod
    def _admit_noop_completion(db, build_id, budget):
        """Admit proof bodies and referenced original receipts before decoding."""
        size = 0
        for table in ("item_completion_authorities", "item_completion_attestations", "item_pending_outputs"):
            if db.execute("select 1 from sqlite_master where name=?", (table,)).fetchone():
                row = db.execute(f"select length(payload_json) from {table} where build_id=?",
                    (build_id,)).fetchone()
                size += 0 if row is None else row[0]
        source_size = db.execute("select length(f.payload_json) from item_builds b "
            "join frozen_inputs f on f.receipt_id=b.frozen_receipt_id where b.id=?",
            (build_id,)).fetchone()
        size += 0 if source_size is None else source_size[0]
        original_output = db.execute("select length(f.payload_json) from item_completion_authorities a "
            "join frozen_inputs f on f.receipt_id=json_extract(a.payload_json,'$.output_receipt.id') "
            "where a.build_id=?", (build_id,)).fetchone()
        size += 0 if original_output is None else original_output[0]
        if db.execute("select 1 from sqlite_master where name='item_completion_output_receipts'").fetchone():
            original_output = db.execute("select length(i.payload_json)+coalesce(length(f.payload_json),0) "
                "from item_completion_output_receipts i left join frozen_inputs f "
                "on f.receipt_id=json_extract(i.payload_json,'$.output_receipt_id') where i.build_id=?",
                (build_id,)).fetchone()
            size += 0 if original_output is None else original_output[0]
        if size * 4 > budget // 4:
            raise ValueError("No-op completion proof exceeds its metadata buffer budget")

    def _publish_sparse(self, value: ItemPublication, *, receipt, verification, cancelled):
        """Publish producer-declared affected ranges and certify reused coverage.

        The external producer owns numerical impact; Data owns original coverage,
        coordinate replacement, pending visibility, immutable versions and the
        closing source/output fence. A row-only ingest is never a predecessor.
        """
        if receipt is None or value.start is None or value.end is None or value.complete_at is None:
            raise ValueError("Sparse publication requires frozen inputs and complete coverage")
        previous_rows = self._completed_rows("b.id=? and b.name=?", (value.reuse_build_id, value.name))
        if len(previous_rows) != 1:
            raise ValueError("Sparse publication requires an original whole-range completion")
        previous = previous_rows[0]
        first, last = _date_value(value.start), _date_value(value.end)
        cutoff = _date_value(value.complete_at)
        spec = self.get(value.name)
        definition = _definition_hash(spec)
        if (previous["definition_hash"] != definition or previous["start_date"] > first.isoformat()
                or previous["end_date"] < first.isoformat() or cutoff != receipt.information_cutoff):
            raise ValueError("Sparse publication changed its original definition/coverage or input cutoff")
        ranges = sorted((_date_value(a), _date_value(b)) for a, b in (value.affected_ranges or ()))
        if any(a < first or b > last or a > b for a, b in ranges) or any(
                b >= c for (_, b), (c, _) in zip(ranges, ranges[1:])):
            raise ValueError("Sparse affected ranges must be disjoint and inside complete coverage")
        from datetime import timedelta
        next_unknown = max(first, _date_value(previous["end_date"]) + timedelta(days=1))
        for a, b in ranges:
            if b < next_unknown:
                continue
            if a > next_unknown:
                break
            next_unknown = b + timedelta(days=1)
        if next_unknown <= last:
            raise ValueError("Sparse affected ranges leave a hole in newly declared coverage")
        original = self._lake.inputs.get(previous["frozen_receipt_id"])
        if original.information_cutoff is None or cutoff < original.information_cutoff:
            raise ValueError("Sparse publication cannot regress original visibility")
        with self._store.connect() as db:
            pending = pending_dates(db, previous["id"])
        if any(first.isoformat() <= day <= last.isoformat() and original.information_cutoff < _date_value(available) <= cutoff
                and not any(a <= _date_value(day) <= b for a, b in ranges) for day, available in pending):
            raise ValueError("Sparse affected ranges omit newly mature original output coordinates")
        frame = self._normalize(spec, value.frame)
        covered = pl.lit(False)
        for a, b in ranges:
            covered = covered | pl.col("time").is_between(a, b)
        if frame.filter(~covered).height:
            raise ValueError("Sparse output lies outside declared affected ranges")
        rows, result_commit = 0, previous["result_commit"]
        group = verification.sparse_group
        if group is not None:
            with self._store.connect() as db:
                lease = db.execute("select run_id from update_leases where source='items' and dataset=?",
                    (value.name,)).fetchone()
                if (not group.active or group.owner is not self or group.thread_id != get_ident()
                        or group.connection is not db or not db.in_transaction
                        or (group.receipt_id, group.digest) != (receipt.receipt_id, receipt.digest)
                        or value.name not in group.leases or lease is None or lease[0] != group.leases[value.name]
                        or ranges or not frame.is_empty()):
                    raise RuntimeError("Sparse publication requires its active owned no-op transaction")
        run_id = uuid4().hex if group is None else group.leases[value.name]
        with (self._store.dataset_writer("items", value.name, run_id) if group is None else nullcontext()):
            with self._store.connect() as db:
                authority = json.loads(db.execute("select payload_json from item_completion_authorities where build_id=?",
                    (previous["id"],)).fetchone()[0])
                if authority["kind"] != "native" and not (authority["kind"] == "attested" and not ranges
                                                           and authority.get("output_receipt")):
                    raise ValueError("Sparse publication requires native whole-range producer authority")
                check_bound = output_check_bound(db, previous["id"], authority)
                if check_bound is None:
                    raise ValueError("Sparse publication requires original output-check boundary maintenance")
                later = db.execute("select b.row_count,b.min_observation,b.max_observation "
                    "from version_segments b join version_commits c on c.seq=b.commit_seq "
                    "where c.source='items' and c.dataset=? and c.status='committed' and c.seq>? and b.row_count>0"
                    + (" limit 1" if not ranges else ""), (value.name, previous["result_commit"] or 0))
                for batch in later:
                    if (batch["min_observation"] is None or batch["max_observation"] is None or not any(
                            a <= _date_value(batch["min_observation"]) <= _date_value(batch["max_observation"]) <= b for a, b in ranges)):
                        raise ValueError("Sparse publication has later output outside declared affected ranges")
                if not ranges and db.execute("select 1 from version_checks where source='items' and dataset=? and id>? limit 1",
                        (value.name, check_bound)).fetchone():
                    raise ValueError("Sparse publication has later output timing outside declared affected ranges")
                checks = (() if not ranges else db.execute(
                    "select request_json from version_checks where source='items' and dataset=? and id>?",
                    (value.name, check_bound)))
                for check in checks:
                    scopes = [query["item_range"] for query in json.loads(check["request_json"]) if "item_range" in query]
                    if not scopes or any(not any(a <= _date_value(scope["start"]) <= _date_value(scope["end"]) <= b
                            for a, b in ranges) for scope in scopes):
                        raise ValueError("Sparse publication has later output timing outside declared affected ranges")
            # Read/replace only affected output ranges; no untouched historical
            # version or partition is republished under a later cutoff.
            for a, b in ranges:
                report = self._replace_range(value.name, frame.filter(pl.col("time").is_between(a, b)),
                    start=a, end=b, available_date=cutoff, input_receipt=receipt,
                    expected_definition_hash=definition, historical_baseline=False,
                    run_id=run_id, _verification=verification, cancelled=cancelled)
                rows += report.rows_committed
                result_commit = report.commit_seq
            check_bound = self._store._rows("select coalesce(max(id),0) as id from version_checks "
                "where source='items' and dataset=?", (value.name,))[0]["id"]
        def check_canceled():
            if cancelled and cancelled():
                raise CancelledError("Sparse DataItem completion cancelled")
        with (self._lake.inputs.publication_guard(receipt, check_canceled=check_canceled) if group is None
                else nullcontext()), self._store.connect() as db:
            if group is None and not self._lake.inputs.is_current(receipt):
                raise RuntimeError("Sparse DataItem inputs changed before completion")
            current = db.execute("select coalesce(max(seq),0) from version_commits where source='items' "
                "and dataset=? and status='committed'", (value.name,)).fetchone()[0]
            current_check = db.execute("select coalesce(max(id),0) from version_checks "
                "where source='items' and dataset=?", (value.name,)).fetchone()[0]
            if current != (result_commit or 0) or current_check != check_bound or self.get(value.name) != spec:
                raise RuntimeError("Sparse DataItem output or definition changed before completion")
            if not ranges:
                # Repeat no-op admission only after the ordinary source, output,
                # check and definition fences. Match the exact original parent
                # authority and complete frozen payload, not a stale old build
                # that merely has equal inputs or an unchanged result counter.
                continuation = {"build_id": previous["id"],
                    "digest": verified_completion_digest(db, previous["id"]), "affected_ranges": []}
                candidates = db.execute("select b.id,b.frozen_receipt_id,length(a.payload_json) as size from item_builds b "
                    "join frozen_inputs f on f.receipt_id=b.frozen_receipt_id "
                    "join item_completion_authorities a on a.build_id=b.id "
                    "where b.name=? and b.definition_hash=? and b.start_date=? and b.end_date=? "
                    "and b.result_commit is ? and b.status='success' and f.digest=? order by b.id desc",
                    (value.name, definition, str(first), str(last), result_commit, receipt.digest))
                for candidate in candidates:
                    if group is not None:
                        self._admit_noop_completion(db, candidate["id"],
                            verification.timing_buffer_bytes or ExecutionOptions().max_buffer_bytes)
                    authority = json.loads(db.execute("select payload_json from item_completion_authorities where build_id=?",
                        (candidate["id"],)).fetchone()[0])
                    if authority.get("kind") == "native" and authority.get("continuation") == continuation:
                        self._lake.inputs.get(candidate["frozen_receipt_id"])
                        verified_completion_digest(db, candidate["id"])
                        check_canceled()
                        return ItemBuildReport(value.name, "unchanged", 0, result_commit, receipt.max_commit,
                            first, last, receipt.dependency_digest, receipt.receipt_id)
            cursor = db.execute("insert into item_builds(name,definition_hash,dependency_digest,start_date,end_date,"
                "input_commit,result_commit,status,created_at,frozen_receipt_id) values(?,?,?,?,?,?,?,'success',?,?)",
                (value.name, definition, receipt.dependency_digest, str(first), str(last), receipt.max_commit,
                    result_commit, datetime.now(UTC).isoformat(), receipt.receipt_id))
            _save_completion(db, cursor.lastrowid, checked_target=cutoff.isoformat(), pending=[(day, available) for day, available in pending
                if first.isoformat() <= day <= last.isoformat() and not any(a <= _date_value(day) <= b for a, b in ranges)],
                authority={"kind": "native", "continuation": {"build_id": previous["id"],
                    "digest": verified_completion_digest(db, previous["id"]), "affected_ranges": ranges}})
            check_canceled()
        return ItemBuildReport(value.name, "success", rows, result_commit, receipt.max_commit,
            first, last, receipt.dependency_digest, receipt.receipt_id)

    def read(
        self, name: str, *, start: DateLike | None = None, end: DateLike | None = None,
        assets: tuple[str, ...] | list[str] | None = None,
        as_of: DateLike | None = None, view: str = "history", strict: bool = False,
        max_commit: int | None = None, max_check_id: int | None = None,
    ) -> pl.LazyFrame:
        """Read causal history, an explicit as-of snapshot, or diagnostic versions."""
        spec = self.get(name)
        if view not in {"history", "snapshot", "versions"}:
            raise ValueError("view must be history, snapshot, or versions")
        if view == "snapshot" and as_of is None:
            raise ValueError("snapshot reads require as_of")
        if self._max_commit is not None:
            if max_commit is not None and max_commit > self._max_commit:
                raise ValueError("max_commit exceeds the captured input boundary")
            max_commit = self._max_commit if max_commit is None else max_commit
        if self._as_of is not None:
            if as_of is not None and _date_value(as_of) > self._as_of:
                raise ValueError("as_of exceeds the captured information cutoff")
            as_of = self._as_of if as_of is None else as_of
        if self._max_check_id is not None:
            if max_check_id is not None and max_check_id > self._max_check_id:
                raise ValueError("max_check_id exceeds the captured input boundary")
            max_check_id = self._max_check_id if max_check_id is None else max_check_id
        if not self._store.manifest("items", name):
            return pl.DataFrame(schema={"time": pl.Date, "asset_id": pl.String, "value": scalar_dtype(spec.value_dtype), "observation_date": pl.Date, "available_date": pl.Date, "version_available_date": pl.Date, "_baseline": pl.Boolean}).lazy()
        frame = self._query.query(
            name, source="items", view="versions", max_commit=max_commit,
            observation_start=start, observation_end=end, assets=assets,
            max_check_id=max_check_id,
        )
        if strict:
            frame = frame.filter(~pl.col("_baseline").fill_null(False))
        if as_of is not None:
            frame = frame.filter(pl.col("time") <= _date_value(as_of))
        if view == "history":
            frame = frame.filter(pl.col("time") <= pl.col("source_time"))
        if view != "versions":
            frame = frame.sort("time", "ingested_at", "_commit_seq").unique(["source_time", "asset_id"], keep="last", maintain_order=True)
        return frame.rename({"time": "version_available_date", "source_time": "time"}).sort("time", "asset_id", "version_available_date")

    @overload
    def status(self, name: str, *, include_inactive: bool = False) -> dict[str, Any]: ...

    @overload
    def status(self, name: None = None, *, include_inactive: bool = False) -> list[dict[str, Any]]: ...

    def status(self, name: str | None = None, *, include_inactive: bool = False) -> list[dict[str, Any]] | dict[str, Any]:
        rows = self._store.dataset_statuses(source="items", datasets=None if name is None else [name], include_inactive=include_inactive)
        if name is None:
            return rows
        self.get(name, include_inactive=include_inactive)
        row = dict(rows[0]) if rows else {"name": name, "row_count": 0}
        item = self._store._rows("select spec_hash from item_definitions where name=?", (name,))[0]
        builds = self._store._rows("select dependency_digest,input_commit,result_commit,frozen_receipt_id from item_builds where name=? and status='success' order by id desc limit 1", (name,))
        row["definition_hash"] = item["spec_hash"]
        initializations = self._store._rows("select name,definition_hash,start_date,end_date,status "
            "from item_initializations where name=?", (name,))
        row["initialization"] = dict(initializations[0]) if initializations else None
        physical_hash = self._store.dataset_spec_hash("items", name)
        manifests = self._store.manifest("items", name)
        committed = self._store._rows(
            "with latest as (select b.partition_path,max(c.seq) as seq from version_segments b "
            "join version_commits c on c.seq=b.commit_seq where c.source='items' and c.dataset=? "
            "and c.status='committed' group by b.partition_path) "
            "select latest.partition_path,c.spec_hash from latest join version_commits c on c.seq=latest.seq",
            (name,),
        )
        hashes = {value["partition_path"]: value["spec_hash"] for value in committed}
        row["committed_definition_current"] = bool(manifests) and all(
            hashes.get(value["partition_path"]) == physical_hash for value in manifests
        )
        row.update(builds[0] if builds else {})
        # The physical manifest's time is availability; report the daily axis
        # separately using registered batch observation bounds.
        bounds = self._store._rows("select min(b.min_observation) as minimum_time,max(b.max_observation) as maximum_time from version_segments b join version_commits c on c.seq=b.commit_seq where c.source='items' and c.dataset=? and c.status='committed'", (name,))[0]
        row.update(bounds)
        return row

    def builds(self, name: str) -> list[dict[str, Any]]:
        """Successful immutable build references, retained after item archival."""
        self.get(name, include_inactive=True)
        return self._store._rows(
            "select id,name,definition_hash,dependency_digest,start_date,end_date,"
            "input_commit,result_commit,status,created_at,frozen_receipt_id "
            "from item_builds where name=? and status='success' order by id",
            (name,),
        )

    def completion_statuses(self, *, names: tuple[str,...] | list[str] | None = None) -> list[dict[str,Any]]:
        """Batch-read DataItem completion flags without decoding original inputs."""
        return self._store.completion_statuses(source="items",datasets=names)

    @contextmanager
    def completion_guard(self, *, names: tuple[str,...] | list[str] | None = None) -> Iterator[list[dict[str,Any]]]:
        """Fence metadata flags through a finite consumer reference publication.

        The consumer owns its reference CAS and cancellation. Do not run kernels,
        providers or Data writes inside this context; original values are unread.
        """
        with self._store.publication_read_view():
            yield self.completion_statuses(names=names)

    def completed_builds(self, name: str) -> list[dict[str, Any]]:
        """Read verified whole-range authorities, excluding row-only ingests.

        Original native or explicitly attested completion, pending visibility
        and source bindings are verified from records. Missing authorities are
        absent; damaged authorities fail explicitly. No values are read and
        no missing evidence is created.
        """
        self.get(name, include_inactive=True)
        return self._completed_rows("b.name=? and b.status='success' order by b.id", (name,))

    def manifest(self, name: str, *, include_inactive: bool = False) -> list[dict[str, Any]]:
        """Committed public partition receipts for application references."""
        self.get(name, include_inactive=include_inactive)
        definition = self._store._rows("select spec_hash from item_definitions where name=?", (name,))[0]["spec_hash"]
        rows = self._store._rows(
            "select p.*,v.commit_seq,v.minimum_time,v.maximum_time from partition_manifest p "
            "left join (select b.partition_path,max(b.commit_seq) as commit_seq,"
            "min(b.min_observation) as minimum_time,max(b.max_observation) as maximum_time "
            "from version_segments b join version_commits c on c.seq=b.commit_seq "
            "where c.source='items' and c.dataset=? and c.status='committed' group by b.partition_path) v "
            "on v.partition_path=p.partition_path where p.source='items' and p.dataset=? "
            "order by p.source,p.dataset,p.partition_path", (name, name))
        return [{**row, "definition_hash": definition} for row in rows]

    def _reaches(self, first: str, target: str, seen: set[str] | None = None) -> bool:
        if first == target:
            return True
        seen = set() if seen is None else seen
        if first in seen:
            return False
        seen.add(first)
        return any(self._reaches(str(row["dataset"]), target, seen) for row in self._store._rows("select dataset from item_dependencies where item_name=? and kind='item'", (first,)))

    @staticmethod
    def _physical_spec(spec: DataItemSpec) -> DatasetSpec:
        return DatasetSpec(spec.name, "by_date", source="items", description=spec.description,
                           date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"},
                           request_options={"item_definition_hash": _definition_hash(spec)})

    def _normalize(self, spec: DataItemSpec, frame: pl.DataFrame) -> pl.DataFrame:
        frame = frame.drop([name for name in VERSION_FIELDS if name in frame.columns])
        mapping = {spec.time_column if spec.time_column in frame.columns else "time": "time", spec.asset_column if spec.asset_column in frame.columns else "asset_id": "asset_id", spec.value_column if spec.value_column in frame.columns else "value": "value"}
        missing = set(mapping) - set(frame.columns)
        if missing:
            raise ValueError(f"DataItem producer is missing columns: {sorted(missing)}")
        if spec.time_column != "time" and spec.time_column in frame.columns and "time" in frame.columns and "available_date" not in frame.columns:
            frame = frame.with_columns(pl.col("time").alias("available_date"))
        collision = [value for key, value in mapping.items() if key != value and value in frame.columns]
        frame = frame.drop(collision).rename({key: value for key, value in mapping.items() if key != value})
        frame = frame.with_columns(pl.col("time").cast(pl.Date, strict=True), pl.col("asset_id").cast(pl.String, strict=True), pl.col("value").cast(scalar_dtype(spec.value_dtype), strict=True))
        if frame.select(pl.any_horizontal(pl.col("time").is_null(), pl.col("asset_id").is_null(), pl.col("asset_id").str.strip_chars().str.len_chars() == 0).any()).item():
            raise ValueError("DataItem keys must not be null")
        frame = frame.with_columns(
            (pl.col("observation_date").cast(pl.Date) if "observation_date" in frame.columns else pl.col("time")).alias("observation_date"),
            (pl.col("available_date").cast(pl.Date) if "available_date" in frame.columns else pl.col("time")).alias("available_date"),
        )
        frame = frame.with_columns(
            pl.when(pl.col("value").is_null() & pl.col(name).is_null()).then(pl.col("time")).otherwise(pl.col(name)).alias(name)
            for name in ("observation_date", "available_date")
        )
        if frame["available_date"].null_count() or frame["observation_date"].null_count():
            raise ValueError("DataItem evidence dates must not be null")
        if "version_available_date" in frame.columns:
            frame = frame.with_columns(pl.col("version_available_date").cast(pl.Date, strict=True))
        return frame.sort("time", "asset_id", *( ["version_available_date"] if "version_available_date" in frame.columns else []))

    def _build(self, name: str, *, start: date, end: date, initialize: bool, config: ExecutionOptions | None, stack: tuple[str, ...],
               progress: Callable[[ItemBuildReport], None] | None, cancelled: Callable[[], bool] | None, force: bool = False,
               input_windows: Mapping[str, tuple[DateLike, DateLike]] | None = None,
               frozen_receipt: FrozenInputReceipt | None = None,
               completed_through: date | None = None, frozen_spec: DataItemSpec | None = None,
               pinned_producer: Producer | None = None,
               pinned_policy: ProducerUpdatePolicy | None = None) -> ItemBuildReport:
        self._store.ensure_writable()
        if end < start:
            raise ValueError("end precedes start")
        if name in stack:
            raise ValueError("DataItem dependency cycle")
        if cancelled and cancelled():
            return ItemBuildReport(name, "cancelled", 0, None, 0, start, end, "")
        spec = frozen_spec or self.get(name)
        self._validate_output_anchors(spec, pinned_policy or self._policy(spec))
        from dataclasses import replace
        requests = dict(frozen_receipt.requests) if frozen_receipt is not None else self._input_requests(spec, input_windows)
        for dependency in sorted((value for value in spec.inputs if isinstance(value, ItemInput)), key=lambda value: value.name):
            if frozen_receipt is None and self.get(dependency.name).inputs:
                parent = self.update(dependency.name, start=start, end=end, config=config, progress=progress,
                    cancelled=cancelled)
                if parent.status == "cancelled":
                    return ItemBuildReport(name, "cancelled", 0, None, parent.max_input_commit, start, end, "")
        row = self._store._rows("select coalesce(max(seq),0) as seq from version_commits where status='committed'")[0]
        max_commit = int(row["seq"])
        # Frozen receipts capture dependency contracts and central immutable bytes
        # in one SQLite read transaction, independently from later publications.
        receipt = frozen_receipt or self._lake.inputs.freeze(requests, information_cutoff=end,
            max_commit=max_commit, retain_batch_bounds=True)
        max_commit = receipt.max_commit
        through = receipt.information_cutoff or end
        definition_hash = _definition_hash(spec)
        dependency_digest = receipt.dependency_digest
        prior = self._completed_rows(
            "name=? and definition_hash=? and dependency_digest=? and start_date=? and end_date=? and status='success' order by id desc limit 1",
            (name, definition_hash, dependency_digest, start.isoformat(), end.isoformat()),
        )
        if prior:
            overwritten = self._store._rows("select 1 from version_segments b "
                "join version_commits c on c.seq=b.commit_seq where c.source='items' and c.dataset=? "
                "and c.status='committed' and b.commit_seq>? and (b.min_observation is null "
                "or b.max_observation is null or (b.min_observation<=? and b.max_observation>=?)) limit 1",
                (name, prior[0]["result_commit"] or 0, end.isoformat(), start.isoformat()))
            if overwritten:
                prior = []
        if prior and not force:
            return ItemBuildReport(name, "unchanged", 0, prior[0]["result_commit"], max_commit, start, end, dependency_digest, receipt.receipt_id)
        options = config or ExecutionOptions()
        policy = pinned_policy if frozen_spec is not None else self._policy(spec)
        frames: dict[str, pl.DataFrame] = {}
        buffered = 0
        read_start = start
        ordered_inputs = list(requests)
        if policy is not None and policy.history_observations is not None:
            calendar_alias = policy.calendar_input
            if calendar_alias not in requests or not receipt.evidence[calendar_alias]["general_snapshots"]:
                raise ValueError("Finite-history calendar must be a complete General input")
            ordered_inputs.remove(calendar_alias)
            ordered_inputs.insert(0, calendar_alias)
        for key in ordered_inputs:
            request = requests[key]
            bounded_history = policy is not None and (policy.historical_impact == "same_date"
                or policy.history_observations is not None)
            definition = (self._store.get_dataset(request.source, request.dataset)
                          if isinstance(request, RawInput) else None)
            windowed = bounded_history and (isinstance(request, ItemInput)
                or definition is not None and json.loads(definition["spec_json"])["update_type"] == "by_date")
            frame = self._lake.inputs.read(receipt, key,
                start=read_start if windowed else None, end=end if windowed else None).collect()
            buffered += frame.estimated_size()
            if buffered > options.max_buffer_bytes // 2:
                raise MemoryError("Frozen DataItem inputs exceed max_buffer_bytes; use smaller declared input windows")
            frames[key] = frame
            if policy is not None and policy.history_observations is not None and key == policy.calendar_input:
                from bagelquant_data import input_index
                calendar = self._lake.inputs.read(receipt, key, view="snapshot", fields=()).collect()
                axis = input_index.coordinate_dates(calendar, policy.calendar_field, policy.calendar_selector)
                previous = [_date_value(value) for value in axis if _date_value(value) < start]
                if previous:
                    read_start = previous[-(policy.history_observations + 1)] if len(previous) > policy.history_observations else previous[0]
        if not spec.inputs:
            raise ValueError("An initialized DataItem must declare inputs")
        producer = None
        if spec.producer_key:
            producer = pinned_producer or self._producers.get((spec.producer_key, str(spec.producer_revision)))
            if producer is None:
                raise ValueError(f"Producer is not registered: {spec.producer_key}@{spec.producer_revision}")
        raw = {value.key: frames[value.key] for value in spec.inputs if isinstance(value, RawInput)}
        items = {value.key: frames[value.key] for value in spec.inputs if isinstance(value, ItemInput)}
        from .pit import _general_snapshot_identity
        general_evidence = {
            key: receipt.evidence[key] for key, frame in raw.items()
            if "_snapshot_id" in frame.columns
        }
        if policy is not None and policy.history_observations is not None:
            from bagelquant_data import input_index
            calendar_alias = policy.calendar_input
            if calendar_alias not in general_evidence:
                raise ValueError("Finite-history calendar must be a complete General input")
            calendar_evidence = general_evidence[calendar_alias]
            calendar_request = cast(RawInput, requests[calendar_alias])
            calendar_frame = general_input_snapshot(raw[calendar_alias], through,
                snapshots=calendar_evidence["general_snapshots"], checks=calendar_evidence["checks"],
                strict=calendar_request.strict,
                include_historical_baseline=calendar_request.include_historical_baseline)
            with self._store.connect() as db:
                input_index.save_coordinates(db, receipt.digest, calendar_alias,
                    policy.calendar_field, policy.calendar_selector, calendar_frame)
        resolvers = {
            key: (lambda cutoff, frame=raw[key], evidence=evidence,
                  request=requests[key]: general_input_snapshot(
                      frame, cutoff, snapshots=evidence["general_snapshots"],
                      checks=evidence["checks"], strict=request.strict,
                      include_historical_baseline=isinstance(request, RawInput)
                      and request.include_historical_baseline,
                  ).lazy())
            for key, evidence in general_evidence.items()
        }
        general_boundaries = {
            _date_value(value["pit_date"])
            for evidence in general_evidence.values()
            for value in evidence["general_snapshots"]
            if value["mode"] != "initialize"
        } | {
            _date_value(value["pit_date"])
            for evidence in general_evidence.values() for value in evidence["checks"]
        }
        for key in items:
            general_boundaries.update(self._lake.inputs._empty_baseline_boundaries(receipt, key))
        final_output: dict[str, pl.DataFrame] = {}
        pending_output: list[tuple[str, str]] = []
        final_baseline = False

        def evaluate(raw_snapshots: dict, item_snapshots: dict, cutoff: date) -> pl.DataFrame:
            nonlocal final_baseline
            snapshots = {key: value.collect() for key, value in {**raw_snapshots, **item_snapshots}.items()}
            baseline = any("_baseline" in frame.columns and frame["_baseline"].fill_null(False).any() for frame in snapshots.values())
            # An empty initialization still carries unverified timing evidence;
            # no row exists on which to project its baseline marker.
            baseline = baseline or any(
                identity is not None and identity["baseline"]
                for key, evidence in general_evidence.items()
                for identity in [_general_snapshot_identity(
                    evidence["general_snapshots"], cutoff, checks=evidence["checks"],
                    strict=requests[key].strict,
                    include_historical_baseline=isinstance(requests[key], RawInput)
                    and cast(RawInput, requests[key]).include_historical_baseline,
                )]
            )
            # Nonempty snapshots already carry timing selected at this cutoff
            # over the admitted observation window, including a finite halo.
            # Reselecting the full receipt would reread unrelated history and
            # could propagate an unconsumed baseline into a verified tail.
            # Empty inputs have no row marker, so retain their original
            # transitive proof (whose parents may need a halo or General event).
            baseline = baseline or any(self._lake.inputs._baseline_at(receipt, key, cutoff,
                                       max_buffer_bytes=int(options.max_buffer_bytes - buffered))
                                       for key in snapshots if snapshots[key].is_empty()
                                       and self._lake.inputs._could_have_baseline(receipt, key))
            context = BuildContext(snapshots, start, end, cutoff, max_commit, baseline, completed_through)
            if producer:
                result = producer(context)
                frame = result.collect() if isinstance(result, pl.LazyFrame) else result
            else:
                frame = apply_transforms(snapshots[spec.inputs[0].key], spec.transforms, snapshots)
            frame = self._normalize(spec, frame).filter(pl.col("time").is_between(start, end))
            if policy is not None and policy.output_anchors:
                parts = []
                for alias in policy.output_anchors:
                    anchor = snapshots.get(alias)
                    if anchor is None:
                        raise ValueError("Output anchor lacks declared time/asset coordinates")
                    if anchor.is_empty():
                        parts.append(pl.DataFrame(schema={"time": pl.Date, "asset_id": pl.String}))
                        continue
                    if "asset_id" not in anchor.columns:
                        raise ValueError("Output anchor lacks declared time/asset coordinates")
                    axis = "source_time" if "source_time" in anchor.columns else "time"
                    if axis not in anchor.columns or "_snapshot_id" in anchor.columns:
                        raise ValueError("Output anchor requires by-date coordinates")
                    parts.append(anchor.select(pl.col(axis).cast(pl.Date).alias("time"),
                        pl.col("asset_id").cast(pl.String)))
                coordinates = pl.concat(parts).unique()
                if frame.select("time", "asset_id").join(coordinates,
                        on=["time", "asset_id"], how="anti").height:
                    raise ValueError("Producer output contains coordinates outside its declared output anchor")
            if cutoff == through:
                final_output["frame"] = frame.filter(pl.col("available_date") <= through)
                pending_output[:] = [(day.isoformat(), available.isoformat()) for day, available in
                    frame.filter(pl.col("available_date") > through).select("time", "available_date").unique().iter_rows()]
                final_baseline = baseline
            return frame.with_columns(pl.lit(baseline).alias("_build_baseline"))

        committed_rows = 0
        result_commit: int | None = None

        def publish_group(frame, baseline, *, executor, writer_buffer=0,
                          publication_bytes=None):
            before = committed_rows
            def published(rows, commit):
                nonlocal committed_rows, result_commit
                committed_rows = before + rows
                result_commit = commit
                if progress:
                    progress(ItemBuildReport(name, "running", committed_rows, result_commit, max_commit,
                        start, end, dependency_digest, receipt.receipt_id))
            return self._publish_proven(name, frame, historical_baseline=baseline,
                input_receipt=receipt, expected_definition_hash=definition_hash,
                writer_executor=executor, partition_workers=partition_workers,
                writer_buffer_bytes=writer_buffer, commit_batch_rows=options.commit_batch_rows,
                commit_batch_bytes=publication_bytes, cancelled=cancelled, on_commit=published)

        partition_workers = min(options.workers, options.max_in_flight or options.workers,
                                options.batch_size or options.workers)
        with ThreadPoolExecutor(max_workers=options.workers, thread_name_prefix="data-item") as executor:
            try:
                output_options = replace(options, max_buffer_bytes=options.max_buffer_bytes - buffered)
                writer_buffer = output_options.max_buffer_bytes // (options.workers + 1)
                publication_bytes = max(1, writer_buffer // 6)
                with closing(iter_computed_versions(raw=raw, items=items, start=start, end=through, evaluate=lambda raw, items: evaluate(raw, items, through), evaluate_at=evaluate, raw_resolvers=resolvers, extra_boundaries=sorted(general_boundaries), config=output_options, cancelled=cancelled, executor=executor)) as outputs:
                    for output in outputs:
                        if cancelled and cancelled():
                            raise CancelledError("DataItem build cancelled")
                        for baseline in (False, True):
                            if output.is_empty():
                                continue
                            group = output.filter(pl.col("_build_baseline") == baseline).drop("_build_baseline")
                            if group.is_empty():
                                continue
                            publish_group(group, baseline, executor=executor,
                                          writer_buffer=writer_buffer, publication_bytes=publication_bytes)
                        if progress:
                            progress(ItemBuildReport(name, "running", committed_rows, result_commit, max_commit, start, end, dependency_digest, receipt.receipt_id))
                        writer_buffer = 0
            except CancelledError:
                return ItemBuildReport(name, "cancelled", committed_rows, result_commit, max_commit, start, end, dependency_digest, receipt.receipt_id)
            if "frame" in final_output and self._store.manifest("items", name):
                existing = self.read(name, start=start, end=end, view="snapshot", as_of=through)
                keys = ["time", "asset_id"]
                # A later externally published version can dominate the causal
                # revisions just rebuilt above. Repair the latest affected view
                # at this frozen boundary, retaining the old historical version.
                expected = final_output["frame"].with_columns(pl.lit(final_baseline).alias("_baseline"))
                identity = [key for key in ("time", "asset_id", "value", "observation_date", "available_date", "_baseline")
                    if key in expected.columns]
                selected = existing.select(identity).collect().sort(keys)
                target = expected.select(identity).sort(keys)
                changed = (target.head(0) if _same_ordered_rows(target, selected, identity)
                    else _comparison_frame(target, identity).join(_comparison_frame(selected, identity),
                        on=identity, how="anti", nulls_equal=True))
                if changed.height:
                    repair = expected.join(changed.select(keys), on=keys, how="semi").drop("_baseline").with_columns(
                        pl.max_horizontal("time", pl.lit(through)).alias("version_available_date"))
                    try:
                        publish_group(repair, final_baseline, executor=executor)
                    except CancelledError:
                        return ItemBuildReport(name, "cancelled", committed_rows, result_commit,
                            max_commit, start, end, dependency_digest, receipt.receipt_id)
                existing_keys = existing.select(keys).collect()
                current_keys = final_output["frame"].select(keys)
                removed_keys = (existing_keys.head(0) if _same_ordered_rows(existing_keys, current_keys, keys)
                                else existing_keys.join(current_keys, on=keys, how="anti"))
                if removed_keys.height:
                    # Only actual withdrawals need the old values/extra columns.
                    removed = existing.join(removed_keys.lazy(), on=keys, how="inner",
                                            maintain_order="left").collect()
                    removed = removed.with_columns(pl.lit(None, dtype=scalar_dtype(spec.value_dtype)).alias("value"), pl.max_horizontal("time", pl.lit(through)).alias("version_available_date"))
                    try:
                        publish_group(removed, final_baseline, executor=executor)
                    except CancelledError:
                        return ItemBuildReport(name, "cancelled", committed_rows, result_commit,
                            max_commit, start, end, dependency_digest, receipt.receipt_id)
        with self._store.connect() as db:
            db.execute("begin immediate")
            cursor = db.execute("insert into item_builds(name,definition_hash,dependency_digest,start_date,end_date,input_commit,result_commit,status,created_at,frozen_receipt_id) values(?,?,?,?,?,?,?,'success',?,?)", (name, definition_hash, dependency_digest, start.isoformat(), end.isoformat(), max_commit, result_commit, datetime.now(UTC).isoformat(), receipt.receipt_id))
            _save_completion(db, cursor.lastrowid, pending=pending_output)
        return ItemBuildReport(name, "success", committed_rows, result_commit, max_commit, start, end, dependency_digest, receipt.receipt_id)

    def _input_requests(self, spec: DataItemSpec,
                        input_windows: Mapping[str, tuple[DateLike, DateLike]] | None):
        from dataclasses import replace
        requests = {value.key: replace(value, view="versions", fields=(), include_historical_baseline=True)
                    if isinstance(value, RawInput) else replace(value, view="versions") for value in spec.inputs}
        for key, window in (input_windows or {}).items():
            if key not in requests:
                raise ValueError(f"Unknown input window alias: {key}")
            first, last = (_date_value(value) for value in window)
            if last < first:
                raise ValueError("input window end precedes start")
            original = requests[key]
            if (original.start is not None and first < _date_value(original.start)
                    or original.end is not None and last > _date_value(original.end)):
                raise ValueError("input window cannot widen the declared dependency window")
            if isinstance(original, RawInput):
                if (original.observation_start is not None and first < _date_value(original.observation_start)
                        or original.observation_end is not None and last > _date_value(original.observation_end)):
                    raise ValueError("input window cannot widen the declared observation window")
                definition = self._store.get_dataset(original.source, original.dataset)
                if definition is None or json.loads(definition["spec_json"])["update_type"] != "by_date":
                    raise ValueError("input windows require by-date dependencies")
            requests[key] = replace(original, start=first, end=last)
        return requests

__all__ = ["ItemAPI"]
