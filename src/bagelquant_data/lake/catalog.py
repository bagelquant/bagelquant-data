"""Lake catalog interfaces."""

from __future__ import annotations

from dataclasses import dataclass, field

from bagelquant_data.lake.snapshot import SnapshotRef


@dataclass(slots=True)
class LakeCatalog:
    """In-memory snapshot catalog."""

    _snapshots: dict[tuple[str, str], SnapshotRef] = field(default_factory=dict)

    def put(self, snapshot: SnapshotRef) -> None:
        """Store the latest snapshot for a dataset."""

        self._snapshots[(snapshot.source, snapshot.dataset)] = snapshot

    def latest(self, source: str, dataset: str) -> SnapshotRef | None:
        """Return the latest snapshot for a dataset."""

        return self._snapshots.get((source, dataset))
