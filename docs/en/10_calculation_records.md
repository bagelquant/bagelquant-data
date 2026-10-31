# Calculation records

Ordinary reads consume typed original Parquet values. Freeze, plans, completion
checks and range identities use compact SQLite metadata without decoding history.
`raw.completion_statuses` and `items.completion_statuses` batch node flags in one
finite metadata view. Definitions, publication revisions, contiguous coverage,
unfinished updates and direct dependency state decide readiness.

`items.completion_read_context(names=...)` yields completion flags and keeps
subsequent metadata reads, including `completed_builds` and nested
`inputs.read_context(..., verify=False)`, in the same SQLite snapshot. It works
on a read-only lake and does not block competing Data writers. Later contexts
observe later commits. Use it for metadata-only readiness planning; consumer
reference publication uses the writable publication guards described below.

`inputs.completed_range(receipt, alias, start=..., end=...)` checks certified
Item coverage inside the selected frozen Source. Sparse rows and entirely empty
outputs can cover a complete range. The check uses original completion proofs,
captured producer parents, content and visibility ceilings; later completion
flags, builds and row-only ingests cannot extend that authority. A row-only or
unsupported visibility scope, including a native timing attestation, requires
authenticated complete coverage of the entire consumer range at or after that
original content and output-check boundary. Recognized `item_range` witnesses
retain their declared range checks. Malformed request containers, members or
declared ranges remain unproved. It returns
`False` for an unproved range, raises for damaged bindings, and rejects Raw
aliases. It reads metadata only, honors the enclosing read context's buffer and
cancellation limits, and creates no evidence. This proves completion in the
original Source; current readiness and byte integrity have separate checks.

Schema 9 stores each immutable Arrow schema once. Frozen inputs contain thin
headers and alias-to-Source references; shared metadata blocks retain one frozen
original segment/check snapshot across receipts. Live projections cannot replace it.

User metadata may use the key `schema_ipc` for arbitrary text. Only canonical
compact hexadecimal strings are pooled as schema bytes; other strings retain
their exact spelling. Registered schema references still validate their checksum
on explicit audit. Existing registered receipts remain unchanged; canonical
generated-schema encodings retain their identities.
`inputs.get` returns an explicit lazy `evidence` Mapping. `describe` includes
small per-alias definition/schema identities, row/batch counts, bounds and Source
references. Neither operation expands historical metadata. Unchanged currentness
uses recorded alias-local revisions and complete producer authorities; unknown
or changed evidence keeps the conservative metadata comparison.

Scoped `selection_equivalent` comparisons use captured observation and availability
bounds to skip disjoint immutable branches before loading segment headers.
Missing bounds retain candidates, and optional Raw summary bounds apply separately
to each batch. The same visibility and exact-summary fallback still decide equality.
This reduces repeated metadata decoding without reading original values or building
indexes. A scoped proof does not audit skipped descendants; explicit `inputs.verify`
still checks all retained evidence.

For non-strict scalar snapshots, `records_only=True` can also prove equality
from unchanged range metadata without preparing exact summaries. This requires
complete visibility of the selected interval, using captured availability bounds
or the original segment's maximum availability delay, plus unchanged relevant
visibility witnesses and their check timing. Equal cutoffs alone do not
justify ignoring changed timing evidence. Partial visibility or unsupported
witnesses retain the exact-summary fallback; a missing proof remains unknown.
Raw `start`/`end` and `observation_start`/`observation_end` are intersected for
this comparison, so widening an enclosing request does not invalidate an
unchanged prefix. Frozen requests, ordinary range identities and registered
selection descriptors retain their original identity. Metadata admission uses
the operation's buffer and cancellation checks without loading an unrelated
optional summary Source.

`register_selection_set(receipt, descriptors)` validates complete public
descriptors and returns `{id, digest, source, count}`. `describe_selection_set`
checks that header and original Source without resolving historical descriptors.
`read_selection_set(reference, keys=[...])` reads only selected descriptor keys;
omitting keys explicitly reads the whole set for preparation or audit. Obtain
keys only from `inputs.selection_descriptor_identity`: its `selection.handle.v1`
hash binds every scope/proof field except enclosing `source` and
`requested_cutoff`. Bodies omit those same two fields; the original set Source
and retained explicit `as_of` reconstruct the exact requested boundary. Canonical
cutoff, original `proof_source`, strictness and selected evidence stay bound.
Different explicit `as_of` values remain distinct. Complete 64-member chunks
share their indexes across Sources, so a cutoff/append does not copy historical
descriptor bodies or index rows. Earlier selection-set formats require a fresh
registry; there is no automatic conversion. `inputs.verify` checks shared descendants, complete set
membership and proofs, together with retained original byte evidence.

`inputs.describe`, `request_identity`, `range_identity`, `selection_identity`
and `selection_descriptors` are read-only. Ordinary range tokens are always
available for supported typed selections and remain stable across optional
maintenance. New segments outside a consumed window and changes to unconsumed
fields preserve identity. Compact date/asset bounds are conservative for sparse
coordinate intersections; exact equality may require explicit selected summaries.

`prepare_coordinate_selector` retains only caller-supplied Domain coordinates;
`prepare_range_selections` registers small metadata summaries and preserves their
original Source reference. Each finite group expands parent metadata only for
its original proof aliases, including when a consumer uses a different alias.
Unconsumed aliases stay lazy; Source checksum checks, closing descriptor checks
and transactional publication still apply. This does not replace whole-Source
currentness or original-byte audits. `index_selection(s)` and `prepare_general_projections`
explicitly compare admitted original typed selections. Coordinate identities
require an explicitly prepared small date axis; missing axes return `None`.
These operations never create historical row indexes.

`inputs.verify` checks immutable receipt bindings, exact original value and
visibility file bytes, typed schemas and bounds, and registered optional summaries.
Cancellation closes readers and discards partial operation caches. Resource limits
do not change numerical identity; insufficient admission raises `MemoryError`.
Missing or damaged originals require a trusted backup.

`items.completion_guard(names=...)` yields the same batch flags under a finite
Data publication fence and requires a writable lake. Consumers can publish
their own guarded reference CAS inside that view. Kernels, providers and Data
writes belong outside the guard. `inputs.publication_guard(receipts)` likewise
requires writable admission and fences Data writers through the consumer CAS.
