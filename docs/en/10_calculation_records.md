# Calculation records

Ordinary reads consume typed original Parquet values. Freeze, plans, completion
checks and range identities use compact SQLite metadata without decoding history.
`raw.completion_statuses` and `items.completion_statuses` batch node flags in one
finite metadata view. Definitions, publication revisions, contiguous coverage,
unfinished updates and direct dependency state decide readiness.

Schema 9 stores each immutable Arrow schema once. Frozen inputs contain thin
headers and alias-to-Source references; shared metadata blocks retain one frozen
original segment/check snapshot across receipts. Live projections cannot replace it.
`inputs.get` returns an explicit lazy `evidence` Mapping. `describe` includes
small per-alias definition/schema identities, row/batch counts, bounds and Source
references. Neither operation expands historical metadata. Unchanged currentness
uses recorded alias-local revisions and complete producer authorities; unknown
or changed evidence keeps the conservative metadata comparison.

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
original Source reference. `index_selection(s)` and `prepare_general_projections`
explicitly compare admitted original typed selections. Coordinate identities
require an explicitly prepared small date axis; missing axes return `None`.
These operations never create historical row indexes.

`inputs.verify` checks immutable receipt bindings, exact original value and
visibility file bytes, typed schemas and bounds, and registered optional summaries.
Cancellation closes readers and discards partial operation caches. Resource limits
do not change numerical identity; insufficient admission raises `MemoryError`.
Missing or damaged originals require a trusted backup.

`items.completion_guard(names=...)` yields the same batch flags under a finite
Data publication fence. Consumers can publish their own guarded reference CAS
inside that view. Kernels, providers and Data writes belong outside the guard.
