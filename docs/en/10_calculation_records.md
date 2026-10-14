# Calculation records

Ordinary reads consume typed original Parquet values. Freeze, plans, completion
checks and range identities use compact SQLite metadata without decoding history.
`raw.completion_statuses` and `items.completion_statuses` batch node flags in one
finite metadata view. Definitions, publication revisions, contiguous coverage,
unfinished updates and direct dependency state decide readiness.

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
