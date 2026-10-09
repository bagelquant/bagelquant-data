# Calculation records

Ordinary reads trust registered immutable metadata and consume typed values without byte/content rehashing. Explicit audits retain original integrity checks and compare present current-version derived indexes to original evidence. New outputs retain canonical hashes.

Public owner APIs: `inputs.describe / request_identity / selection_identity / index_plan / build_index / verify`. Index maintenance is explicit, frozen-plan based and cancelable; normal opens never backfill or rewrite historical receipts/manifests. Missing/partial/version-mismatched derived indexes provide no selection/interval proof. Supported authoritative metadata exact hits and covering-parent shortcuts remain valid. Admission limits do not change numerical identity.

Full `verify` uses its explicit resource options, or inherits the active read context. If selection-summary auditing cannot fit the admitted buffer, it raises `MemoryError` rather than reporting a complete audit or increasing the budget. Original-byte transport can spill independently of this selection working-set requirement.

`inputs.request_identity(receipt, alias)` hashes original registered alias request, cutoff and complete evidence metadata. It excludes enclosing receipt IDs/digests/global counters and optional indexes. It is stable across index maintenance and unrelated root aliases; alias-local checks and parent links conservatively invalidate. This full-request token is separate from exact selected-window proof.
