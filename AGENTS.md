# Data agent entry

This repository owns provider-neutral Raw and typed DataItems, declarations,
category trees, coverage, versions, PIT, frozen input receipts and local recovery.
Data has no BagelQuant package dependency; never import Core, BT or Workbench.
The library-only facades are `lake.catalog`, `lake.raw`, `lake.items`,
`lake.integrity` and `lake.inputs`; pure statistics live in `exploration`.
Read-only Raw/DataItem update plans select initialization, frozen-range resume and incremental work.
Both `data_meta_path` and `lake_path` are mandatory caller-owned paths.
Catalog declaration batches are atomic and receipt-backed; integrity exposes actual storage usage and frozen temporary cleanup without deleting history.
DataItem updates accept caller-declared input windows pinned in frozen receipts; new scoped freezes preserve late revisions and legacy receipt identity.
Large complete by-date baseline checks may freeze exact batch-bound seals after bounded tuple equality proof; original witnesses remain immutable.
Integrity can reopen premature Raw baseline completion at unchanged bounds/hash only before incremental evidence; the initializer retains historical receipts.
Stable `inputs.request_identity` uses original alias metadata, independent of optional indexes.
Ordinary publication/currentness reads registered metadata and optional selection indexes;
consumed reads remain typed. Full original-byte/IPC checks belong to explicit
`inputs.verify`, which also compares derived indexes with original evidence.
Missing/version-mismatched indexes mean unknown; never scan IPC to prove currentness.
`inputs.index_plan/build_index` explicitly builds historical indexes without changing receipts.
Fresh lakes index new batches; opening old writable/read-only lakes never backfills history.

Before work, read [`.ai/README.md`](.ai/README.md), mandatory development rules,
affected topic rules, [`README.md`](README.md), [`pyproject.toml`](pyproject.toml)
and relevant local docs. Inspect tracked and untracked Git changes first.
Keep this entry short; detailed owner contracts live under `.ai/rules/`.

- Use Python 3.13 and `uv`; run commands from this repository.
- Preserve unrelated work, Git metadata, credentials, environments and data.
- Tests use isolated temporary lakes and fake providers; never read or mutate
  the workspace's real data root or consume provider quota for validation.
- Do not commit, push, create PRs, merge, release, deploy, install services,
  call providers, update real data or change governance unless explicitly requested.
- Coverage is commit-backed truth; queries never submit updates or call providers.
- Schema 7 is a fresh-lake hard cut; do not add migration/compatibility readers,
  adopt orphan files or substitute current provider bytes for historical evidence.
- Default `DataLake.inspect` checks committed schema/lake binding without changing
  original files or SQLite sidecars. Ordinary read-only queries use normal WAL
  coordination; do not replace them with immutable main-file reads that omit WAL.
  Explicit runtime inspection/open uses ordinary WAL coordination without copies.
- Expose neutral data and evidence; Core owns Domain/Panel conversion and numerical
  artifacts, BT owns account/evaluation artifacts. Do not duplicate their storage.
- Workbench supplies China semantics and research intent through public APIs;
  it owns app metadata/governance/orchestration, not generic data mechanisms.
- Workbench owns global scheduling, hardware detection, admission and runtime
  policy. Data uses explicit local limits, one pool and serial metadata publication;
  default execution is serial. Never infer worker counts from machine resources.

For formal work in an integrated workspace, create/resume a root `.ai/tasks/`
record using the workspace CLI. Discover the workspace with
`git rev-parse --show-superproject-working-tree`. If empty, inspect checkout
ancestors as candidates. Accept only a candidate that is its own Git root,
declares the four component paths in `.gitmodules` and has their index gitlinks
(mode `160000`); this checkout must match its exact declared relative owner path.
For every candidate, also verify
root `AGENTS.md`, `.ai/README.md`, `.ai/workflow.md`, `.ai/rules/workspace.md`
and `scripts/ai.py` exist. Read root workflow and workspace rules; load
cross-repository contracts only for affected topics. Never guess parent paths.
In a standalone checkout, follow these local rules and record plan, validation
and handoff in the conversation; do not create a component task directory.
Plan/read-only mode never writes task records or implementation files.

Validate code changes with `uv run pytest`, `uv run pyright` and `uv run ruff check .`.
Report affected contracts, checks/results, unrun checks and next steps honestly.

`inputs.verify` also accepts a nonempty finite sequence of original frozen
receipts. Each root identity/digest and every dependency edge are checked;
shared parents and original batches are checked once within this invocation.
The multi-root report lists original root IDs/digests and aggregate counts.
There is no new aggregate receipt or cross-call validity cache; a later call
rechecks bytes. `inputs.is_current(..., config=...)` separately selects indexed metadata with explicit admission and context budget inheritance.
`inputs.read_context` shares deeply immutable original metadata and a same-thread
SQLite read view for a finite operation; entry reads metadata; `verify=True` explicitly audits bytes.
External objects supply only ID/digest. Context-local verify reuses checked batch
keys/graph summaries while checking every root digest; no frame/validity cache survives exit.
`inputs.read` narrows inclusive observation windows using captured bounds only;
verify retains every original batch and supports progress/cancellation during bounded decode/IPC checks.
`inputs.window_read_supported` plans physical windows from captured bounds only.
`integrity.snapshot` creates stable independent main/WAL/current-file copies with
an explicitly unverified report, preserving originals/sidecars. Explicitly audit required original inputs when checking backup integrity; strict backup remains unchanged.
