# Data agent entry

This repository owns provider-neutral Raw and typed DataItems, declarations,
category trees, coverage, versions, PIT, frozen input receipts and local recovery.
Data has no BagelQuant package dependency; never import Core, BT or Workbench.
The library-only facades are `lake.catalog`, `lake.raw`, `lake.items`,
`lake.integrity` and `lake.inputs`; pure statistics live in `exploration`.
Read-only update plans require actual new/affected ranges and record-only prerequisites; `items.recertify` admits only proven no-op reuse without producers; follow update-rule acceptance.
Both `data_meta_path` and `lake_path` are mandatory caller-owned paths; disjoint empty sparse groups share one fenced Source check and atomic completion publication.
Catalog declaration batches are atomic and receipt-backed; integrity exposes storage usage and frozen temporary cleanup without deleting history. Conditional `items.register(expected_definition_hash=...)` fences one captured active declaration; it does not make several restores atomic or prove original recovery evidence.
DataItem updates pin caller input windows in frozen receipts, preserving revisions and legacy identity. `items.resume_update` recovers uncertified first builds from original Sources; completed/lost authority blocks.
Independent ready Item batches share bounded original/key/visibility preparation;
same-date initialization resumes certified windows under its original Source and
cutoff, and publishes whole completion only after all windows. Other producer
policies retain their declared input requirements and explicit buffer admission.
`items.completed_builds` exposes whole-range authorities; `inputs.publication_guard` fences Data writers through consumer CAS without audit/maintenance.
Schema 9 retains one immutable Parquet body and shared segment/check/schema headers; selection handles bind immutable scope/PIT proof, with Source/requested cutoff in set bindings and shared chunk indexes across append.
Frozen receipts are thin references; their public evidence Mapping is explicitly
lazy. get/describe/request_identity never expand history. Registered selection
sets expose header-only validation and keyed reads; audits traverse all originals.
Node completion rows prove successful contiguous coverage, definitions and direct
dependency revisions; failed/partial updates cannot advance success flags.
`integrity.update_owner` binds Raw/native writers to one workflow owner; explicit
abandonment requires verified publisher exit and never relies on heartbeat age.
`integrity.item_removal_plan/remove_items` explicitly discard exact unreferenced
Item histories; retained references block removal and shared upstreams survive.
Ordinary freeze, plans, currentness and range/selection identities read metadata
only. Missing exact optional summaries permit conservative metadata range tokens;
optional maintenance never changes ordinary identity. Coordinate axes and caller
Domain selectors are small explicit summaries, never historical row indexes.
`inputs.prepare_range_selections` registers compact consumed scopes;
`index_selection(s)` and `prepare_general_projections` explicitly admit exact typed
comparisons where conservative range evidence cannot prove equality.
`inputs.verify` audits exact original Parquet bytes, schema, bounds and saved
summaries. Missing/damaged originals require a trusted backup. Schema 8 databases
are rejected; there is no compatibility reader, Arrow body replica or row-proof
backfill. `index_record_lookups` adds metadata access indexes only.

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
- Schema 9 is a fresh-lake hard cut; do not add migration/compatibility readers,
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

Validate with `uv run pytest`, `uv run pyright` and `uv run ruff check .`; report actual results and unrun checks.

Schema-nine completion flags and range identities use metadata only. Read the
storage rule for exact single-body Parquet, visibility, frozen/PIT, optional
compact summary and backup-required recovery contracts. Retired per-row indexes
and Arrow-body backup APIs must not be reintroduced.
