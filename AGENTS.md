# Data agent entry

This repository owns provider-neutral Raw and typed DataItems, declarations,
category trees, coverage, versions, PIT, frozen input receipts and local recovery.
Data has no BagelQuant package dependency; never import Core, BT or Workbench.
The library-only facades are `lake.catalog`, `lake.raw`, `lake.items`,
`lake.integrity` and `lake.inputs`; pure statistics live in `exploration`.
Both `data_meta_path` and `lake_path` are mandatory caller-owned paths.
Catalog declaration batches are atomic and receipt-backed; integrity exposes
actual storage usage and explicit frozen temporary cleanup without deleting history.

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
- Schema 6 is a fresh-lake hard cut; do not add migration/compatibility readers,
  adopt orphan files or substitute current provider bytes for historical evidence.
- `DataLake.inspect` checks current committed schema/lake binding without changing
  original files or SQLite sidecars. Ordinary read-only queries use normal WAL
  coordination; do not replace them with immutable main-file reads that omit WAL.
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
