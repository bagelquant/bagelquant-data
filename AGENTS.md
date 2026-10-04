# Data agent entry

This repository owns the provider-neutral Raw lake, dataset declarations,
coverage, PIT queries, explicit updates and provider adapters. The refactor target
also assigns neutral generic derived datasets, versions and frozen input evidence
to Data; that expansion is pending stage 2, not an implemented API.
Data has no BagelQuant package dependency; never import Core, BT or Workbench.
The current library-only facades are `lake.admin`, `lake.update` and `lake.query`;
stage 2 may replace their contracts without compatibility layers.

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
- Schema v4 is a fresh-lake hard cut; do not add migration/compatibility readers,
  adopt orphan files or substitute current provider bytes for historical evidence.
- Expose neutral data and evidence; Core owns Domain/Panel conversion and numerical
  artifacts, BT owns account/evaluation artifacts. Do not duplicate their storage.
- Workbench supplies China semantics and research intent through public APIs;
  it owns app metadata/governance/orchestration, not generic data mechanisms.

For formal work in an integrated workspace, create/resume a root `.ai/tasks/`
record using the workspace CLI. Discover the workspace with
`git rev-parse --show-superproject-working-tree`. If empty, inspect checkout
ancestors as candidates. Accept only a candidate that is its own Git root,
declares the six component paths in `.gitmodules` and has their index gitlinks
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
