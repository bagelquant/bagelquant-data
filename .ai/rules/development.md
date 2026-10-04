# Data development rules

## Preparation and boundaries

- Inspect `git status --short --branch` including untracked changes before editing;
  read README, manifest and affected docs. Work in the owning repository and preserve
  unrelated user edits. Never remove Git metadata, merge histories or absorb repos.
- Use Python 3.13 and `uv`. Choose cross-platform paths/APIs; account for separators,
  casing, line endings, permissions, shell syntax and environment conventions on
  Windows/macOS. Use `pathlib`; never persist developer-specific absolute paths.
- Data has no BagelQuant package dependency. Keep provider adapters at the
  integration edge; storage/query/ledger are provider-neutral. Declare any
  necessary dependency in the owning manifest.

## Implementation

- Prefer the smallest complete system and existing lower-level primitives. Keep
  one authoritative API/model/state/execution path per concern. Delete dead code
  rather than add adapters, aliases, flags or parallel implementations.
- Do not keep deprecated readers/routes/DSL aliases or migration shims unless
  an explicit compatibility window is requested. Add abstractions, dependencies,
  configuration or persisted state only for a current concrete responsibility.
- Keep dependencies and boundaries visible. Remove a feature's exports, tests,
  docs, configuration and generated references together; preserve real shared
  data, recovery evidence, user work, credentials, environments and caches.
- Use deterministic composable logic, descriptive names and typed public
  boundaries; follow surrounding structured docstrings. Validate boundaries
  with actionable errors and never silently swallow exceptions.
- Use `logging`, not `print`, in library logic. Prefer vectorized Polars/NumPy;
  row-wise loops need justification and measurement. Ordering/grouping are explicit.
- Preserve stable `(time, asset_id)` keys, sparse/dense and missing-membership
  semantics. Distinguish observation, information cutoff, signal-effective
  and execution time; never replace PIT joins with latest-value joins.
- Performance changes preserve contracts unless explicitly changed/documented.
  Formula changes need hand-checkable examples and regression tests; compare
  optimized output with a simple reference where practical. Test relevant sparse
  calendars, listings/delistings, gaps, missing/duplicate keys and empty frames.
- Tests use temporary lake roots and fake providers; never read/mutate the
  workspace's real data root or consume provider quota for validation.

## Verification and delivery

- Run `uv run pytest`, `uv run pyright` and `uv run ruff check .` for code changes;
  use focused checks for docs-only work. Generate referenced output from its
  source/generator; never manually edit generated documents or API artifacts.
- State cross-repo public contract changes. Keep edits independently coherent,
  update bounds/versions only when required, test Data first then all affected
  consumers, and report each repository separately.
- Major architecture changes update applicable AGENTS and owner rules in the
  same change; update root instructions if cross-repository boundaries change.
- Do not commit caches, credentials, databases, provider data, environments,
  build outputs, research artifacts or local task records. Use stable `main`
  and short-lived single-purpose branches; honor session branch instructions.
  Conventional Commit summaries are imperative and at most 72 characters.
- Commit/push/PR/merge/release/publication/deployment/service installation,
  provider calls, real-data updates and governance transitions each require an
  explicit request. Commit components separately before authorized gitlink updates.
  Package publication verifies version/build/registry/credentials/version absence;
  report exact uploaded versions/artifacts.
- Report behavior/why, repository-specific checks/results, unrun checks and reasons,
  and contract/version/data/operational caveats. Distinguish existing failures from
  introduced failures; never claim a check passed unless run.
