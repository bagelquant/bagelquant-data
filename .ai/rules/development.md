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

## Package target and staged refactor

- Target ownership: Raw and neutral generic derived datasets, their storage,
  queries, versions, PIT selection and frozen input evidence belong to Data.
  Current Raw APIs exist; the expanded derived/frozen-input contracts and
  generic mechanisms still in Workbench are pending the Data refactor, stage 2.
- Return neutral frames plus schema, availability and immutable identity evidence;
  never expose Core types or import Core. Core owns generic Domain/Panel
  conversion and numerical artifacts; BT owns account/evaluation artifacts.
  Do not add a second canonical store for either package's results.
- Data freezes selected input versions/evidence. Workbench freezes submitted
  research definitions, China semantics and backend receipt references; it owns
  app metadata/governance/task orchestration, not input bytes or generic proofs.
- Follow rules -> Data -> Core -> BT -> Workbench -> new database/service restart.
  Stage 1 changes instructions only; concrete APIs and storage schemas are deferred
  to their owner stages. Breaking refactors remove old paths without compatibility.

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
- For a package refactor, name one owner per capability/artifact and prove the
  public API works without Workbench using temporary roots and fake providers.
  Check dependency direction, remove superseded paths when authority moves,
  and verify frozen input integrity separately from research submission metadata.
  A missing backend API is work for its owner, not a generic Workbench workaround.
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
