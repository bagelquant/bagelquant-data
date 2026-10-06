# Data AI workflow and rule routes

Always read [development rules](rules/development.md). Then load the affected
topic rules; rules are authoritative instructions and linked docs explain use.

| Task topic | Owner rules | Relevant docs |
| --- | --- | --- |
| Dataset/provider mapping, availability cutoff, ingestion, scopes, retries, pagination/resources | [Ingestion and updates](rules/ingestion-updates.md) | [Datasets](../docs/en/3_datasets.md), [Sources](../docs/en/4_sources.md), [Updates](../docs/en/5_updates.md) |
| Raw/DataItems, classification, Parquet/SQLite commit, frozen/PIT inputs, schema, recovery and integrity | [Storage and recovery](rules/storage-recovery.md) | [Queries](../docs/en/6_queries.md), [Operations](../docs/en/7_operations.md), [DataItems](../docs/en/8_items.md), [Exploration](../docs/en/9_exploration.md) |
| Public facade or data-boundary change | Both owner rules and [development](rules/development.md) | [Overview](../docs/en/1_overview.md), [Quickstart](../docs/en/2_quickstart.md) |

Data is independently versioned and imports no BagelQuant package. Its target
owns Raw and neutral DataItems, categories, storage/query/versions/PIT, frozen
input receipts and recovery. Version 0.7 implements the stage-2 public facades
with explicit data_meta_path/lake_path and one Data SQLite (schema 5). Data's
guides describe this implemented contract; real database/service cutover remains
separate. Exploration returns pure statistics and Polars tables.

Workbench remains the downstream composition root for China semantics, app
metadata, research governance and task orchestration through public package APIs.
Data owns generic dataset mechanisms; Core owns Domain/Panel conversion and
numerical artifact storage, BT owns account/evaluation artifact storage. Data
does not become a second authority for their results. Keep provider specifics
at the integration edge and pass neutral frames/schema/availability/identity
evidence across the Data/Core boundary; neither package imports the other.
Data docs stay in this repository. The website links to package documentation
and does not collect or republish it.

Integration discovery and standalone fallback are in [AGENTS.md](../AGENTS.md).
After verifying an integration root, read its
`.ai/rules/contracts/package-boundaries.md` for cross-package work and use its
`.ai/workflow.md` and root task CLI;
the bilingual usage guide is `docs/ai-workflow.md` / `docs/zh-CN/ai-workflow.md`
in that verified root. Rules/templates are versioned; actual task records are
local, ignored, and owned only by the workspace root. A clone does not restore them.
