# Data AI workflow and rule routes

Always read [development rules](rules/development.md). Then load the affected
topic rules; rules are authoritative instructions and linked docs explain use.

| Task topic | Owner rules | Relevant docs |
| --- | --- | --- |
| Dataset/provider mapping, availability cutoff, ingestion, scopes, retries, pagination/resources | [Ingestion and updates](rules/ingestion-updates.md) | [Datasets](../docs/en/3_datasets.md), [Sources](../docs/en/4_sources.md), [Updates](../docs/en/5_updates.md) |
| Raw/derived datasets, Parquet/SQLite commit, frozen/PIT input evidence, schema, recovery, integrity/quarantine | [Storage and recovery](rules/storage-recovery.md) | [Queries](../docs/en/6_queries.md), [Operations](../docs/en/7_operations.md), [Chinese PIT overview](../docs/zh-CN/1_overview.md) |
| Public facade or data-boundary change | Both owner rules and [development](rules/development.md) | [Overview](../docs/en/1_overview.md), [Quickstart](../docs/en/2_quickstart.md) |

Data is independently versioned and imports no BagelQuant package. Its target
owns Raw and neutral generic derived datasets, storage/query/versions/PIT and
frozen input evidence. The derived-data expansion and relocation of generic
input mechanisms from Workbench are pending stage 2; current guides describe
the existing Raw lake, not those future APIs or schemas.

Workbench remains the downstream composition root for China semantics, app
metadata, research governance and task orchestration through public package APIs.
Data owns generic dataset mechanisms; Core owns Domain/Panel conversion and
numerical artifact storage, BT owns account/evaluation artifact storage. Data
does not become a second authority for their results. Keep provider specifics
at the integration edge and pass neutral frames/schema/availability/identity
evidence across the Data/Core boundary; neither package imports the other.
Data docs are collected by the website from GitHub default branches rather than
workspace gitlinks; edit package docs here, not in generated website content.

Integration discovery and standalone fallback are in [AGENTS.md](../AGENTS.md).
After verifying an integration root, read its
`.ai/rules/contracts/package-boundaries.md` for cross-package work and use its
`.ai/workflow.md` and root task CLI;
the bilingual usage guide is `docs/ai-workflow.md` / `docs/zh-CN/ai-workflow.md`
in that verified root. Rules/templates are versioned; actual task records are
local, ignored, and owned only by the workspace root. A clone does not restore them.
