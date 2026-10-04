# Data AI workflow and rule routes

Always read [development rules](rules/development.md). Then load the affected
topic rules; rules are authoritative instructions and linked docs explain use.

| Task topic | Owner rules | Relevant docs |
| --- | --- | --- |
| Dataset/provider mapping, availability cutoff, ingestion, scopes, retries, pagination/resources | [Ingestion and updates](rules/ingestion-updates.md) | [Datasets](../docs/en/3_datasets.md), [Sources](../docs/en/4_sources.md), [Updates](../docs/en/5_updates.md) |
| Parquet/SQLite commit, frozen/PIT query, schema, recovery, integrity/quarantine | [Storage and recovery](rules/storage-recovery.md) | [Queries](../docs/en/6_queries.md), [Operations](../docs/en/7_operations.md), [Chinese PIT overview](../docs/zh-CN/1_overview.md) |
| Public facade or data-boundary change | Both owner rules and [development](rules/development.md) | [Overview](../docs/en/1_overview.md), [Quickstart](../docs/en/2_quickstart.md) |

Data is independently versioned and imports no BagelQuant package. Workbench is
the downstream composition root and owns Raw/DataItem orchestration, market
semantics, scope reset policy and publication/lifecycle decisions. Keep generic
storage/query/ledger mechanics here and provider specifics at the integration edge.
Data docs are collected by the website from GitHub default branches rather than
workspace gitlinks; edit package docs here, not in generated website content.

Integration discovery and standalone fallback are in [AGENTS.md](../AGENTS.md).
After verifying an integration root, use its `.ai/workflow.md` and root task CLI;
the bilingual usage guide is `docs/ai-workflow.md` / `docs/zh-CN/ai-workflow.md`
in that verified root. Rules/templates are versioned; actual task records are
local, ignored, and owned only by the workspace root. A clone does not restore them.
