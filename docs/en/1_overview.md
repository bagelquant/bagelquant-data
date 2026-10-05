# Overview

Data owns the provider-neutral lake: Raw, DataItems, classification, schemas,
versions, coverage, frozen input evidence and recovery. Its five facades are
`catalog`, `raw`, `items`, `integrity` and `inputs`; descriptive functions live in
`bagelquant_data.exploration`. The package has no Core, BT or Workbench dependency.

Open with explicit `data_meta_path` and `lake_path`. The former is one SQLite
file; the latter holds immutable Parquet generations under `raw/<source>/<dataset>/`
and `items/<name>/`, followed by `year=YYYY/month=MM/`. File references are relative
to the matching lake. Callers may relocate the metadata and lake together while
preserving their relative relationship. Read-only opens validate existing state
and neither initialize nor recover anything.

`general` represents complete snapshots. `by_date` represents declared daily
date/parameter scopes. DataItems are neutral long tables with `time: Date`,
`asset_id: String` and a declared scalar `value`; selected keys are unique and
ordered. Classification does not determine stable dataset identity or file paths.

Data accepts explicit local execution limits, progress and cancellation. Defaults
are serial. Workbench owns hardware detection, global scheduling, admission and
native thread budgets. Core computes external producer results in memory; Data
publishes neutral results once. Core and BT keep their own numerical/result artifacts.

Version 0.7 uses metadata schema 5. Reject old or unversioned databases; no migration,
provider refetch, orphan adoption or automatic history deletion is performed.
Real database/service cutover belongs to the separately authorized operations stage.
