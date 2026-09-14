# PIT queries

```python
# Latest complete observation versions, restricted to numerical source dates.
latest = lake.query.query("daily", source="tushare",
    observation_start="2026-01-01", observation_end="2026-01-31")
# What was known at the calculation cutoff, including later-month revisions.
known = lake.query.query("daily", source="tushare", as_of_date="2026-09-09",
    observation_start="2026-01-01", observation_end="2026-01-31")
versions = lake.query.query("daily", source="tushare", view="versions")
# Observation-axis values for a numerical consumer.
observations = lake.query.observations("daily", source="tushare", start="2026-01-01")
```

`source_time` is the observation/announcement date; `time` is availability.
`start` and `end` filter availability after version resolution. `observation_start`
and `observation_end` filter the independent source axis and prune manifests using
source-date bounds. `as_of_date` filters visibility before selecting the latest
business-key version. `ingested_before` accepts a timezone-aware timestamp.
Fields and numerical filters must be applied after version selection.

`view="history"` selects each observation's version known on its own source date.
`observations()` also restores the ordinary numerical `time` axis; an explicit
`as_of_date` instead resolves its whole input window at that cutoff.
`lake.query.frozen()` pins a visible commit ceiling for one computation.
`version_evidence()` supplies immutable visible batch identities without numerical
reads. Check timestamps do not participate in these identities.

General uses `query_general()`: latest complete snapshot by default, or an explicit
`as_of_date`, `snapshot_id`, or `ingested_before`. `view="versions"` audits all
eligible snapshots. `snapshots()` lists complete snapshot metadata including empty
snapshots. Historical initialization is a baseline available for historical reads;
subsequent snapshots never backdate changes or merge rows from different snapshots.
