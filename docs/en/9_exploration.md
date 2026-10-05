# Exploration

```python
from bagelquant_data import exploration

frame = lake.items.read("close").collect()
summary = exploration.overview(frame, coordinates=coordinates)
daily = exploration.coverage(frame, coordinates, by="time")
distribution = exploration.distribution(frame)
quantiles = exploration.quantiles(frame)
outliers = exploration.outliers(frame)
series = exploration.time_series(frame, "A")
section = exploration.cross_section(frame, "2020-01-02")
```

Functions distinguish null, NaN and infinity. Coverage denominators require an
explicit unique non-null `(time, asset_id)` grid, supporting sparse membership.
Numeric distribution/quantiles use finite values; categorical distributions
return frequencies. `profile_item` returns summary and Polars tables. Data
creates no charts, reports, web services or application state; Workbench presents
these results using its own UI and domain coordinates.
