# Dataset declarations

A dataset declares its provider API, date parameters, source date fields, business
keys, date kind, fixed parameters, parameter expansion, and transport options.

```python
from bagelquant_data import DatasetSpec

income = DatasetSpec(
    "income", "by_daily", source="tushare", source_api="income_vip",
    date_kind="calendar", date_params=("f_ann_date",),
    source_time_fields=("f_ann_date",),
    primary_key_extra=(
        "ann_date", "end_date", "report_type", "comp_type", "end_type", "update_flag"
    ),
    nullable_primary_key_extra=("ann_date", "comp_type", "end_type"),
    field_mappings={"f_ann_date": "time", "ts_code": "asset_id"},
    availability_timezone="Asia/Shanghai", availability_day_offset=-1,
    request_options={"pagination": "offset", "page_size": 1000, "max_pages": 10000},
)
```

`date_kind="trading"` requires a General calendar. `calendar` dates include
weekends. Multiple `date_params` produce independent date/parameter scopes.
`source_time_fields` uses the first non-null source date; original fields remain
available. The business key is `source_time`, `asset_id`, and `primary_key_extra`.
`nullable_primary_key_extra` explicitly names provider qualifiers whose missing value
still participates in identity; time, asset, and every other key remain strict. The
version key adds ingestion time and commit sequence.

`source_api_param_sets` expands list values as a Cartesian product. A registered
General catalog can supply `parameter_dataset`, `parameter_field`, and
`parameter_name`; this creates date × parameter requests and no asset watermark.
`request_discovery` can discover parameter values through a declared provider API.
Transport options are definition data, including pagination and null-payload rules.

General needs no numerical key and stores each changed complete snapshot from an
explicit update. An unchanged refresh records only a check. A failed variant or page
cannot replace its last complete snapshot.
The old asset update type, year/bucket layout, and revision-watermark options are
unsupported. TOML registration and form-generated declarations use the same validator.
