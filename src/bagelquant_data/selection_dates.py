"""Row-local date selection on immutable provider payload keys."""
from __future__ import annotations

from datetime import date
from collections.abc import Mapping

import polars as pl


def date_range_expression(schema: Mapping[str, pl.DataType], selection: tuple[str, date, date]) -> pl.Expr:
    """Retain the same nullable Date/Datetime/compact-or-ISO date interpretation."""
    column, first, last = selection
    value = pl.col(column)
    if schema[column] == pl.Date or isinstance(schema[column], pl.Datetime):
        value = value.cast(pl.Date, strict=False)
    else:
        text = value.cast(pl.String)
        value = pl.coalesce(text.str.strptime(pl.Date, "%Y%m%d", strict=False),
            text.str.strptime(pl.Date, "%Y-%m-%d", strict=False))
    return value.is_between(first, last, closed="both")
