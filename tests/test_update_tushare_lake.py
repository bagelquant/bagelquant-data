from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "update_tushare_lake.py"
SPEC = importlib.util.spec_from_file_location("update_tushare_lake", SCRIPT_PATH)
assert SPEC is not None
update_tushare_lake = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = update_tushare_lake
SPEC.loader.exec_module(update_tushare_lake)

TableProgress = update_tushare_lake.TableProgress
format_seconds = update_tushare_lake.format_seconds
render_bar = update_tushare_lake.render_bar


@dataclass(frozen=True, slots=True)
class Job:
    table: str


@dataclass(frozen=True, slots=True)
class Report:
    jobs: tuple[Job, ...]


class TtyBuffer(StringIO):
    def isatty(self) -> bool:
        return True


def test_render_bar_scales_completed_calls() -> None:
    assert render_bar(1, 2, width=10) == "[#####-----]"
    assert render_bar(2, 2, width=10) == "[##########]"


def test_format_seconds_includes_minutes() -> None:
    assert format_seconds(90) == "90.00s (1.50m)"


def test_table_progress_updates_one_tty_line_and_completes_once() -> None:
    stream = TtyBuffer()
    progress = TableProgress(Report((Job("daily"), Job("daily"))), stream=stream)

    progress({"table": "daily", "status": "success", "rows_written": 10})
    progress({"table": "daily", "status": "success", "rows_written": 20})

    output = stream.getvalue()
    assert "\rdaily [" in output
    assert output.count("daily completed") == 1
    assert "calls=2, rows=30, failures=0" in output
    assert "trade_date=" not in output


def test_table_progress_emits_one_summary_per_table_for_non_tty() -> None:
    stream = StringIO()
    progress = TableProgress(
        Report((Job("daily"), Job("income"), Job("income"))),
        stream=stream,
    )

    progress({"table": "daily", "status": "success", "rows_written": 1})
    progress({"table": "income", "status": "success", "rows_written": 2})
    progress({"table": "income", "status": "success", "rows_written": 3})

    output = stream.getvalue()
    assert output.count("daily completed") == 1
    assert output.count("income completed") == 1
    assert "daily [" not in output
    assert "income [" not in output


def test_table_progress_counts_failures_and_latest_error() -> None:
    stream = TtyBuffer()
    progress = TableProgress(Report((Job("income"), Job("income"))), stream=stream)

    progress(
        {
            "table": "income",
            "status": "failed",
            "rows_written": 0,
            "error": "temporary outage",
        }
    )
    progress({"table": "income", "status": "success", "rows_written": 7})

    output = stream.getvalue()
    assert "failures=1" in output
    assert "latest error=temporary outage" in output
