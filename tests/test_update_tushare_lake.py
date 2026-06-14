from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

import polars as pl

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
collect_config = update_tushare_lake.collect_config
main = update_tushare_lake.main
UpdateConfig = update_tushare_lake.UpdateConfig


@dataclass(frozen=True, slots=True)
class Job:
    table: str


@dataclass(frozen=True, slots=True)
class Spec:
    table: str


@dataclass(frozen=True, slots=True)
class Plan:
    table: str
    kind: str


@dataclass(frozen=True, slots=True)
class Report:
    jobs: tuple[Job, ...] = ()
    plans: tuple[Plan, ...] = ()


class TtyBuffer(StringIO):
    def isatty(self) -> bool:
        return True


def test_render_bar_scales_completed_calls() -> None:
    assert render_bar(1, 2, width=10) == "[#####-----]"
    assert render_bar(2, 2, width=10) == "[##########]"


def test_format_seconds_includes_minutes() -> None:
    assert format_seconds(90) == "90.00s (1.50m)"


def test_collect_config_defaults_to_repo_local_lake(monkeypatch) -> None:
    defaults: dict[str, str | None] = {}

    def fake_prompt(label: str, *, default: str | None = None, **_):
        defaults[label] = default
        return default or ""

    monkeypatch.setattr(update_tushare_lake, "prompt", fake_prompt)
    monkeypatch.setattr(
        update_tushare_lake,
        "resolve_tushare_token",
        lambda: (None, "not configured"),
    )

    config = collect_config()

    assert defaults["Lake path"] == str(update_tushare_lake.DEFAULT_LAKE)
    assert config.lake == update_tushare_lake.DEFAULT_LAKE


def test_main_declined_preview_does_not_build_executable_scan(monkeypatch) -> None:
    events: list[str] = []

    class ScanManager:
        def tushare_update_specs(self):
            return ("income",)

        def preview_tushare_updates(self, specs, *, start_date, end_date):
            events.append(f"preview:{specs[0]}:{start_date}:{end_date}")
            return Report(())

        def scan_tushare_updates(self, *_args, **_kwargs):
            raise AssertionError("declined preview should not run executable scan")

    monkeypatch.setattr(
        update_tushare_lake,
        "collect_config",
        lambda: UpdateConfig(Path("lake"), "2024-01-01", "2024-01-31", None, "test"),
    )
    monkeypatch.setattr(
        update_tushare_lake,
        "build_scan_manager",
        lambda _config: ScanManager(),
    )
    monkeypatch.setattr(
        update_tushare_lake,
        "missing_reference_tables",
        lambda _manager: (),
    )
    monkeypatch.setattr(
        update_tushare_lake,
        "print_plan",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        update_tushare_lake,
        "prompt_yes_no",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        update_tushare_lake,
        "build_manager",
        lambda _config: (_ for _ in ()).throw(
            AssertionError("declined preview should not build update manager")
        ),
    )

    main()

    assert events == ["preview:income:2024-01-01:2024-01-31"]


def test_main_confirmed_update_scans_after_preview(monkeypatch) -> None:
    events: list[str] = []
    executable_report = Report((Job("income"),))

    class ScanManager:
        def tushare_update_specs(self):
            return ("income",)

        def preview_tushare_updates(self, specs, *, start_date, end_date):
            events.append(f"preview:{specs[0]}:{start_date}:{end_date}")
            return Report(())

    class UpdateManager:
        def scan_tushare_updates(self, specs, *, start_date, end_date):
            events.append(f"scan:{specs[0]}:{start_date}:{end_date}")
            return executable_report

        def execute_tushare_update_report(
            self,
            report,
            *,
            progress,
            continue_on_error,
        ):
            events.append(
                f"execute:{report is executable_report}:{continue_on_error}"
            )
            return ()

        def tushare_api_call_log(self):
            return pl.DataFrame()

    monkeypatch.setattr(
        update_tushare_lake,
        "collect_config",
        lambda: UpdateConfig(
            Path("lake"),
            "2024-01-01",
            "2024-01-31",
            "token",
            "test",
        ),
    )
    monkeypatch.setattr(
        update_tushare_lake,
        "build_scan_manager",
        lambda _config: ScanManager(),
    )
    monkeypatch.setattr(
        update_tushare_lake,
        "missing_reference_tables",
        lambda _manager: (),
    )
    monkeypatch.setattr(
        update_tushare_lake,
        "print_plan",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        update_tushare_lake,
        "prompt_yes_no",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        update_tushare_lake,
        "prompt_update_scope",
        lambda selected_specs, _preview_report: selected_specs,
    )
    monkeypatch.setattr(
        update_tushare_lake,
        "build_manager",
        lambda _config: UpdateManager(),
    )

    main()

    assert events == [
        "preview:income:2024-01-01:2024-01-31",
        "scan:income:2024-01-01:2024-01-31",
        "execute:True:True",
    ]


def test_main_price_scope_scans_only_price_specs(monkeypatch) -> None:
    events: list[str] = []
    specs = (Spec("daily"), Spec("income"), Spec("adj_factor"))
    preview_report = Report(
        plans=(
            Plan("daily", "price"),
            Plan("income", "fundamental"),
            Plan("adj_factor", "price"),
        )
    )

    class ScanManager:
        def tushare_update_specs(self):
            return specs

        def preview_tushare_updates(self, selected_specs, *, start_date, end_date):
            events.append(
                "preview:" + ",".join(spec.table for spec in selected_specs)
            )
            return preview_report

    class UpdateManager:
        def scan_tushare_updates(self, selected_specs, *, start_date, end_date):
            events.append("scan:" + ",".join(spec.table for spec in selected_specs))
            return Report()

        def execute_tushare_update_report(
            self,
            report,
            *,
            progress,
            continue_on_error,
        ):
            events.append(f"execute:{continue_on_error}")
            return ()

        def tushare_api_call_log(self):
            return pl.DataFrame()

    answers = iter(["1"])
    monkeypatch.setattr(
        update_tushare_lake,
        "collect_config",
        lambda: UpdateConfig(Path("lake"), "2024-01-01", "2024-01-31", "token", "t"),
    )
    monkeypatch.setattr(update_tushare_lake, "build_scan_manager", lambda _: ScanManager())
    monkeypatch.setattr(update_tushare_lake, "missing_reference_tables", lambda _: ())
    monkeypatch.setattr(update_tushare_lake, "print_plan", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(update_tushare_lake, "prompt_yes_no", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(update_tushare_lake, "prompt", lambda *_args, **_kwargs: next(answers))
    monkeypatch.setattr(update_tushare_lake, "build_manager", lambda _: UpdateManager())

    main()

    assert events == [
        "preview:daily,income,adj_factor",
        "scan:daily,adj_factor",
        "execute:True",
    ]


def test_main_fundamental_scope_includes_fundamental_vip(monkeypatch) -> None:
    events: list[str] = []
    specs = (Spec("daily"), Spec("income"), Spec("income_vip"))
    preview_report = Report(
        plans=(
            Plan("daily", "price"),
            Plan("income", "fundamental"),
            Plan("income_vip", "fundamental_vip"),
        )
    )

    class ScanManager:
        def tushare_update_specs(self):
            return specs

        def preview_tushare_updates(self, selected_specs, *, start_date, end_date):
            return preview_report

    class UpdateManager:
        def scan_tushare_updates(self, selected_specs, *, start_date, end_date):
            events.append("scan:" + ",".join(spec.table for spec in selected_specs))
            return Report()

        def execute_tushare_update_report(
            self,
            report,
            *,
            progress,
            continue_on_error,
        ):
            return ()

        def tushare_api_call_log(self):
            return pl.DataFrame()

    answers = iter(["2"])
    monkeypatch.setattr(
        update_tushare_lake,
        "collect_config",
        lambda: UpdateConfig(Path("lake"), "2024-01-01", "2024-01-31", "token", "t"),
    )
    monkeypatch.setattr(update_tushare_lake, "build_scan_manager", lambda _: ScanManager())
    monkeypatch.setattr(update_tushare_lake, "missing_reference_tables", lambda _: ())
    monkeypatch.setattr(update_tushare_lake, "print_plan", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(update_tushare_lake, "prompt_yes_no", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(update_tushare_lake, "prompt", lambda *_args, **_kwargs: next(answers))
    monkeypatch.setattr(update_tushare_lake, "build_manager", lambda _: UpdateManager())

    main()

    assert events == ["scan:income,income_vip"]


def test_main_selection_scope_repeats_until_quit_and_ignores_duplicates(
    monkeypatch,
) -> None:
    events: list[str] = []
    specs = (Spec("daily"), Spec("income"), Spec("cashflow"))
    preview_report = Report(
        plans=(
            Plan("daily", "price"),
            Plan("income", "fundamental"),
            Plan("cashflow", "fundamental"),
        )
    )

    class ScanManager:
        def tushare_update_specs(self):
            return specs

        def preview_tushare_updates(self, selected_specs, *, start_date, end_date):
            return preview_report

    class UpdateManager:
        def scan_tushare_updates(self, selected_specs, *, start_date, end_date):
            events.append("scan:" + ",".join(spec.table for spec in selected_specs))
            return Report()

        def execute_tushare_update_report(
            self,
            report,
            *,
            progress,
            continue_on_error,
        ):
            return ()

        def tushare_api_call_log(self):
            return pl.DataFrame()

    answers = iter(["3", "2", "2", "1", "q"])
    monkeypatch.setattr(
        update_tushare_lake,
        "collect_config",
        lambda: UpdateConfig(Path("lake"), "2024-01-01", "2024-01-31", "token", "t"),
    )
    monkeypatch.setattr(update_tushare_lake, "build_scan_manager", lambda _: ScanManager())
    monkeypatch.setattr(update_tushare_lake, "missing_reference_tables", lambda _: ())
    monkeypatch.setattr(update_tushare_lake, "print_plan", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(update_tushare_lake, "prompt_yes_no", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(update_tushare_lake, "prompt", lambda *_args, **_kwargs: next(answers))
    monkeypatch.setattr(update_tushare_lake, "build_manager", lambda _: UpdateManager())

    main()

    assert events == ["scan:income,daily"]


def test_main_scope_quit_exits_before_building_update_manager(monkeypatch) -> None:
    class ScanManager:
        def tushare_update_specs(self):
            return (Spec("daily"),)

        def preview_tushare_updates(self, selected_specs, *, start_date, end_date):
            return Report(plans=(Plan("daily", "price"),))

    answers = iter(["4"])
    monkeypatch.setattr(
        update_tushare_lake,
        "collect_config",
        lambda: UpdateConfig(Path("lake"), "2024-01-01", "2024-01-31", None, "test"),
    )
    monkeypatch.setattr(update_tushare_lake, "build_scan_manager", lambda _: ScanManager())
    monkeypatch.setattr(update_tushare_lake, "missing_reference_tables", lambda _: ())
    monkeypatch.setattr(update_tushare_lake, "print_plan", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(update_tushare_lake, "prompt_yes_no", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(update_tushare_lake, "prompt", lambda *_args, **_kwargs: next(answers))
    monkeypatch.setattr(
        update_tushare_lake,
        "build_manager",
        lambda _config: (_ for _ in ()).throw(
            AssertionError("scope quit should not build update manager")
        ),
    )

    main()


def test_main_empty_scope_selection_returns_to_scope_menu(monkeypatch) -> None:
    events: list[str] = []
    specs = (Spec("income"),)

    class ScanManager:
        def tushare_update_specs(self):
            return specs

        def preview_tushare_updates(self, selected_specs, *, start_date, end_date):
            return Report(plans=(Plan("income", "fundamental"),))

    class UpdateManager:
        def scan_tushare_updates(self, selected_specs, *, start_date, end_date):
            events.append("scan:" + ",".join(spec.table for spec in selected_specs))
            return Report()

        def execute_tushare_update_report(
            self,
            report,
            *,
            progress,
            continue_on_error,
        ):
            return ()

        def tushare_api_call_log(self):
            return pl.DataFrame()

    answers = iter(["1", "2"])
    monkeypatch.setattr(
        update_tushare_lake,
        "collect_config",
        lambda: UpdateConfig(Path("lake"), "2024-01-01", "2024-01-31", "token", "t"),
    )
    monkeypatch.setattr(update_tushare_lake, "build_scan_manager", lambda _: ScanManager())
    monkeypatch.setattr(update_tushare_lake, "missing_reference_tables", lambda _: ())
    monkeypatch.setattr(update_tushare_lake, "print_plan", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(update_tushare_lake, "prompt_yes_no", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(update_tushare_lake, "prompt", lambda *_args, **_kwargs: next(answers))
    monkeypatch.setattr(update_tushare_lake, "build_manager", lambda _: UpdateManager())

    main()

    assert events == ["scan:income"]


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
