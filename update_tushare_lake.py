from __future__ import annotations

import json
import os
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from time import perf_counter
from collections.abc import Mapping
from typing import TextIO

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from bagelquant_data.datasource import DataSourceRegistry, TushareDataSource
from bagelquant_data.lake import (
    DataLakeManager,
    LocalDataLake,
    TushareTableUpdateSpec,
    TushareUpdateReport,
)

LOCAL_CONFIG = ROOT / ".bagelquant-data-local.json"
DEFAULT_LAKE = ROOT / ".bagelquant-data-lake"
SEPARATOR = "-" * 20


@dataclass(frozen=True, slots=True)
class UpdateConfig:
    lake: Path
    start_date: str
    end_date: str
    token: str | None
    token_source: str


def prompt(
    label: str,
    *,
    default: str | None = None,
    required: bool = False,
) -> str:
    suffix = f" [{default}]" if default else ""
    while True:
        value = input(f"{label}{suffix}: ").strip()
        if value:
            return value
        if default is not None:
            return default
        if not required:
            return ""
        print("Please enter a value.")


def prompt_yes_no(label: str, *, default: bool = False) -> bool:
    suffix = " [Y/n]" if default else " [y/N]"
    value = input(f"{label}{suffix}: ").strip().lower()
    if not value:
        return default
    return value in {"y", "yes"}


def print_block(title: str) -> None:
    print()
    print(SEPARATOR)
    print(title)
    print(SEPARATOR)


def read_local_config() -> dict[str, str]:
    if not LOCAL_CONFIG.exists():
        return {}
    return json.loads(LOCAL_CONFIG.read_text(encoding="utf-8"))


def resolve_tushare_token() -> tuple[str | None, str]:
    config_token = read_local_config().get("tushare_token")
    if config_token:
        return config_token, LOCAL_CONFIG.name
    env_token = os.environ.get("TUSHARE_TOKEN")
    if env_token:
        return env_token, "TUSHARE_TOKEN"
    return None, "not configured"


def collect_config() -> UpdateConfig:
    print_block("BagelQuant Tushare lake updater")
    print("Press Enter to accept the default shown in brackets.")
    print(SEPARATOR)
    lake = Path(prompt("Lake path", default=str(DEFAULT_LAKE))).expanduser()
    start = prompt("Start date", default="2000-01-01")
    end = prompt("End date", default=date.today().isoformat())
    token, token_source = resolve_tushare_token()
    print(f"Tushare token: {token_source}")
    return UpdateConfig(
        lake=lake,
        start_date=start,
        end_date=end,
        token=token,
        token_source=token_source,
    )


def build_manager(config: UpdateConfig) -> DataLakeManager:
    registry = DataSourceRegistry()
    registry.register(TushareDataSource(token=config.token))
    return DataLakeManager(LocalDataLake(config.lake), registry=registry)


def build_scan_manager(config: UpdateConfig) -> DataLakeManager:
    return DataLakeManager(LocalDataLake(config.lake))


def format_seconds(seconds: float) -> str:
    return f"{seconds:.2f}s ({seconds / 60:.2f}m)"


def print_plan(
    config: UpdateConfig,
    report: TushareUpdateReport,
    *,
    scan_seconds: float,
) -> None:
    grouped = {
        "price": [],
        "fundamental": [],
        "fundamental_vip": [],
        "general": [],
    }
    for plan in report.plans:
        grouped.setdefault(plan.kind, []).append(plan)
    print_block("Tushare lake update plan")
    print(f"lake: {config.lake}")
    print(f"range: {report.requested_start} .. {report.requested_end}")
    print(f"tables: {', '.join(plan.table for plan in report.plans)}")
    print("reference refreshes: only if local stock_basic or trade_cal is missing")
    print(f"scan preview time: {format_seconds(scan_seconds)}")
    estimated_calls = sum(plan.estimated_job_count for plan in report.plans)
    print(f"estimated api calls: {estimated_calls}")
    print(SEPARATOR)
    for kind, plans in grouped.items():
        if not plans:
            continue
        print(f"{kind}")
        for plan in plans:
            preview = ""
            if kind in {"fundamental", "fundamental_vip"}:
                last = (
                    plan.last_update_date.isoformat()
                    if plan.last_update_date
                    else "none"
                )
                effective = (
                    plan.effective_start.isoformat()
                    if plan.effective_start
                    else "none"
                )
                preview = f", last_update={last}, effective_start={effective}"
            elif plan.pending_items:
                first = plan.pending_items[0]
                last = plan.pending_items[-1]
                preview = f", first={first}, last={last}"
            print(
                f"- {plan.table}: {plan.estimated_job_count} call(s), "
                f"status={plan.status}{preview}"
            )
        print(SEPARATOR)


def spec_table(spec: TushareTableUpdateSpec) -> str:
    return spec.table


def prompt_update_scope(
    specs: tuple[TushareTableUpdateSpec, ...],
    preview_report: TushareUpdateReport,
) -> tuple[TushareTableUpdateSpec, ...] | None:
    specs_by_table = {spec_table(spec): spec for spec in specs}
    plans_by_table = {plan.table: plan for plan in preview_report.plans}
    ordered_tables = [
        spec_table(spec)
        for spec in specs
        if spec_table(spec) in plans_by_table
    ]

    while True:
        print_block("Which tables to update?")
        print("1. price only")
        print("2. fundamental only")
        print("3. selection only")
        print("4. quit")
        choice = prompt("Update scope")
        if choice == "1":
            selected = tuple(
                specs_by_table[table]
                for table in ordered_tables
                if plans_by_table[table].kind == "price"
            )
            if selected:
                return selected
            print("No price tables are available in this update plan.")
            continue
        if choice == "2":
            selected = tuple(
                specs_by_table[table]
                for table in ordered_tables
                if plans_by_table[table].kind in {"fundamental", "fundamental_vip"}
            )
            if selected:
                return selected
            print("No fundamental tables are available in this update plan.")
            continue
        if choice == "3":
            selected = prompt_table_selection(ordered_tables, specs_by_table)
            if selected:
                return selected
            print("No tables selected.")
            continue
        if choice in {"4", "q", "quit", "exit"}:
            return None
        print("Unknown option. Choose 1, 2, 3, or 4.")


def prompt_table_selection(
    ordered_tables: list[str],
    specs_by_table: dict[str, TushareTableUpdateSpec],
) -> tuple[TushareTableUpdateSpec, ...]:
    selected: list[TushareTableUpdateSpec] = []
    selected_tables: set[str] = set()
    while True:
        print_block("Select tables")
        for index, table in enumerate(ordered_tables, start=1):
            marker = "*" if table in selected_tables else " "
            print(f"{index}. [{marker}] {table}")
        print("q. quit")
        choice = prompt("Table")
        if choice.lower() in {"q", "quit", "exit"}:
            return tuple(selected)
        try:
            index = int(choice)
        except ValueError:
            print("Unknown table. Choose a number or q.")
            continue
        if index < 1 or index > len(ordered_tables):
            print("Unknown table. Choose a number or q.")
            continue
        table = ordered_tables[index - 1]
        if table in selected_tables:
            print(f"{table} already selected.")
            continue
        selected_tables.add(table)
        selected.append(specs_by_table[table])
        print(f"selected {table}")


class TableProgress:
    def __init__(self, report: TushareUpdateReport, *, stream: TextIO | None = None) -> None:
        self._stream = stream or sys.stdout
        self._is_tty = self._stream.isatty()
        self._total = Counter(job.table for job in report.jobs)
        self._completed: Counter[str] = Counter()
        self._started: dict[str, float] = {}
        self._rows: Counter[str] = Counter()
        self._failures: Counter[str] = Counter()
        self._latest_error: dict[str, str] = {}

    def __call__(self, event: Mapping[str, object]) -> None:
        table = str(event.get("table", ""))
        if not table:
            return
        if table and table not in self._started:
            self._started[table] = perf_counter()
        rows_value = event.get("rows_written")
        rows = rows_value if isinstance(rows_value, int) else 0
        self._rows[table] += rows
        if event.get("status") == "failed":
            self._failures[table] += 1
            error = event.get("error")
            if error:
                self._latest_error[table] = str(error)
        self._completed[table] += 1

        total = self._total[table]
        completed = self._completed[table]
        line = (
            f"{table} {render_bar(completed, total)} "
            f"{completed}/{total} calls, rows={self._rows[table]}, "
            f"failures={self._failures[table]}"
        )
        if self._is_tty:
            print(f"\r{line}", end="", file=self._stream, flush=True)

        if completed >= total:
            elapsed = perf_counter() - self._started[table]
            summary = (
                f"{table} completed in {format_seconds(elapsed)}, "
                f"calls={completed}, rows={self._rows[table]}, "
                f"failures={self._failures[table]}"
            )
            error = self._latest_error.get(table)
            if error:
                summary = f"{summary}, latest error={error}"
            if self._is_tty:
                print(file=self._stream, flush=True)
            print(summary, file=self._stream, flush=True)


def render_bar(completed: int, total: int, width: int = 28) -> str:
    if total <= 0:
        filled = width
    else:
        filled = min(width, max(0, round(width * completed / total)))
    return f"[{'#' * filled}{'-' * (width - filled)}]"


def missing_reference_tables(manager: DataLakeManager) -> tuple[str, ...]:
    return tuple(
        table
        for table in ("stock_basic", "trade_cal")
        if manager.latest("tushare", table) is None
    )


def main() -> None:
    config = collect_config()
    scan_manager = build_scan_manager(config)
    specs = scan_manager.tushare_update_specs()
    if not specs:
        print_block("No registered tables")
        print("Run `uv run python manage_data_lake.py` and choose action 1 first.")
        return

    missing_refs = missing_reference_tables(scan_manager)
    if missing_refs:
        if not config.token:
            print_block("Missing Tushare token")
            print(f"Missing references: {', '.join(missing_refs)}")
            print("Tushare token is required to refresh missing references.")
            print("Run `uv run python manage_data_lake.py` and choose action 11.")
            return
        manager = build_manager(config)
        print_block("Reference refresh")
        print(f"missing references: {', '.join(missing_refs)}")
        if "stock_basic" in missing_refs:
            print("refreshing tushare stock_basic")
            manager.update_tushare_stock_basic()
        if "trade_cal" in missing_refs:
            print("refreshing tushare trade_cal")
            manager.update_tushare_trading_calendar(start_date="2000-01-01")
    else:
        print_block("Reference refresh")
        print("local stock_basic and trade_cal already exist; skipping refresh")

    scan_manager = build_scan_manager(config)
    specs = scan_manager.tushare_update_specs()
    scan_started = perf_counter()
    preview_report = scan_manager.preview_tushare_updates(
        specs,
        start_date=config.start_date,
        end_date=config.end_date,
    )
    scan_seconds = perf_counter() - scan_started
    print_plan(config, preview_report, scan_seconds=scan_seconds)
    if not prompt_yes_no("Proceed with this update", default=True):
        print_block("Preview complete")
        print("Preview complete. No data was updated.")
        return
    selected_specs = prompt_update_scope(specs, preview_report)
    if selected_specs is None:
        print_block("Preview complete")
        print("Preview complete. No data was updated.")
        return
    if not config.token:
        print_block("Missing Tushare token")
        print("Tushare token is not configured.")
        print("Run `uv run python manage_data_lake.py` and choose action 11.")
        return

    manager = build_manager(config)
    report = manager.scan_tushare_updates(
        selected_specs,
        start_date=config.start_date,
        end_date=config.end_date,
    )
    update_started = perf_counter()
    refs = manager.execute_tushare_update_report(
        report,
        progress=TableProgress(report),
        continue_on_error=True,
    )
    update_seconds = perf_counter() - update_started
    log = manager.tushare_api_call_log()
    recent = log.tail(len(report.jobs)) if report.jobs else log.head(0)
    failures = (
        recent.filter(recent["status"] == "failed").height if recent.height else 0
    )
    rows = recent["rows"].sum() if recent.height else 0
    print_block("Update summary")
    print(f"api calls: {len(report.jobs)}")
    print(f"rows returned: {rows}")
    print(f"failures: {failures}")
    print(f"snapshots written: {len(refs)}")
    print(f"update time: {format_seconds(update_seconds)}")
    print(SEPARATOR)


if __name__ == "__main__":
    main()
