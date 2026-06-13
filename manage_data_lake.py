# ruff: noqa: E402

from __future__ import annotations

import getpass
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

import polars as pl

from bagelquant_data.datasource import DataSourceRegistry, TushareDataSource
from bagelquant_data.lake import DataLakeManager, LocalDataLake

LOCAL_CONFIG = ROOT / ".bagelquant-data-local.json"
SEPARATOR = "-" * 20


def prompt(label: str, *, default: str | None = None) -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{label}{suffix}: ").strip()
    return value or default or ""


def prompt_int(label: str, *, default: int) -> int:
    value = prompt(label, default=str(default))
    try:
        return int(value)
    except ValueError:
        print(f"Invalid number, using {default}.")
        return default


def read_local_config() -> dict[str, str]:
    if not LOCAL_CONFIG.exists():
        return {}
    return json.loads(LOCAL_CONFIG.read_text(encoding="utf-8"))


def write_local_config(config: dict[str, str]) -> None:
    LOCAL_CONFIG.write_text(
        json.dumps(config, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def build_manager(lake: Path) -> DataLakeManager:
    return DataLakeManager(LocalDataLake(lake))


def build_tushare_manager(lake: Path) -> DataLakeManager | None:
    token = read_local_config().get("tushare_token")
    if not token:
        print("Tushare token is not configured. Choose action 11 first.")
        return None
    registry = DataSourceRegistry()
    registry.register(TushareDataSource(token=token))
    return DataLakeManager(LocalDataLake(lake), registry=registry)


def refresh_tushare_refs(lake: Path) -> bool:
    manager = build_tushare_manager(lake)
    if manager is None:
        return False
    print_block("Refresh Tushare reference tables")
    print("refreshing stock_basic for list_status L, D, P")
    manager.update_tushare_stock_basic()
    print("refreshing trade_cal from 2000-01-01")
    manager.update_tushare_trading_calendar(start_date="2000-01-01")
    print(SEPARATOR)
    print("reference tables refreshed")
    return True


def ensure_tushare_refs(lake: Path, manager: DataLakeManager) -> None:
    missing = [
        table
        for table in ("stock_basic", "trade_cal")
        if manager.latest("tushare", table) is None
    ]
    if not missing:
        return
    print(SEPARATOR)
    print(f"missing reference tables: {', '.join(missing)}")
    refresh_tushare_refs(lake)


def print_block(title: str) -> None:
    print()
    print(SEPARATOR)
    print(title)
    print(SEPARATOR)


def print_frame(frame: pl.DataFrame) -> None:
    with pl.Config(tbl_formatting="ASCII_FULL"):
        print(frame)


def print_menu() -> None:
    print_block("Choose an action")
    print("1. Add Tushare table to update list")
    print("2. List Tushare update tables")
    print("3. Remove Tushare table from update list")
    print("4. List sources")
    print("5. List tables")
    print("6. List snapshots")
    print("7. List fields")
    print("8. List assets")
    print("9. Inspect Tushare API call log")
    print("10. Delete source, table, or snapshot")
    print("11. Set Tushare token")
    print("12. Show local config status")
    print("13. Refresh stock_basic and trade_cal")
    print("q. Quit")


def add_tushare_update_table(lake: Path, manager: DataLakeManager) -> None:
    print_block("Add Tushare table")
    table = prompt("Tushare table, for example daily")
    if not table:
        print("No table entered.")
        return
    print()
    print("Table type")
    print(SEPARATOR)
    print("1. Infer automatically")
    print("2. Price")
    print("3. Fundamental")
    print("4. Fundamental VIP")
    print("5. General")
    kind_choice = prompt("Type", default="1")
    kind_by_choice = {
        "1": None,
        "2": "price",
        "3": "fundamental",
        "4": "fundamental_vip",
        "5": "general",
    }
    if kind_choice not in kind_by_choice:
        print("Unknown type. Choose 1, 2, 3, 4, or 5.")
        return
    manager.register_tushare_update_table(table, kind=kind_by_choice[kind_choice])
    print(SEPARATOR)
    print(f"registered tushare/{table}")
    ensure_tushare_refs(lake, manager)


def list_tushare_update_tables(manager: DataLakeManager) -> None:
    print_block("Tushare update tables")
    frame = manager.tushare_update_tables()
    if frame.is_empty():
        print("No Tushare update tables registered.")
        return
    print_frame(frame)


def remove_tushare_update_table(manager: DataLakeManager) -> None:
    print_block("Remove Tushare table")
    table = prompt("Tushare table to remove")
    if not table:
        print("No table entered.")
        return
    manager.remove_tushare_update_table(table)
    print(f"removed tushare/{table} from update list")


def list_sources(manager: DataLakeManager) -> None:
    print_block("Sources")
    for source in manager.list_sources():
        print(source)


def list_tables(manager: DataLakeManager) -> None:
    print_block("Tables")
    source = prompt("Source filter, blank for all")
    rows = [
        {
            "source": source_name,
            "table": table,
            "status": "available",
            "snapshot": manager.latest(source_name, table).snapshot_id
            if manager.latest(source_name, table) is not None
            else "",
        }
        for source_name, table in manager.list_tables(source or None)
    ]
    if source in {"", "tushare"}:
        known = {(row["source"], row["table"]) for row in rows}
        for table in ("stock_basic", "trade_cal"):
            if ("tushare", table) in known:
                continue
            ref = manager.latest("tushare", table)
            rows.append(
                {
                    "source": "tushare",
                    "table": table,
                    "status": "available" if ref is not None else "missing",
                    "snapshot": ref.snapshot_id if ref is not None else "",
                }
            )
    if not rows:
        print("No tables found.")
        return
    print_frame(pl.DataFrame(rows).sort(["source", "table"]))


def list_snapshots(manager: DataLakeManager) -> None:
    print_block("Snapshots")
    source = prompt("Source", default="tushare")
    table = prompt("Table", default="daily")
    for ref in manager.snapshots(source, table):
        print(ref.snapshot_id)


def list_fields(manager: DataLakeManager) -> None:
    print_block("Fields")
    source = prompt("Source filter, blank for all")
    limit = prompt_int("Rows to show", default=100)
    print_frame(manager.lake.fields(source or None).head(limit))


def list_assets(manager: DataLakeManager) -> None:
    print_block("Assets")
    source = prompt("Source", default="tushare")
    for asset_id in manager.lake.asset_ids(source):
        print(asset_id)


def inspect_tushare_log(manager: DataLakeManager) -> None:
    print_block("Tushare API call log")
    table = prompt("Table filter, blank for all")
    status = prompt("Status filter: success, empty, failed, or blank")
    limit = prompt_int("Rows to show", default=50)
    frame = manager.tushare_api_call_log()
    if table:
        frame = frame.filter(frame["table"] == table)
    if status:
        frame = frame.filter(frame["status"] == status)
    print_frame(frame.tail(limit))


def delete(manager: DataLakeManager) -> None:
    print_block("Delete lake data")
    source = prompt("Source", default="tushare")
    table = prompt("Table, blank deletes the whole source")
    snapshot = prompt("Snapshot, blank deletes latest table target")
    target = source if not table else f"{source}/{table}"
    if snapshot:
        target = f"{target}@{snapshot}"
    confirm = input(f"Delete {target}? [y/N]: ").strip().lower()
    if confirm not in {"y", "yes"}:
        print("Delete cancelled.")
        return
    manager.delete(source, table or None, snapshot=snapshot or None)
    print(f"deleted {target}")


def set_tushare_token(_: DataLakeManager) -> None:
    print_block("Set Tushare token")
    token = getpass.getpass("Tushare token: ").strip()
    if not token:
        print("No token entered.")
        return
    config = read_local_config()
    config["tushare_token"] = token
    write_local_config(config)
    print(f"saved token to {LOCAL_CONFIG.name}")


def show_config_status(_: DataLakeManager) -> None:
    print_block("Local config status")
    config = read_local_config()
    has_token = bool(config.get("tushare_token"))
    print(f"local config: {LOCAL_CONFIG}")
    print(f"tushare token saved: {'yes' if has_token else 'no'}")


def refresh_refs_action(lake: Path, _: DataLakeManager) -> None:
    refresh_tushare_refs(lake)


def main() -> None:
    print_block("BagelQuant local lake manager")
    lake = Path(prompt("Lake path", default=".bagelquant-data-lake")).expanduser()
    manager = build_manager(lake)
    actions = {
        "1": lambda active: add_tushare_update_table(lake, active),
        "2": list_tushare_update_tables,
        "3": remove_tushare_update_table,
        "4": list_sources,
        "5": list_tables,
        "6": list_snapshots,
        "7": list_fields,
        "8": list_assets,
        "9": inspect_tushare_log,
        "10": delete,
        "11": set_tushare_token,
        "12": show_config_status,
        "13": lambda active: refresh_refs_action(lake, active),
    }
    while True:
        print_menu()
        choice = prompt("Action")
        if choice.lower() in {"q", "quit", "exit"}:
            return
        action = actions.get(choice)
        if action is None:
            print("Unknown action.")
            continue
        action(manager)


if __name__ == "__main__":
    main()
