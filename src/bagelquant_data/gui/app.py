"""Streamlit application for managing a local BagelQuant data lake."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from bagelquant_data.gui.codegen import (
    RetrievalSelection,
    lake_read_snippet,
    loader_read_snippet,
    panel_agreement_snippet,
)
from bagelquant_data.gui.config import (
    DEFAULT_CONFIG_PATH,
    GuiConfig,
    PeriodicJobConfig,
    SourceConfig,
    TableConfig,
    UniverseConfig,
    load_config,
    save_config,
)
from bagelquant_data.gui.orchestration import (
    build_registry,
    run_due_jobs,
    run_table_update,
    token_available,
    token_from_environment,
)
from bagelquant_data.lake import DataLakeManager, LocalDataLake
from bagelquant_data.utils.exceptions import DatasetNotFoundError

TABLE_KINDS = ("price", "fundamental", "fundamental_vip")
SCHEDULE_UNITS = ("minutes", "hours", "days")


def main() -> None:
    """Run the Streamlit GUI."""

    st.set_page_config(page_title="BagelQuant Data Lake", layout="wide")
    st.title("BagelQuant Data Lake")

    config_path = Path(
        st.sidebar.text_input("Config file", value=str(DEFAULT_CONFIG_PATH))
    )
    config = _session_config(config_path)
    config.lake_root = st.sidebar.text_input("Lake root", value=config.lake_root)

    token = token_from_environment(st.secrets)
    token_state = (
        "available" if token_available(streamlit_secrets=st.secrets) else "missing"
    )
    st.sidebar.caption(f"Tushare token: {token_state}")
    if st.sidebar.button("Save config", use_container_width=True):
        save_config(config, config_path)
        st.sidebar.success("Config saved")

    lake = LocalDataLake(config.lake_root)
    registry = build_registry(tushare_token=token)
    manager = DataLakeManager(lake, registry=registry)

    tabs = st.tabs(["Lake Setup", "Data Sources", "Retrieve Data", "Update Data Lake"])
    with tabs[0]:
        _lake_setup(config, manager)
    with tabs[1]:
        _data_sources(config)
    with tabs[2]:
        _retrieve_data(config, lake)
    with tabs[3]:
        _update_data_lake(config, manager, config_path, token is not None)


def _session_config(path: Path) -> GuiConfig:
    if "bq_data_gui_config_path" not in st.session_state:
        st.session_state["bq_data_gui_config_path"] = str(path)
        st.session_state["bq_data_gui_config"] = load_config(path)
    if st.session_state["bq_data_gui_config_path"] != str(path):
        st.session_state["bq_data_gui_config_path"] = str(path)
        st.session_state["bq_data_gui_config"] = load_config(path)
    return st.session_state["bq_data_gui_config"]


def _lake_setup(config: GuiConfig, manager: DataLakeManager) -> None:
    st.subheader("Lake Contents")
    sources = manager.list_sources()
    tables = manager.list_tables()
    col_a, col_b, col_c = st.columns(3)
    col_a.metric("Sources", len(sources))
    col_b.metric("Tables", len(tables))
    col_c.metric("Configured universes", len(config.universes))

    st.dataframe(
        pd.DataFrame(tables, columns=["source", "table"])
        if tables
        else pd.DataFrame(columns=["source", "table"]),
        use_container_width=True,
    )

    selected_source = st.selectbox(
        "Source",
        options=sources or tuple(config.source_names()) or ("tushare",),
        key="lake_source",
    )
    if selected_source:
        source_tables = tuple(
            table for source, table in tables if source == selected_source
        )
        selected_table = st.selectbox(
            "Table snapshots",
            options=source_tables or ("",),
            key="lake_snapshot_table",
        )
        if selected_table:
            snapshots = manager.snapshots(selected_source, selected_table)
            st.dataframe(
                pd.DataFrame(
                    {
                        "snapshot": item.snapshot_id,
                        "year": item.year,
                        "month": item.month,
                        "created_at": item.created_at.isoformat(),
                        "rows": (item.metadata or {}).get("rows"),
                    }
                    for item in snapshots
                ),
                use_container_width=True,
                height=180,
            )
            confirm_delete = st.checkbox(
                "Confirm local table deletion",
                key="confirm-delete-local-table",
            )
            if st.button(
                "Delete local table",
                disabled=not confirm_delete,
                use_container_width=False,
            ):
                manager.delete(selected_source, selected_table)
                st.success(f"Deleted {selected_source}/{selected_table}")
                st.rerun()
        st.caption("Asset ids")
        st.dataframe(
            pd.DataFrame({"asset_id": manager.lake.asset_ids(selected_source)}),
            use_container_width=True,
            height=180,
        )
        st.caption("Data item ids")
        st.dataframe(
            pd.DataFrame({"data_item_id": manager.lake.data_item_ids(selected_source)}),
            use_container_width=True,
            height=180,
        )

    st.subheader("Universes")
    with st.form("add_universe"):
        source = st.text_input("Universe source", value=selected_source or "tushare")
        name = st.text_input("Universe name", value="my-universe")
        raw_assets = st.text_area("Asset ids, one per line", value="")
        submitted = st.form_submit_button("Save universe")
    if submitted:
        asset_ids = [line.strip() for line in raw_assets.splitlines() if line.strip()]
        _upsert_universe(
            config,
            UniverseConfig(source=source, name=name, asset_ids=asset_ids),
        )
        if asset_ids:
            manager.define_universe(source, name, asset_ids)
        st.success(f"Saved universe {name}")

    if config.universes:
        st.dataframe(
            pd.DataFrame(
                {
                    "source": item.source,
                    "name": item.name,
                    "assets": len(item.asset_ids),
                }
                for item in config.universes
            ),
            use_container_width=True,
        )
        delete_universe = st.selectbox(
            "Delete configured universe",
            options=[""] + [f"{item.source}/{item.name}" for item in config.universes],
        )
        if st.button("Delete universe config", disabled=not delete_universe):
            source, name = delete_universe.split("/", maxsplit=1)
            config.universes = [
                item
                for item in config.universes
                if not (item.source == source and item.name == name)
            ]
            st.success(f"Deleted universe config {delete_universe}")
            st.rerun()


def _data_sources(config: GuiConfig) -> None:
    st.subheader("Configured Sources")
    if st.button("Add Tushare", use_container_width=False):
        if "tushare" not in config.source_names():
            config.sources.append(SourceConfig(name="tushare", provider="tushare"))
        st.success("Tushare source configured")

    for source in config.sources:
        with st.expander(f"{source.name} ({source.provider})", expanded=True):
            source.enabled = st.checkbox(
                "Enabled",
                value=source.enabled,
                key=f"source-enabled-{source.name}",
            )
            if st.button(
                "Delete source config",
                key=f"delete-source-{source.name}",
                use_container_width=False,
            ):
                config.sources = [
                    item for item in config.sources if item.name != source.name
                ]
                st.success(f"Deleted source config {source.name}")
                st.rerun()
            _table_editor(source)


def _table_editor(source: SourceConfig) -> None:
    st.caption("Tables")
    for index, table in enumerate(source.tables):
        cols = st.columns([2, 2, 2, 2, 1])
        table.name = cols[0].text_input(
            "Table",
            value=table.name,
            key=f"table-name-{source.name}-{index}",
        )
        table.kind = cols[1].selectbox(
            "Kind",
            options=TABLE_KINDS,
            index=TABLE_KINDS.index(table.kind),
            key=f"table-kind-{source.name}-{index}",
        )
        table.start_date = cols[2].text_input(
            "Start",
            value=table.start_date,
            key=f"table-start-{source.name}-{index}",
        )
        table.end_date = cols[3].text_input(
            "End",
            value=table.end_date or "",
            key=f"table-end-{source.name}-{index}",
        ) or None
        table.workers = cols[4].number_input(
            "Workers",
            min_value=1,
            max_value=64,
            value=table.workers,
            key=f"table-workers-{source.name}-{index}",
        )
        if st.button(
            "Delete table config",
            key=f"delete-table-{source.name}-{index}",
            use_container_width=False,
        ):
            del source.tables[index]
            st.success(f"Deleted table config {table.name}")
            st.rerun()
    with st.form(f"add-table-{source.name}"):
        name = st.text_input("New table", value="daily")
        kind = st.selectbox("Table kind", options=TABLE_KINDS)
        start_date = st.text_input("Start date", value="2000-01-01")
        workers = st.number_input("Workers", min_value=1, max_value=64, value=4)
        add = st.form_submit_button("Add table")
    if add:
        source.tables.append(
            TableConfig(
                source=source.name,
                name=name,
                kind=kind,
                start_date=start_date,
                workers=int(workers),
            )
        )
        st.success(f"Added {name}")


def _retrieve_data(config: GuiConfig, lake: LocalDataLake) -> None:
    st.subheader("Retrieve Data")
    sources = lake.list_sources() or tuple(config.source_names()) or ("tushare",)
    source = st.selectbox("Source", options=sources, key="retrieve-source")
    tables = tuple(table for src, table in lake.list_tables(source)) or tuple(
        table.name for table in config.tables_for(source)
    )
    table = st.selectbox("Table", options=tables or ("daily",), key="retrieve-table")
    cols = st.columns(4)
    year = _optional_int(cols[0].text_input("Year", value=""))
    month = _optional_int(cols[1].text_input("Month", value=""))
    fields = _csv_values(cols[2].text_input("Fields", value=""))
    universe = _csv_values(cols[3].text_input("Universe", value=""))

    try:
        data = lake.read(source, table, year=year, month=month)
    except DatasetNotFoundError:
        data = pd.DataFrame()
        st.info("No local data found for this selection.")
    if fields and not data.empty:
        available = [field for field in fields if field in data.columns]
        if available:
            data = data.loc[:, available]
    st.dataframe(data.head(500), use_container_width=True)

    panel_field = st.text_input("Panel field", value=fields[0] if fields else "close")
    include_core = st.checkbox("Include optional bagelquant-core conversion snippet")
    selection = RetrievalSelection(
        lake_root=config.lake_root,
        source=source,
        table=table,
        year=year,
        month=month,
        fields=tuple(fields),
        universe=tuple(universe),
        panel_field=panel_field,
        include_core_conversion=include_core,
    )
    snippet_choice = st.radio(
        "Generated code",
        options=("LocalDataLake read", "Loader read", "Panel agreement"),
        horizontal=True,
    )
    snippet = {
        "LocalDataLake read": lake_read_snippet,
        "Loader read": loader_read_snippet,
        "Panel agreement": panel_agreement_snippet,
    }[snippet_choice](selection)
    st.code(snippet, language="python")


def _update_data_lake(
    config: GuiConfig,
    manager: DataLakeManager,
    config_path: Path,
    provider_ready: bool,
) -> None:
    st.subheader("Update Data Lake")
    if not provider_ready:
        st.warning("Set TUSHARE_TOKEN or Streamlit secrets before provider updates.")

    table_options = [
        table
        for source in config.sources
        for table in source.tables
        if source.enabled and table.enabled
    ]
    labels = [f"{table.source}/{table.name}" for table in table_options]
    selected = st.selectbox("Configured table", options=labels)
    if st.button("Update now", disabled=not provider_ready or not table_options):
        table = table_options[labels.index(selected)]
        with st.spinner(f"Updating {selected}"):
            snapshots = run_table_update(manager, table)
        st.success(f"Created {len(snapshots)} snapshot partition set(s)")

    st.subheader("Periodic Jobs")
    _job_editor(config)
    if st.button("Run due jobs", disabled=not provider_ready):
        with st.spinner("Running due jobs"):
            snapshots = run_due_jobs(manager, config, now=datetime.now(UTC))
        save_config(config, config_path)
        st.success(f"Created {len(snapshots)} snapshot partition set(s)")


def _job_editor(config: GuiConfig) -> None:
    if config.periodic_jobs:
        st.dataframe(
            pd.DataFrame(
                {
                    "name": job.name,
                    "source": job.source,
                    "table": job.table,
                    "kind": job.kind,
                    "schedule": f"{job.every} {job.unit}",
                    "enabled": job.enabled,
                    "last_run_at": job.last_run_at,
                }
                for job in config.periodic_jobs
            ),
            use_container_width=True,
        )
    with st.form("add-periodic-job"):
        name = st.text_input("Job name", value="tushare-daily")
        source = st.text_input("Source", value="tushare")
        table = st.text_input("Table", value="daily")
        kind = st.selectbox("Kind", options=TABLE_KINDS)
        every = st.number_input("Every", min_value=1, value=1)
        unit = st.selectbox("Unit", options=SCHEDULE_UNITS, index=2)
        start_date = st.text_input("Start date", value="2000-01-01")
        workers = st.number_input("Workers", min_value=1, max_value=64, value=4)
        add = st.form_submit_button("Add job")
    if add:
        config.periodic_jobs.append(
            PeriodicJobConfig(
                name=name,
                source=source,
                table=table,
                kind=kind,
                every=int(every),
                unit=unit,
                start_date=start_date,
                workers=int(workers),
            )
        )
        st.success(f"Added job {name}")


def _upsert_universe(config: GuiConfig, universe: UniverseConfig) -> None:
    config.universes = [
        item
        for item in config.universes
        if not (item.source == universe.source and item.name == universe.name)
    ]
    config.universes.append(universe)


def _csv_values(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def _optional_int(value: str) -> int | None:
    return int(value) if value.strip() else None


if __name__ == "__main__":
    main()
