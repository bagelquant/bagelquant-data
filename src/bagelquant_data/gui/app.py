"""Streamlit application for managing a local BagelQuant data lake."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

from bagelquant_data.gui import tushare_catalog
from bagelquant_data.gui.codegen import (
    RetrievalSelection,
    lake_read_snippet,
    loader_read_snippet,
    panel_agreement_snippet,
)
from bagelquant_data.gui.config import (
    DEFAULT_CONFIG_PATH,
    GuiConfig,
    SourceConfig,
    TableConfig,
    load_config,
    save_config,
)
from bagelquant_data.gui.orchestration import (
    build_registry,
    default_tushare_source,
    run_all_table_updates,
    token_available,
    token_from_config,
)
from bagelquant_data.lake import DataLakeManager, LocalDataLake
from bagelquant_data.utils.exceptions import DatasetNotFoundError

TABLE_KINDS = ("general", "price", "fundamental", "fundamental_vip")


def main() -> None:
    """Run the Streamlit GUI."""

    st.set_page_config(
        page_title="BagelQuant Data Lake",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    st.title("BagelQuant Data Lake")

    config_path = Path(
        st.sidebar.text_input("Config file", value=str(DEFAULT_CONFIG_PATH))
    )
    config = _session_config(config_path)
    config.lake_root = st.sidebar.text_input("Lake root", value=config.lake_root)

    token = token_from_config(config, streamlit_secrets=st.secrets)
    token_state = (
        "available"
        if token or token_available(streamlit_secrets=st.secrets)
        else "missing"
    )
    st.sidebar.caption(f"Tushare token: {token_state}")
    if st.sidebar.button("Save config", width="stretch"):
        save_config(config, config_path)
        st.sidebar.success("Config saved")

    lake = LocalDataLake(config.lake_root)
    registry = build_registry(tushare_token=token)
    manager = DataLakeManager(lake, registry=registry)

    tabs = st.tabs(["Lake Setup", "Data Sources", "Retrieve Data"])
    with tabs[0]:
        _lake_setup(config, manager)
    with tabs[1]:
        _data_sources(config, manager, config_path, token is not None)
    with tabs[2]:
        _retrieve_data(config, lake)


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
    col_a, col_b = st.columns(2)
    col_a.metric("Sources", len(sources))
    col_b.metric("Tables", len(tables))

    st.dataframe(
        pd.DataFrame(tables, columns=["source", "table"])
        if tables
        else pd.DataFrame(columns=["source", "table"]),
        width="stretch",
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
                width="stretch",
                height=180,
            )
            confirm_delete = st.checkbox(
                "Confirm local table deletion",
                key="confirm-delete-local-table",
            )
            if st.button(
                "Delete local table",
                disabled=not confirm_delete,
                width="content",
            ):
                manager.delete(selected_source, selected_table)
                st.success(f"Deleted {selected_source}/{selected_table}")
                st.rerun()
    st.subheader("Data Items")
    data_items = _data_item_catalog(manager.lake, tables)
    item_sources = (
        sorted(data_items["source"].unique().tolist()) if not data_items.empty else []
    )
    item_source = st.selectbox(
        "Data item source",
        options=["All", *item_sources],
        key="data-item-source",
    )
    filtered_items = data_items
    if item_source != "All":
        filtered_items = filtered_items.loc[filtered_items["source"] == item_source]
    item_tables = (
        sorted(filtered_items["table"].unique().tolist())
        if not filtered_items.empty
        else []
    )
    item_table = st.selectbox(
        "Data item table",
        options=["All", *item_tables],
        key="data-item-table",
    )
    if item_table != "All":
        filtered_items = filtered_items.loc[filtered_items["table"] == item_table]
    st.dataframe(
        filtered_items,
        width="stretch",
        height=260,
    )


def _data_sources(
    config: GuiConfig,
    manager: DataLakeManager,
    config_path: Path,
    provider_ready: bool,
) -> None:
    _ensure_tushare_stock_basic(config)
    _sync_source_tokens_from_session(config)
    st.subheader("Update Data Lake")
    settings = st.columns([2, 1])
    config.update_start_date = settings[0].text_input(
        "Update start date",
        value=config.update_start_date,
        key="update-start-date",
    )
    config.update_workers = int(
        settings[1].number_input(
            "Workers",
            min_value=1,
            max_value=64,
            value=config.update_workers,
            key="update-workers",
        )
    )
    current_token = token_from_config(config, streamlit_secrets=st.secrets)
    provider_ready = provider_ready or current_token is not None
    if not provider_ready:
        st.warning("Configure a Tushare token before provider updates.")
    if st.button(
        "Update data lake",
        disabled=not provider_ready,
        width="content",
    ):
        update_manager = DataLakeManager(
            manager.lake,
            registry=build_registry(tushare_token=current_token),
        )
        with st.spinner("Updating enabled data source tables"):
            snapshots = run_all_table_updates(update_manager, config)
        save_config(config, config_path)
        st.success(f"Created {len(snapshots)} snapshot partition set(s)")

    st.subheader("Configured Sources")
    for source in config.sources:
        st.markdown(f"#### {source.name} ({source.provider})")
        source_controls = st.columns([1, 3, 1])
        source.enabled = source_controls[0].checkbox(
            "Enabled",
            value=source.enabled,
            key=f"source-enabled-{source.name}",
        )
        if source.provider == "tushare":
            source.token = source_controls[1].text_input(
                "Tushare token",
                value=source.token or "",
                type="password",
                key=f"source-token-{source.name}",
            ) or None
        if source_controls[2].button(
            "Delete source",
            key=f"delete-source-{source.name}",
            width="content",
        ):
            config.sources = [
                item for item in config.sources if item.name != source.name
            ]
            st.success(f"Deleted source config {source.name}")
            st.rerun()
        _table_editor(source)

    st.divider()
    if st.button("Add source", width="content"):
        if "tushare" not in config.source_names():
            config.sources.append(default_tushare_source())
            st.success("Tushare source configured")
            st.rerun()
        st.info("Tushare source is already configured.")


def _sync_source_tokens_from_session(config: GuiConfig) -> None:
    for source in config.sources:
        if source.provider != "tushare":
            continue
        key = f"source-token-{source.name}"
        if key in st.session_state:
            source.token = str(st.session_state[key]) or None


def _table_editor(source: SourceConfig) -> None:
    st.caption("Tables")
    if not source.tables:
        st.info("No tables configured for this source.")
    for category in _configured_table_categories(source):
        st.markdown(f"##### {category}")
        header = st.columns([1, 2, 2, 4, 2])
        header[0].caption("Enabled")
        header[1].caption("Table")
        header[2].caption("Kind")
        header[3].caption("Description")
        header[4].caption("Action")
        for index, table in _configured_tables_for_category(source, category):
            cols = st.columns([1, 2, 2, 4, 2])
            required = source.provider == "tushare" and table.name == "stock_basic"
            table.enabled = cols[0].checkbox(
                "Enabled",
                value=True if required else table.enabled,
                key=f"table-enabled-{source.name}-{index}",
                disabled=required,
                label_visibility="collapsed",
            )
            table.name = cols[1].text_input(
                "Table",
                value=table.name,
                key=f"table-name-{source.name}-{index}",
                disabled=required,
                label_visibility="collapsed",
            )
            table.kind = cols[2].selectbox(
                "Kind",
                options=TABLE_KINDS,
                index=TABLE_KINDS.index(table.kind),
                key=f"table-kind-{source.name}-{index}",
                disabled=required,
                label_visibility="collapsed",
            )
            cols[3].caption(
                tushare_catalog.tushare_table_description(table.name) or "-"
            )
            if required:
                cols[4].caption("Required")
                table.enabled = True
                table.kind = "general"
                continue
            _table_delete_controls(source, index, table, cols[4])
    with st.expander("Add table", expanded=False):
        add_cols = st.columns([2, 5, 2, 1])
        categories = _tushare_table_categories()
        selected_category = add_cols[0].selectbox(
            "Category",
            options=categories,
            key=f"add-table-category-{source.name}",
        )
        catalog = _tushare_table_catalog_for_category(selected_category)
        labels = [entry.label for entry in catalog]
        selected_label = add_cols[1].selectbox(
            "New table",
            options=labels,
            key=f"add-table-name-{source.name}-{selected_category}",
        )
        selected_entry = catalog[labels.index(selected_label)]
        kind = add_cols[2].selectbox(
            "Table kind",
            options=TABLE_KINDS,
            index=TABLE_KINDS.index(selected_entry.default_kind),
            key=f"add-table-kind-{source.name}-{selected_entry.api}",
        )
        if add_cols[3].button(
            "Add table",
            key=f"add-table-submit-{source.name}",
            width="content",
        ):
            _add_source_table(source, selected_entry.api, kind)


def _add_source_table(source: SourceConfig, name: str, kind: str) -> None:
    if any(table.name == name for table in source.tables):
        st.warning(f"{name} is already configured.")
        return
    source.tables.append(
        TableConfig(
            source=source.name,
            name=name,
            kind=kind,
        )
    )
    for key in (
        f"add-table-category-{source.name}",
        f"add-table-submit-{source.name}",
    ):
        st.session_state.pop(key, None)
    for key in list(st.session_state):
        if key.startswith(
            (
                f"add-table-name-{source.name}-",
                f"add-table-kind-{source.name}-",
            )
        ):
            st.session_state.pop(key, None)
    st.success(f"Added {name}")
    st.rerun()


def _configured_table_categories(source: SourceConfig) -> tuple[str, ...]:
    categories = []
    for table in source.tables:
        category = _table_category(table.name)
        if category not in categories:
            categories.append(category)
    return tuple(categories)


def _configured_tables_for_category(
    source: SourceConfig,
    category: str,
) -> tuple[tuple[int, TableConfig], ...]:
    indexed = [
        (index, table)
        for index, table in enumerate(source.tables)
        if _table_category(table.name) == category
    ]
    return tuple(
        sorted(
            indexed,
            key=lambda item: (
                0 if item[1].name == "stock_basic" else 1,
                _table_display_name(item[1].name),
                item[1].name,
            ),
        )
    )


def _table_category(api: str) -> str:
    entry = tushare_catalog.tushare_table_entry(api)
    return entry.category_zh if entry is not None else "未分类"


def _table_display_name(api: str) -> str:
    entry = tushare_catalog.tushare_table_entry(api)
    return entry.name_zh if entry is not None else api


def _tushare_table_categories() -> tuple[str, ...]:
    helper = getattr(tushare_catalog, "tushare_table_categories", None)
    if callable(helper):
        return helper()
    catalog = tushare_catalog.tushare_table_catalog()
    return tuple(
        dict.fromkeys(entry.category_zh for entry in catalog)
    )


def _tushare_table_catalog_for_category(category_zh: str):
    helper = getattr(tushare_catalog, "tushare_table_catalog_for_category", None)
    if callable(helper):
        return helper(category_zh)
    return tuple(
        entry
        for entry in tushare_catalog.tushare_table_catalog()
        if entry.category_zh == category_zh
    )


def _table_delete_controls(
    source: SourceConfig,
    index: int,
    table: TableConfig,
    container,
) -> None:
    key = f"pending-delete-table-{source.name}-{index}"
    if st.session_state.get(key) != table.name:
        if container.button(
            "Delete table config",
            key=f"delete-table-{source.name}-{index}",
            width="content",
        ):
            st.session_state[key] = table.name
            st.rerun()
        return
    confirm_cols = container.columns(2)
    if confirm_cols[0].button(
        "Confirm",
        key=f"confirm-delete-table-{source.name}-{index}",
        width="content",
    ):
        del source.tables[index]
        st.session_state.pop(key, None)
        st.success(f"Deleted table config {table.name}")
        st.rerun()
    if confirm_cols[1].button(
        "Cancel",
        key=f"cancel-delete-table-{source.name}-{index}",
        width="content",
    ):
        st.session_state.pop(key, None)
        st.rerun()


def _ensure_tushare_stock_basic(config: GuiConfig) -> None:
    for source in config.sources:
        if source.provider != "tushare":
            continue
        source.tables = [
            table
            for table in source.tables
            if not (table.source == source.name and table.name == "stock_basic")
        ]
        source.tables.insert(
            0,
            TableConfig(
                source=source.name,
                name="stock_basic",
                kind=tushare_catalog.default_tushare_table_kind("stock_basic"),
                enabled=True,
            ),
        )


def _retrieve_data(config: GuiConfig, lake: LocalDataLake) -> None:
    st.subheader("Retrieve Data")
    sources = lake.list_sources() or tuple(config.source_names()) or ("tushare",)
    source = st.selectbox("Source", options=sources, key="retrieve-source")
    tables = tuple(table for src, table in lake.list_tables(source)) or tuple(
        table.name for table in config.tables_for(source)
    )
    table = st.selectbox("Table", options=tables or ("daily",), key="retrieve-table")
    cols = st.columns(2)
    start_date = cols[0].text_input("Start date", value="2000-01-01")
    end_date = cols[1].text_input("End date", value="2024-12-31")

    try:
        data = lake.read(source, table)
    except DatasetNotFoundError:
        data = pd.DataFrame()
        st.info("No local data found for this selection.")

    available_fields = [str(column) for column in data.columns]
    panel_field = st.selectbox(
        "Panel field",
        options=available_fields or ["close"],
        index=(available_fields.index("close") if "close" in available_fields else 0),
        key="panel-field",
    )
    preview = _filter_by_date(data, start_date=start_date, end_date=end_date)
    st.dataframe(preview.iloc[:50, :20], width="stretch")

    include_core = st.checkbox("Include optional bagelquant-core conversion snippet")
    if include_core:
        st.caption(
            "The conversion snippet shows how to turn a panel agreement payload "
            "into downstream bagelquant-core Domain and Panel objects."
        )
    selection = RetrievalSelection(
        lake_root=config.lake_root,
        source=source,
        table=table,
        fields=(panel_field,),
        panel_field=panel_field,
        start_date=start_date,
        end_date=end_date,
        include_core_conversion=include_core,
    )
    snippet_choice = st.radio(
        "Generated code",
        options=("LocalDataLake read", "Loader read", "Panel agreement"),
        horizontal=True,
    )
    st.caption(
        {
            "LocalDataLake read": (
                "Reads the selected local lake table directly from disk."
            ),
            "Loader read": (
                "Uses Loader for lake-first retrieval with provider fallback options."
            ),
            "Panel agreement": (
                "Shapes one field into a panel-ready agreement for downstream systems."
            ),
        }[snippet_choice]
    )
    snippet = {
        "LocalDataLake read": lake_read_snippet,
        "Loader read": loader_read_snippet,
        "Panel agreement": panel_agreement_snippet,
    }[snippet_choice](selection)
    st.code(snippet, language="python")


def _data_item_catalog(
    lake: LocalDataLake,
    tables: tuple[tuple[str, str], ...],
) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for source, table in tables:
        if table.startswith("__"):
            continue
        try:
            data = lake.read(source, table)
        except DatasetNotFoundError:
            continue
        fields = [str(column) for column in data.reset_index().columns]
        for field in fields:
            if field in {"index", "create_time", "delete_flag"}:
                continue
            rows.append(
                {
                    "source": source,
                    "table": table,
                    "field": field,
                    "data_item_id": f"{source}_{table}_{field}",
                }
            )
    return pd.DataFrame(rows, columns=["source", "table", "field", "data_item_id"])


def _filter_by_date(
    data: pd.DataFrame,
    *,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    if data.empty:
        return data
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    if isinstance(data.index, pd.DatetimeIndex):
        return data.loc[(data.index >= start) & (data.index <= end)]
    date_column = next(
        (
            column
            for column in ("date", "trade_date", "f_ann_date", "datetime", "timestamp")
            if column in data.columns
        ),
        None,
    )
    if date_column is None:
        return data
    dates = pd.to_datetime(data[date_column].astype(str), errors="coerce")
    return data.loc[(dates >= start) & (dates <= end)]


def _running_under_streamlit() -> bool:
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
    except Exception:
        return False
    return get_script_run_ctx(suppress_warning=True) is not None


def _run_with_streamlit() -> None:
    from streamlit.web import cli as streamlit_cli

    sys.argv = ["streamlit", "run", str(Path(__file__).resolve()), *sys.argv[1:]]
    raise SystemExit(streamlit_cli.main())


if __name__ == "__main__":
    if not _running_under_streamlit():
        _run_with_streamlit()
    main()
