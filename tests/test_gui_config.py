from __future__ import annotations

from datetime import UTC, datetime

from bagelquant_data.gui.config import (
    GuiConfig,
    PeriodicJobConfig,
    SourceConfig,
    TableConfig,
    UniverseConfig,
    load_config,
    save_config,
)


def test_gui_config_defaults_when_missing(tmp_path) -> None:
    config = load_config(tmp_path / "missing.yaml")

    assert config.lake_root == ".bagelquant-data-lake"
    assert config.sources == []


def test_gui_config_round_trips_yaml_without_secrets(tmp_path) -> None:
    path = tmp_path / "gui.yaml"
    config = GuiConfig(
        lake_root=str(tmp_path / "lake"),
        sources=[
            SourceConfig(
                name="tushare",
                tables=[
                    TableConfig(
                        source="tushare",
                        name="daily",
                        kind="price",
                        fields=["close"],
                    )
                ],
            )
        ],
        universes=[
            UniverseConfig(
                source="tushare",
                name="banks",
                asset_ids=["tushare_000001.SZ"],
            )
        ],
        periodic_jobs=[
            PeriodicJobConfig(name="daily", source="tushare", table="daily")
        ],
    )

    save_config(config, path)
    loaded = load_config(path)

    assert "token" not in path.read_text(encoding="utf-8").lower()
    assert loaded.lake_root == str(tmp_path / "lake")
    assert loaded.sources[0].tables[0].fields == ["close"]
    assert loaded.universes[0].asset_ids == ["tushare_000001.SZ"]
    assert loaded.periodic_jobs[0].table == "daily"


def test_periodic_job_due_tracks_schedule() -> None:
    job = PeriodicJobConfig(
        name="daily",
        source="tushare",
        table="daily",
        last_run_at="2024-01-01T00:00:00+00:00",
    )

    assert not job.due(datetime(2024, 1, 1, 12, tzinfo=UTC))
    assert job.due(datetime(2024, 1, 2, 1, tzinfo=UTC))
