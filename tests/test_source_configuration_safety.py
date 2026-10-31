"""Runtime credentials never enter portable Data metadata or adapter replay."""
import json

from bagelquant_data import DataLake


class FakeSource:
    name = "fake"

    def __init__(self):
        self.configurations = []

    def configure(self, **options):
        self.configurations.append(options)


def lake_at(root):
    return DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")


def test_credentials_are_runtime_only_including_nested_options_and_copies(tmp_path):
    lake = lake_at(tmp_path / "source")
    source = FakeSource()
    lake.catalog.sources.register(source)
    options = {"token": "synthetic-token", "calls_per_minute": 20,
        "nested": {"password": "synthetic-password", "mode": "safe",
            "entries": [{"API_KEY": "synthetic-api-key", "timeout": 3}]}}
    lake.catalog.sources.configure("fake", **options)
    assert source.configurations == [options]
    with lake._data_meta.connect() as db:
        saved = json.loads(db.execute("SELECT options_json FROM sources WHERE name='fake'").fetchone()[0])
    assert saved == {"token": "<redacted>", "calls_per_minute": 20,
        "nested": {"password": "<redacted>", "mode": "safe",
            "entries": [{"API_KEY": "<redacted>", "timeout": 3}]}}
    replay = FakeSource()
    lake.catalog.sources.register(replay)
    assert replay.configurations == [{"calls_per_minute": 20,
        "nested": {"mode": "safe", "entries": [{"timeout": 3}]}}]
    for operation, name in ((lake.integrity.snapshot, "snapshot"), (lake.integrity.backup, "backup")):
        root = tmp_path / name
        operation(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")
        for path in root.rglob("*"):
            if path.is_file():
                contents = path.read_bytes()
                for credential in (b"synthetic-token", b"synthetic-password", b"synthetic-api-key"):
                    assert credential not in contents


def test_old_plaintext_settings_are_not_replayed_or_silently_scrubbed(tmp_path):
    lake = lake_at(tmp_path)
    source = FakeSource()
    lake.catalog.sources.register(source)
    old = '{"token":"historical-synthetic-token","ordinary":4}'
    with lake._data_meta.connect() as db:
        db.execute("UPDATE sources SET options_json=? WHERE name='fake'", (old,))
    replay = FakeSource()
    lake.catalog.sources.register(replay)
    assert replay.configurations == [{"ordinary": 4}]
    with lake._data_meta.connect() as db:
        assert db.execute("SELECT options_json FROM sources WHERE name='fake'").fetchone()[0] == old


def test_adapter_registration_and_configuration_preserve_disabled_state(tmp_path):
    lake = lake_at(tmp_path)
    lake.catalog.sources.register(FakeSource())
    lake.catalog.sources.disable("fake")
    lake.catalog.sources.configure("fake", token="synthetic-token", timeout=3)
    assert lake.catalog.sources.list()[0]["enabled"] == 0
    lake.catalog.sources.register(FakeSource())
    status = lake.catalog.sources.list()[0]
    assert status["enabled"] == 0 and status["configured"] == 1
    lake.catalog.sources.enable("fake")
    assert lake.catalog.sources.list()[0]["enabled"] == 1
