import pandas as pd
import pytest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

from bagelquant_data.sources.tushare.client import build_client
from bagelquant_data.sources.tushare.source import TushareSource, _to_tushare_params
from bagelquant_data.sources.tushare import source as source_module


def test_tushare_maps_default_daily_date_to_trade_date() -> None:
    assert _to_tushare_params({"date": "2025-01-02"}) == {"trade_date": "20250102"}


def test_provider_quota_coordinates_cooldown_and_pacing(monkeypatch) -> None:
    clock = [100.0]
    monkeypatch.setattr(source_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(source_module.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))

    class Limited:
        def balancesheet(self):
            raise RuntimeError("接口频率超限(500次/分钟)")

    source = TushareSource(client=Limited())
    with pytest.raises(RuntimeError, match="500"):
        source.fetch("balancesheet", {})
    assert source.wait_for_request("income")  # independent endpoint
    assert clock[0] == 100.0
    assert source.wait_for_request("balancesheet")
    assert clock[0] >= 161.0
    before = clock[0]
    assert source.wait_for_request("balancesheet")
    assert clock[0] - before >= 60 / 450 - 1e-9


def test_provider_admission_is_cancelable_without_calling_provider(monkeypatch) -> None:
    from bagelquant_data.core.dataset import DatasetSpec
    from bagelquant_data.pipeline.update import _fetch_one

    source = TushareSource(client=object())
    source._rate_limits["balancesheet"] = (1.0, source_module.time.monotonic() + 60)
    checks = [0]

    def canceled():
        checks[0] += 1
        return checks[0] > 2

    result = _fetch_one(
        DatasetSpec(name="balancesheet", source="tushare", update_type="by_asset"),
        source, {"id": "A"}, "request", 3, 60.0, canceled,
    )
    assert result.status == "cancelled"


def test_tushare_preserves_configured_daily_date_parameter() -> None:
    assert _to_tushare_params({"pub_date": "2025-01-02"}) == {"pub_date": "20250102"}


class _IndustryClient:
    def __init__(self) -> None:
        self.requests: list[tuple[str, str]] = []

    def index_classify(self, *, level: str) -> pd.DataFrame:
        assert level == "L1"
        return pd.DataFrame({"index_code": ["801020.SI", "801010.SI"]})

    def index_member_all(self, *, l1_code: str, is_new: str) -> pd.DataFrame:
        self.requests.append((l1_code, is_new))
        return pd.DataFrame(
            {
                "l1_code": [l1_code],
                "ts_code": ["000001.SZ" if l1_code == "801010.SI" else "000002.SZ"],
            }
        )


def test_tushare_calls_the_declared_provider_api_without_dataset_special_cases() -> None:
    client = _IndustryClient()

    result = TushareSource(client=client).fetch(
        "index_member_all", {"l1_code": "801010.SI", "is_new": "N"}
    )

    assert client.requests == [("801010.SI", "N")]
    assert result.to_dicts() == [{"l1_code": "801010.SI", "ts_code": "000001.SZ"}]


def test_tushare_builds_one_client_per_worker_thread(monkeypatch) -> None:
    clients: list[object] = []

    class Client:
        def trade_cal(self) -> pd.DataFrame:
            return pd.DataFrame({"is_open": [1]})

    def build(_token):
        client = Client()
        clients.append(client)
        return client

    monkeypatch.setattr("bagelquant_data.sources.tushare.source.build_client", build)
    source = TushareSource(token="secret")
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: source.fetch("trade_cal", {}), range(4)))

    assert all(result.height == 1 for result in results)
    assert 1 <= len(clients) <= 2


def test_tushare_client_passes_token_without_writing_user_home(monkeypatch) -> None:
    calls: list[str] = []
    expected = object()
    module = SimpleNamespace(
        pro_api=lambda token: calls.append(token) or expected,
        set_token=lambda _token: (_ for _ in ()).throw(
            AssertionError("set_token must not persist credentials")
        ),
    )
    monkeypatch.setattr(
        "bagelquant_data.sources.tushare.client.importlib.import_module",
        lambda _name: module,
    )

    assert build_client("secret") is expected
    assert calls == ["secret"]
