import pandas as pd
from concurrent.futures import ThreadPoolExecutor

from bagelquant_data.sources.tushare.source import TushareSource, _to_tushare_params


def test_tushare_maps_default_daily_date_to_trade_date() -> None:
    assert _to_tushare_params({"date": "2025-01-02"}) == {"trade_date": "20250102"}


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
