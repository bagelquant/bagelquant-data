from bagelquant_data.sources.tushare.source import _to_tushare_params


def test_tushare_maps_default_daily_date_to_trade_date() -> None:
    assert _to_tushare_params({"date": "2025-01-02"}) == {"trade_date": "20250102"}


def test_tushare_preserves_configured_daily_date_parameter() -> None:
    assert _to_tushare_params({"pub_date": "2025-01-02"}) == {"pub_date": "20250102"}
