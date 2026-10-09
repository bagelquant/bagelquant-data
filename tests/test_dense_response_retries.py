"""Dense daily coverage retries transient empty responses without relaxing truth."""
import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec


class Source:
    name = 'custom'

    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = 0

    def fetch(self, api, params):
        self.calls += 1
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return response


def run(tmp_path, responses, *, required=True, attempts=3, cancel=None, required_start=None, pagination=None):
    source = Source(responses)
    lake = DataLake.open(data_meta_path=tmp_path / 'meta.sqlite', lake_path=tmp_path / 'lake')
    lake.catalog.sources.register(source)
    lake.raw.ingest(DatasetSpec('calendar', 'general'),
                    pl.DataFrame({'time': ['20250102'], 'is_open': [1]}))
    lake.raw.register(DatasetSpec('daily', 'by_date', calendar='calendar',
                                field_mappings={'trade_date': 'time', 'ts_code': 'asset_id'}))
    report = lake.raw.initialize('daily', source='custom', start='2025-01-02', end='2025-01-02',
        max_retries=attempts, retry_backoff_seconds=0, cancel_requested=(None if cancel is None else lambda: cancel(source)),
        source_options={'require_nonempty_scopes': required, 'required_nonempty_start': required_start, 'pagination': pagination, 'page_size': 1})
    return lake, source, report


def valid():
    return pl.DataFrame({'trade_date': ['20250102'], 'ts_code': ['A'], 'close': [10.0]})


def test_transient_empty_then_exception_shares_retry_budget_and_commits(tmp_path):
    lake, source, report = run(tmp_path, [pl.DataFrame(), RuntimeError('transient'), valid()])
    assert source.calls == report.request_count == 3
    assert report.status == 'success'
    assert report.rows_committed == 1
    assert lake.integrity.update_scopes('daily', source='custom')[0]['status'] == 'success'
    assert lake.integrity.coverage('daily', source='custom', start='2025-01-02', end='2025-01-02')['complete']


def test_persistent_dense_empty_exhausts_budget_without_coverage(tmp_path):
    lake, source, report = run(tmp_path, [pl.DataFrame()] * 3)
    assert source.calls == report.request_count == 3
    assert report.status != 'success'
    assert report.rows_committed == 0
    assert lake.integrity.update_scopes('daily', source='custom')[0]['status'] == 'invalid'
    assert not lake.raw.manifest('daily', source='custom')


@pytest.mark.parametrize('options', [{'required': False}, {'required_start': '2025-01-03'}])
def test_allowed_empty_does_not_retry(tmp_path, options):
    lake, source, report = run(tmp_path, [pl.DataFrame()], **options)
    assert source.calls == report.request_count == 1
    assert report.status == 'no_data'
    assert lake.integrity.update_scopes('daily', source='custom')[0]['status'] == 'empty'


def test_cancellation_during_dense_retry_preserves_uncommitted_scope(tmp_path):
    # Cancellation becomes visible after the first physical call.
    lake, source, report = run(tmp_path, [pl.DataFrame()], cancel=lambda s: s.calls > 0)
    assert source.calls == report.request_count == 1
    assert report.status == 'cancelled'
    assert report.rows_committed == 0
    assert not lake.raw.manifest('daily', source='custom')
    assert not lake.integrity.active_update_leases()


def test_offset_first_page_retries_but_terminal_empty_is_permitted(tmp_path):
    lake, source, report = run(tmp_path, [pl.DataFrame(), valid(), pl.DataFrame()], pagination='offset')
    assert source.calls == report.request_count == 3
    assert report.status == 'success'
    assert report.rows_committed == 1
    assert lake.integrity.update_scopes('daily', source='custom')[0]['status'] == 'success'


def test_persistent_empty_offset_first_page_is_not_complete_inventory(tmp_path):
    lake, source, report = run(tmp_path, [pl.DataFrame()] * 3, pagination='offset')
    assert source.calls == report.request_count == 3
    assert report.status != 'success'
    assert not lake.raw.manifest('daily', source='custom')
    assert lake.integrity.update_scopes('daily', source='custom')[0]['status'] == 'invalid'
