"""Schema-eight single-body storage and metadata completion/range contracts."""
from datetime import UTC, date, datetime
import json

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, RawInput, ItemInput, DataItemSpec, ProducerUpdatePolicy
from bagelquant_data.inputs import InputSelectionScope
from test_ledger_updates import LedgerSource, _daily_lake


def lake_at(path):
    return DataLake.open(data_meta_path=path / "meta.sqlite",lake_path=path / "lake")


def spec():
    return DatasetSpec("quotes","by_date",date_kind="calendar",field_mappings={"time":"time","asset_id":"asset_id"})


def rows(day=1, close=1., volume=5., asset="A"):
    return pl.DataFrame({"time":[date(2020,1,day)],"asset_id":[asset],"close":[close],"volume":[volume]})


def receipt(lake,end="2020-01-04"):
    return lake.inputs.freeze({"raw":RawInput("custom","quotes",view="snapshot")},information_cutoff=end)


def test_large_publication_has_single_original_and_constant_size_metadata(tmp_path):
    lake = lake_at(tmp_path)
    count = 20_000
    frame = pl.DataFrame({"time":[date(2020,1,1)]*count,"asset_id":[f"A{i:05}" for i in range(count)],"close":[1.]*count})
    lake.raw.ingest(spec(),frame,mode="initialize",ingested_at=datetime(2020,1,1,tzinfo=UTC))
    frozen = receipt(lake)
    with lake._data_meta.connect() as db:
        tables = {row[0] for row in db.execute("select name from sqlite_master where type='table'")}
        assert not tables.intersection({"version_batches","input_batch_index","input_batch_value_index","version_check_records"})
        assert db.execute("select count(*) from version_segments").fetchone()[0] == 1
        columns = {row[1] for row in db.execute("pragma table_info(version_segments)")}
        assert "payload" not in columns
        metadata_bytes = db.execute("select sum(length(bounds_json)+length(field_changes_json)) from version_segments").fetchone()[0]
        assert metadata_bytes < 4096
    files = list(lake.lake_path.rglob("*.parquet"))
    assert len(files) == 1
    stored = pl.read_parquet(files[0],hive_partitioning=False)
    assert "_record_id" not in stored.columns and "_payload_hash" not in stored.columns
    assert len(json.dumps({key:dict(value) for key,value in frozen.evidence.items()},default=str)) < 8192
    assert lake.inputs.verify(frozen)["valid"]


def test_append_preserves_original_bytes_and_old_range_metadata(tmp_path,monkeypatch):
    lake = lake_at(tmp_path)
    lake.raw.ingest(spec(),rows(),ingested_at=datetime(2020,1,1,tzinfo=UTC))
    before = receipt(lake)
    original = next(lake.lake_path.rglob("*.parquet"))
    original_bytes = original.read_bytes()
    token = lake.inputs.range_identity(before,"raw",end="2020-01-01",fields=("close",))
    lake.raw.ingest(spec(),rows(2),ingested_at=datetime(2020,1,2,tzinfo=UTC))
    after = receipt(lake)
    assert original.read_bytes() == original_bytes
    assert len(list(lake.lake_path.rglob("*.parquet"))) == 2
    from bagelquant_data import inputs,input_index
    with monkeypatch.context() as patch:
        patch.setattr(inputs,"read_batch",lambda *a,**k:pytest.fail("range check decoded historical data"))
        patch.setattr(input_index,"project_original",lambda *a,**k:pytest.fail("range check projected original data"))
        assert lake.inputs.range_identity(after,"raw",end="2020-01-01",fields=("close",)) == token
    assert lake.inputs.read(before,"raw").collect()["close"].to_list() == [1.]


def test_unconsumed_field_revision_preserves_consumed_range(tmp_path):
    lake = lake_at(tmp_path)
    lake.raw.ingest(spec(),rows(),ingested_at=datetime(2020,1,1,tzinfo=UTC))
    before = receipt(lake)
    close = lake.inputs.range_identity(before,"raw",fields=("close",))
    volume = lake.inputs.range_identity(before,"raw",fields=("volume",))
    lake.raw.ingest(spec(),rows(volume=9.),ingested_at=datetime(2020,1,2,tzinfo=UTC))
    after = receipt(lake)
    assert lake.inputs.range_identity(after,"raw",fields=("close",)) == close
    assert lake.inputs.range_identity(after,"raw",fields=("volume",)) != volume
    assert lake.inputs.read(before,"raw").collect()["volume"].to_list() == [5.]
    assert lake.inputs.read(after,"raw").collect()["volume"].to_list() == [9.]


def test_domain_range_summary_is_read_only_on_repeat(tmp_path,monkeypatch):
    lake = lake_at(tmp_path)
    lake.raw.ingest(spec(),pl.concat([rows(),rows(2,asset="B")]),ingested_at=datetime(2020,1,2,tzinfo=UTC))
    before = receipt(lake)
    coordinates = pl.DataFrame({"time":[date(2020,1,1),date(2020,1,2)],"asset_id":["A","B"]})
    scope = InputSelectionScope("raw",view="snapshot",fields=("close",),coordinates=coordinates)
    lake.inputs.prepare_coordinate_selector(coordinates)
    prepared = lake.inputs.prepare_range_selections(before,[scope])
    from bagelquant_data import input_index
    with monkeypatch.context() as patch:
        patch.setattr(input_index,"read_selector",lambda *a,**k:pytest.fail("unchanged range decoded selector"))
        assert lake.inputs.resolve_selection_descriptors(before,prepared) == prepared
    lake.raw.ingest(spec(),rows(2,close=8.,asset="A"),ingested_at=datetime(2020,1,3,tzinfo=UTC))
    after = receipt(lake)
    resolved = lake.inputs.resolve_selection_descriptors(after,prepared)
    assert resolved[0]["content_token"] == prepared[0]["content_token"]
    continued = lake.inputs.prepare_range_selections(after,[scope])
    assert continued[0]["proof_source"] == prepared[0]["proof_source"]
    assert lake.inputs.verify(after)["valid"]


def test_first_baseline_visibility_is_natural_event_and_frozen_audit(tmp_path):
    lake = lake_at(tmp_path)
    lake.raw.ingest(spec(),rows(),mode="initialize",ingested_at=datetime(2020,1,1,tzinfo=UTC))
    original = receipt(lake)
    lake.raw.ingest(spec(),rows(),ingested_at=datetime(2020,1,2,tzinfo=UTC))
    visible = receipt(lake)
    lake.raw.ingest(spec(),rows(),ingested_at=datetime(2020,1,3,tzinfo=UTC))
    token = lake.inputs.range_identity(visible,"raw",fields=("close",))
    lake.raw.ingest(spec(),rows(),ingested_at=datetime(2020,1,1,tzinfo=UTC))
    assert lake.inputs.range_identity(receipt(lake),"raw",fields=("close",)) == token
    assert lake.raw.read("quotes",source="custom",strict=True,as_of="2020-01-01").collect().is_empty()
    assert lake.inputs.read(original,"raw",strict=True).collect().is_empty()
    assert lake.inputs.read(visible,"raw",strict=True).collect()["close"].to_list() == [1.]
    events = list(lake.lake_path.rglob(".visibility/*.parquet"))
    assert len(events) == 1
    assert set(pl.read_parquet(events[0]).columns) == {"source_time","asset_id","version_commit","available_date"}
    assert lake.inputs.verify(visible)["valid"]
    events[0].write_bytes(b"damaged")
    with pytest.raises(RuntimeError,match="visibility.*checksum"):
        lake.inputs.verify(visible)
    assert lake.inputs.verify(original)["valid"]


def test_original_damage_requires_backup_while_metadata_checks_stay_fast(tmp_path,monkeypatch):
    lake = lake_at(tmp_path)
    lake.raw.ingest(spec(),rows(),ingested_at=datetime(2020,1,1,tzinfo=UTC))
    frozen = receipt(lake)
    path = next(lake.lake_path.rglob("*.parquet"))
    path.write_bytes(b"damaged")
    assert lake.inputs.is_current(frozen)
    assert lake.inputs.range_identity(frozen,"raw")
    with pytest.raises(RuntimeError,match="checksum.*backup"):
        lake.inputs.verify(frozen)


def test_success_flags_cover_ranges_and_failed_retry_does_not_hide_work(tmp_path):
    source = LedgerSource()
    lake = _daily_lake(tmp_path,source)
    lake.raw.initialize("daily",source="custom",start="2025-01-02",end="2025-01-03")
    flags = lake.raw.completion_statuses(source="custom",datasets=["daily"])
    assert len(flags) == 1 and flags[0]["definition_current"] and flags[0]["publication_current"]
    assert lake.raw.plan_updates(["daily"],source="custom",start="2025-01-02",end="2025-01-03") == []
    assert lake.raw.plan_updates(["daily"],source="custom",start="2025-01-01",end="2025-01-03")
    scopes = lake.integrity.update_scopes(source="custom",dataset="daily")
    lake.integrity.reset_update_scopes([scopes[0]["id"]])
    assert not lake.raw.completion_statuses(source="custom",datasets=["daily"])[0]["publication_current"]
    assert lake.raw.plan_updates(["daily"],source="custom",start="2025-01-02",end="2025-01-03")


def test_disjoint_completions_do_not_certify_holes(tmp_path):
    lake = lake_at(tmp_path)
    lake.raw.ingest(spec(),rows(),ingested_at=datetime(2020,1,1,tzinfo=UTC))
    lake.items.register(DataItemSpec("item",(RawInput("custom","quotes"),),producer_key="p",producer_revision="1"))
    def produce(context):
        return pl.DataFrame({"time":[context.start],"asset_id":["A"],"value":[1.]})
    lake.items.register_producer("p","1",produce,update_policy=ProducerUpdatePolicy("same_date"))
    lake.items.initialize("item",start="2020-01-01",end="2020-01-01")
    # Two explicit independent range publications are not a contiguous completion.
    from bagelquant_data.storage.data_meta import DataMetaStore
    with lake._data_meta.connect() as db:
        DataMetaStore.write_completion(db,node_key="items/item",definition_hash=lake.items.completion_statuses(names=["item"])[0]["definition_hash"],
            completed_start="2020-01-04",completed_end="2020-01-04",checked_target="2020-01-04",
            semantic_version="v",dependency_digest="",result_ref="second")
    flag = lake.items.completion_statuses(names=["item"])[0]
    assert flag["completed_start"] == flag["completed_end"] == "2020-01-04"


@pytest.mark.parametrize('count',[1,20_000])
def test_wide_initial_batch_uses_shared_field_change_evidence(tmp_path,count):
    lake = lake_at(tmp_path)
    frame = pl.DataFrame({'time':[date(2020,1,1)]*count,
        'asset_id':[f'A{i}' for i in range(count)],
        **{f'field_{index:03}':[float(index)]*count for index in range(200)}})
    lake.raw.ingest(spec(),frame,mode='initialize',ingested_at=datetime(2020,1,1,tzinfo=UTC))
    with lake._data_meta.connect() as db:
        row = db.execute('select field_changes_json,s.schema_ipc,bounds_json from version_segments b join immutable_schemas s on s.digest=b.schema_ref').fetchone()
        changes = json.loads(row[0])
        assert set(changes) == {'$initial'}
        assert len([name for name in changes['$initial']['fields'] if name.startswith('field_')]) == 200
        assert len(row[0])+len(row[2]) < 4096
        assert len(row[1]) < 12_000


def test_narrow_completion_cannot_recertify_changed_old_prefix(tmp_path):
    lake = lake_at(tmp_path)
    lake.raw.ingest(spec(),rows(),ingested_at=datetime(2020,1,1,tzinfo=UTC))
    store = lake._data_meta
    definition = store.dataset_spec_hash('custom','quotes')
    def complete(first,last):
        store.complete_node(node_key='custom/quotes',definition_hash=definition,
            completed_start=first,completed_end=last,checked_target=last,result_ref='isolated-completion')
    complete('2020-01-01','2020-01-31')
    complete('2020-01-01','2020-01-05')
    assert lake.raw.completion_statuses(source='custom')[0]['checked_target'] == '2020-01-31'
    lake.raw.ingest(spec(),rows(close=9.),ingested_at=datetime(2020,1,31,tzinfo=UTC))
    assert not lake.raw.completion_statuses(source='custom')[0]['publication_current']
    complete('2020-01-30','2020-01-31')
    flag = lake.raw.completion_statuses(source='custom')[0]
    assert flag['completed_start'] == '2020-01-30'
    assert flag['publication_current']
    assert lake.raw.plan_updates(['quotes'],source='custom',start='2020-01-01',end='2020-01-31')


@pytest.mark.parametrize('general',[False,True])
def test_field_change_identity_is_audited_against_original_file(tmp_path,general):
    lake = lake_at(tmp_path)
    definition = DatasetSpec('quotes','general') if general else spec()
    frame = pl.DataFrame({'asset_id':['A'],'close':[1.]}) if general else rows()
    lake.raw.ingest(definition,frame,ingested_at=datetime(2020,1,1,tzinfo=UTC))
    lake.raw.ingest(definition,frame.with_columns(pl.lit(2.).alias('close')),
        ingested_at=datetime(2020,1,2,tzinfo=UTC))
    with lake._data_meta.connect() as db:
        row = db.execute('select commit_seq,partition_path,field_changes_json from version_segments order by commit_seq desc limit 1').fetchone()
        changes = json.loads(row[2])
        changes['close']['identity'] = 'forged-field-token'
        db.execute('update version_segments set field_changes_json=? where commit_seq=? and partition_path=?',
            (json.dumps(changes),row[0],row[1]))
    frozen = receipt(lake)
    with pytest.raises(RuntimeError,match='field change identity'):
        lake.inputs.verify(frozen)


def test_general_calendar_projection_binds_date_value_pairs(tmp_path):
    lake = lake_at(tmp_path)
    definition = DatasetSpec('calendar','general')
    frame = pl.DataFrame({'time':[date(2020,1,1),date(2020,1,2)],'is_open':[0,1],'note':['a','b']})
    lake.raw.ingest(definition,frame,ingested_at=datetime(2020,1,1,tzinfo=UTC))
    def freeze():
        return lake.inputs.freeze({'calendar':RawInput('custom','calendar',view='snapshot')},information_cutoff='2020-01-03')
    before = freeze()
    token = lake.inputs.range_identity(before,'calendar',fields=('time','is_open'))
    lake.raw.ingest(definition,frame.with_columns(pl.lit('changed').alias('note')),ingested_at=datetime(2020,1,2,tzinfo=UTC))
    assert lake.inputs.range_identity(freeze(),'calendar',fields=('time','is_open')) == token
    lake.raw.ingest(definition,frame.with_columns(pl.Series('is_open',[1,0])),ingested_at=datetime(2020,1,3,tzinfo=UTC))
    assert lake.inputs.range_identity(freeze(),'calendar',fields=('time','is_open')) != token


def test_natural_visibility_preserves_null_and_signed_float_keys(tmp_path):
    lake = lake_at(tmp_path)
    from dataclasses import replace
    definition = replace(spec(),primary_key_extra=('variant',),nullable_primary_key_extra=('variant',))
    initial = pl.concat([rows(close=1.),rows(close=2.),rows(close=3.)]).with_columns(
        pl.Series('variant',[None,-0.0,0.0],dtype=pl.Float64))
    lake.raw.ingest(definition,initial,mode='initialize',ingested_at=datetime(2020,1,1,tzinfo=UTC))
    lake.raw.ingest(definition,initial.head(2),ingested_at=datetime(2020,1,2,tzinfo=UTC))
    known = lake.raw.read('quotes',source='custom',view='versions',strict=True).collect()
    assert known['close'].sort().to_list() == [1.,2.]
    assert known.filter(pl.col('variant').is_null()).height == 1
    assert known.height == 2
    assert lake.inputs.verify(receipt(lake))['valid']


def test_raw_description_edit_preserves_flags_and_numerical_identity(tmp_path,monkeypatch):
    from dataclasses import replace
    from bagelquant_data import inputs,input_index
    source = LedgerSource()
    lake = _daily_lake(tmp_path,source)
    lake.raw.initialize('daily',source='custom',start='2025-01-02',end='2025-01-03')
    request = {'raw':RawInput('custom','daily',view='snapshot')}
    before = lake.inputs.freeze(request,information_cutoff='2025-01-03')
    token = lake.inputs.range_identity(before,'raw',fields=('close',))
    identity = lake.inputs.request_identity(before,'raw')
    flags = lake.raw.completion_statuses(source='custom',datasets=['daily'])
    requests = list(source.requests)
    original = lake.raw.get('daily',source='custom')
    lake.raw.register(replace(original,description='Presentation only'))
    def forbidden(*args,**kwargs):
        pytest.fail('description-only change replayed provider or original values')
    monkeypatch.setattr(source,'fetch',forbidden)
    monkeypatch.setattr(inputs,'read_batch',forbidden)
    monkeypatch.setattr(input_index,'project_original',forbidden)
    after = lake.inputs.freeze(request,information_cutoff='2025-01-03')
    assert before.digest != after.digest
    assert before.dependency_digest == after.dependency_digest
    assert lake.inputs.request_identity(after,'raw') == identity
    assert lake.inputs.range_identity(after,'raw',fields=('close',)) == token
    assert lake.inputs.is_current(before)
    assert lake.raw.completion_statuses(source='custom',datasets=['daily']) == flags
    assert lake.raw.plan_updates(['daily'],source='custom',start='2025-01-02',end='2025-01-03') == []
    assert source.requests == requests
    assert lake.raw.get('daily',source='custom').description == 'Presentation only'
    assert 'Presentation only' in json.dumps(lake.catalog.export_declarations())


def test_item_description_edit_reuses_completed_history_and_keeps_metadata_semantic(tmp_path,monkeypatch):
    from dataclasses import replace
    from bagelquant_data import inputs,input_index
    source = LedgerSource()
    lake = _daily_lake(tmp_path,source)
    lake.raw.initialize('daily',source='custom',start='2025-01-02',end='2025-01-03')
    specification = DataItemSpec('twice',(RawInput('custom','daily',alias='raw'),),
        producer_key='twice',producer_revision='1',metadata={'factor':2})
    def produce(context):
        return context.frames['raw'].select(pl.col('source_time').alias('time'),'asset_id',
            (pl.col('close')*2).alias('value'))
    lake.items.register(specification)
    lake.items.register_producer('twice','1',produce,update_policy=ProducerUpdatePolicy('same_date'))
    lake.items.initialize('twice',start='2025-01-02',end='2025-01-03')
    request = {'item':ItemInput('twice')}
    before = lake.inputs.freeze(request,information_cutoff='2025-01-03')
    token = lake.inputs.range_identity(before,'item',fields=('value',))
    identity = lake.inputs.request_identity(before,'item')
    flags = lake.items.completion_statuses(names=['twice'])
    builds = lake.items.builds('twice')
    lake.items.register(replace(specification,description='Presentation only'))
    def forbidden(*args,**kwargs):
        pytest.fail('description-only change reran producer/provider or original values')
    lake.items.register_producer('twice','1',forbidden,update_policy=ProducerUpdatePolicy('same_date'))
    monkeypatch.setattr(source,'fetch',forbidden)
    monkeypatch.setattr(inputs,'read_batch',forbidden)
    monkeypatch.setattr(input_index,'project_original',forbidden)
    after = lake.inputs.freeze(request,information_cutoff='2025-01-03')
    assert before.digest != after.digest
    assert before.dependency_digest == after.dependency_digest
    assert lake.inputs.request_identity(after,'item') == identity
    assert lake.inputs.range_identity(after,'item',fields=('value',)) == token
    assert lake.inputs.is_current(before)
    assert lake.items.completion_statuses(names=['twice']) == flags
    assert lake.items.plan_update('twice',start='2025-01-02',end='2025-01-03') == []
    assert lake.items.update('twice',start='2025-01-02',end='2025-01-03').status == 'unchanged'
    assert lake.items.builds('twice') == builds
    assert lake.items.get('twice').description == 'Presentation only'
    assert 'Presentation only' in json.dumps(lake.catalog.export_declarations())
    lake.items.register(replace(specification,metadata={'factor':3}))
    assert not lake.items.completion_statuses(names=['twice'])[0]['definition_current']
    assert not lake.inputs.is_current(before)


def test_complete_at_advances_flag_without_synthetic_value_version(tmp_path):
    from bagelquant_data import ItemPublication
    lake = lake_at(tmp_path)
    lake.raw.ingest(spec(),rows(2),ingested_at=datetime(2020,1,2,tzinfo=UTC))
    source = receipt(lake,end='2020-01-05')
    lake.items.register(DataItemSpec('result',(RawInput('custom','quotes',view='snapshot'),),
        producer_key='external',producer_revision='1'))
    frame = pl.DataFrame({'time':[date(2020,1,2)]*2,'asset_id':['A']*2,'value':[1.,2.],
        'version_available_date':[date(2020,1,3),date(2020,1,4)]})
    with lake.items.publication(input_receipt=source) as publisher:
        publisher.publish([ItemPublication('result',frame,start='2020-01-02',end='2020-01-02',
            complete_at='2020-01-05')])
    versions = lake.items.read('result',view='versions').collect()
    assert versions.select('value','version_available_date').rows() == [
        (1.,date(2020,1,3)),(2.,date(2020,1,4))]
    flag = lake.items.completion_statuses(names=['result'])[0]
    assert flag['checked_target'] == '2020-01-05' and flag['definition_current'] and flag['publication_current']
    for day,value in [('2020-01-03',1.),('2020-01-04',2.),('2020-01-05',2.)]:
        assert lake.items.read('result',view='snapshot',as_of=day,strict=True).collect()['value'].to_list() == [value]
    frozen = lake.inputs.freeze({'item':ItemInput('result',view='snapshot')},information_cutoff='2020-01-05')
    before_files = {path:path.read_bytes() for path in lake.lake_path.rglob('*.parquet')}
    lake.items.replace_range('result',frame.tail(1),start='2020-01-02',end='2020-01-02',
        available_date='2020-01-06',input_receipt=source)
    assert lake.items.completion_statuses(names=['result'])[0]['checked_target'] == '2020-01-06'
    assert {path:path.read_bytes() for path in lake.lake_path.rglob('*.parquet')} == before_files
    assert lake.items.read('result',view='versions').collect().select('value','version_available_date').equals(
        versions.select('value','version_available_date'))
    assert lake.inputs.read(frozen,'item').collect()['value'].to_list() == [2.]
