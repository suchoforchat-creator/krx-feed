"""Offline regressions for the 2026-10-01 failure and delayed source dates."""
from datetime import date, datetime
import importlib
import json
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from src import compute, optional_pykrx, reconcile
from src.session_dates import completed_session
from src.source_dates import bind_note, note_date, quote_day
from src.utils import SCHEMA_COLUMNS
from src.sources.dxy import DXYCollector
from src.sources.us_yields import USTYieldCollector
from src.sources.kr_rates import KRXKorRates
from src.sources.krx_breadth import KRXBreadthCollector, determine_target
from src.kis.client import KISClient
from src.failure_diagnostic import write_failure
import pipeline
import update_history

KST = ZoneInfo('Asia/Seoul')


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('TEST_NETWORK_FORBIDDEN')
    monkeypatch.setattr('socket.socket.connect', forbidden)
    monkeypatch.setattr('requests.sessions.Session.request', forbidden)


def raw(asset='KOSPI', day='2026-10-01', value=100, field='close', note=None):
    row = {'ts_kst': pd.Timestamp(day, tz=KST), 'field': field, 'value': value,
           'source': 'fixture', 'quality': 'secondary', 'url': 'https://example.org'}
    if note is not None:
        row['notes'] = note
    return pd.DataFrame([row])


def records(target='2026-10-01', observation='2026-10-02 01:00'):
    return compute.compute_records(datetime.fromisoformat(observation).replace(tzinfo=KST),
                                   {a: raw(a, target, v) for a, v in [('KOSPI', 100), ('KOSDAQ', 90)]})


def publish(tmp_path, rows, target=date(2026, 10, 1), now=None):
    latest = tmp_path / 'latest.csv'
    history = tmp_path / 'history.csv'
    pd.DataFrame(rows, columns=SCHEMA_COLUMNS).to_csv(latest, index=False)
    update_history.upsert_from_latest(latest, history, now=now or datetime(2026, 10, 2, 1, tzinfo=KST),
                                     target_date=target, require_source_dates=True,
                                     debug_dir=tmp_path / 'debug')
    return pd.read_csv(history, dtype=str).fillna('')


@pytest.mark.parametrize('run,phase,expected', [
    ('2026-09-29T01:09:00', '1700', '2026-09-28'),
    ('2026-10-02T00:01:00', '1700', '2026-10-01'),
    ('2026-10-02T23:50:00', '1700', '2026-10-02'),
    ('2026-10-02T15:29:00', '1700', '2026-10-01'),
    ('2026-10-05T17:00:00', '1700', '2026-10-02'),
    ('2026-10-06T07:30:00', '0730', '2026-10-02'),
    ('2026-10-09T20:00:00', '1700', '2026-10-08'),
    ('2026-09-28T07:30:00', '0730', '2026-09-23'),
    ('2026-10-03T01:00:00', '1700', '2026-10-02'),
])
def test_completed_session_boundaries(run, phase, expected):
    assert completed_session(datetime.fromisoformat(run).replace(tzinfo=KST), phase).isoformat() == expected


def test_calendar_expiry_does_not_infer_2027():
    with pytest.raises(ValueError, match='CALENDAR_EXPIRED'):
        completed_session(datetime(2027, 1, 2, 17, tzinfo=KST))


@pytest.mark.parametrize('phase', ['0730', '1700'])
def test_calendar_expiry_cannot_hide_in_early_phase(phase):
    with pytest.raises(ValueError, match='CALENDAR_EXPIRED'):
        completed_session(datetime(2027, 1, 1, 1, tzinfo=KST), phase)


def test_before_calendar_coverage_is_not_inferred():
    with pytest.raises(ValueError, match='CALENDAR_BEFORE_COVERAGE'):
        completed_session(datetime(2025, 5, 1, 17, tzinfo=KST))


def test_pykrx_login_failure_is_lazy_and_cached(monkeypatch):
    monkeypatch.setattr(optional_pykrx, '_modules', {})
    monkeypatch.setattr(optional_pykrx, '_error_type', None)
    calls = []
    def fail(name):
        calls.append(name)
        raise json.JSONDecodeError('secret token must not escape', '', 0)
    monkeypatch.setattr(optional_pykrx, 'import_module', fail)
    importlib.reload(importlib.import_module('src.kis.client'))
    importlib.reload(importlib.import_module('src.sources.krx_breadth'))
    assert calls == []  # same import path that crashed 10/1 now does not log in
    for name in ['stock', 'bond', 'stock']:
        with pytest.raises(optional_pykrx.OptionalPykrxUnavailable) as exc:
            optional_pykrx.get_pykrx(name)
        assert 'secret' not in str(exc.value)
    assert calls == ['pykrx']


def test_other_sources_work_after_pykrx_failure(monkeypatch):
    client = KISClient({'mode': 'simulation', 'fallback': {'indexes': {'KOSPI': '^KS11'}}})
    monkeypatch.setattr(client, '_yf_history', lambda *a: raw())
    monkeypatch.setattr(optional_pykrx, '_error_type', 'JSONDecodeError')
    assert not client.get_index_series('KOSPI').empty


def test_compute_keeps_observation_time_and_exact_12_columns():
    rows = records()
    row = rows[0]
    assert list(row) == SCHEMA_COLUMNS
    assert row['ts_kst'] == '2026-10-02 01:00'
    assert note_date(row['notes']) == date(2026, 10, 1)
    assert 'freshness=PRIOR_DATE_REFERENCE' in row['notes']
    assert 'source_age_calendar_days=1' in row['notes']


def test_compute_pairs_metadata_with_last_valid_value():
    frame = pd.concat([raw(day='2026-10-01'), raw(day='2026-10-02', value=None)], ignore_index=True)
    frame.loc[1, 'source'] = 'wrong-last-null-row'
    row = compute.compute_records(datetime(2026, 10, 2, 17, tzinfo=KST), {'KOSPI': frame})[0]
    assert row['source'] == 'fixture'
    assert note_date(row['notes']) == date(2026, 10, 1)


def test_undated_html_keeps_value_but_not_fictitious_day():
    frame = raw(day='2026-10-02', note=bind_note('html', None))
    row = compute.compute_records(datetime(2026, 10, 2, 17, tzinfo=KST), {'KOSPI': frame})[0]
    assert row['value'] == 100
    assert note_date(row['notes']) is None


def test_multi_source_dates_not_silently_collapsed():
    raw_frames = {'UST10Y': raw(day='2026-10-01', value=4.0, field='yield'),
                  'UST2Y': raw(day='2026-09-30', value=3.0, field='yield')}
    rows = compute.compute_records(datetime(2026, 10, 2, 17, tzinfo=KST), raw_frames)
    row = next(r for r in rows if r['asset'] == '2s10s_US')
    assert row['value'] == 100
    assert note_date(row['notes']) is None
    assert 'source_dates_mixed=2026-10-01|2026-09-30' in row['notes']


def test_future_source_value_not_published():
    row = compute.compute_records(datetime(2026, 10, 1, 17, tzinfo=KST), {'KOSPI': raw(day='2026-10-02')})[0]
    assert row['value'] is None
    assert 'future_source_date_rejected' in row['notes']


def test_mixed_date_derived_value_rejects_known_future_leg():
    inputs = {'UST10Y': raw(day='2026-10-02', value=4, field='yield'),
              'UST2Y': raw(day='2026-09-30', value=3, field='yield')}
    rows = compute.compute_records(datetime(2026, 10, 1, 17, tzinfo=KST), inputs)
    row = next(r for r in rows if r['asset'] == '2s10s_US')
    assert row['value'] is None
    assert row['change_abs'] is None
    assert row['change_pct'] is None
    assert 'future_source_date_rejected' in row['notes']


def test_reconcile_preserves_source_date(tmp_path):
    old = records()
    path = tmp_path / 'daily.csv'
    pd.DataFrame(old).to_csv(path, index=False)
    new = records()
    new[0]['value'] = 110
    out = reconcile.reconcile(new, path)
    assert note_date(out[0]['notes']) == date(2026, 10, 1)
    assert 'revised' in out[0]['notes']


def test_midnight_run_writes_source_day_not_runtime(tmp_path):
    result = publish(tmp_path, records())
    assert list(result['time_kst']) == ['2026-10-01 15:30:00']


def test_global_newer_quote_cannot_select_krx_target(tmp_path):
    rows = records()
    rows.append(pipeline._rec('VIX', 'spot', 22, 'pt', source_date=date(2026, 10, 2)))
    result = publish(tmp_path, rows)
    assert result.iloc[0]['time_kst'].startswith('2026-10-01')
    assert result.iloc[0]['vix'] == ''


def test_prior_macro_quote_preserved_with_own_date_not_target(tmp_path):
    rows = records()
    rows.append(pipeline._rec('UST10Y', 'yield', 4.2, '%', source='fixture', source_date=date(2026, 9, 30)))
    result = publish(tmp_path, rows).iloc[0]
    assert result['ust10y'] == '4.2'
    assert 'metric_date.ust10y=2026-09-30' in result['src_tag']
    assert 'prior_date_reference=ust10y' in result['src_tag']
    assert result['quality'] == 'secondary'


def test_unknown_new_macro_quote_not_promoted(tmp_path):
    rows = records()
    rows.append(pipeline._rec('UST10Y', 'yield', 4.2, '%', source='fixture'))
    result = publish(tmp_path, rows).iloc[0]
    assert result['ust10y'] == ''
    assert 'metric_date.ust10y=' not in result['src_tag']


def test_older_macro_observation_cannot_replace_newer_source_day(tmp_path):
    rows = records()
    rows.append(pipeline._rec('UST10Y', 'yield', 4.2, '%', source='fixture', source_date=date(2026, 9, 30)))
    rows.append(pipeline._rec('UST10Y', 'yield', 4.0, '%', source='fixture', source_date=date(2026, 9, 29)))
    result = publish(tmp_path, rows).iloc[0]
    assert result['ust10y'] == '4.2'
    assert 'metric_date.ust10y=2026-09-30' in result['src_tag']


def test_stale_index_does_not_manufacture_target(tmp_path):
    rows = records(target='2026-09-23')
    history = tmp_path / 'history.csv'
    history.write_text('preserve original bytes', encoding='utf-8')
    before = history.read_bytes()
    with pytest.raises(ValueError, match='SOURCE_DATE_NOT_READY:KOSPI'):
        publish(tmp_path, rows)
    assert history.read_bytes() == before


def test_empty_input_does_not_manufacture_runtime_row(tmp_path):
    with pytest.raises(ValueError, match='SOURCE_DATE_NOT_READY:empty_latest'):
        publish(tmp_path, [])
    assert not (tmp_path / 'history.csv').exists()


def test_partial_retry_preserves_same_day_known_value_and_old_rows(tmp_path):
    rows = records()
    rows.append(pipeline._rec('VIX', 'spot', 22, 'pt', source='fixture', source_date=date(2026, 10, 1)))
    first = publish(tmp_path, rows)
    old_row = {k: '' for k in update_history.HISTORY_COLUMNS}
    old_row.update(time_kst='2026-09-24 15:30:00', kospi='old-unreviewed', src_tag='original')
    pd.concat([pd.DataFrame([old_row]), first], ignore_index=True).to_csv(tmp_path / 'history.csv', index=False)
    second = publish(tmp_path, records())
    assert second.iloc[-1]['vix'] == '22.0'
    assert 'metric_date.vix=2026-10-01' in second.iloc[-1]['src_tag']
    assert second.iloc[0].to_dict() == old_row  # no retrospective closed-date normalization


def test_same_date_legacy_value_retained_as_unverified_not_newly_verified(tmp_path):
    old = {k: '' for k in update_history.HISTORY_COLUMNS}
    old.update(time_kst='2026-10-01 15:30:00', vix='22', src_tag='legacy', quality='final')
    pd.DataFrame([old]).to_csv(tmp_path / 'history.csv', index=False)
    row = publish(tmp_path, records()).iloc[0]
    assert row['vix'] == '22'
    assert 'metric_date.vix=UNKNOWN' in row['src_tag']
    assert 'retained_legacy_unverified=vix' in row['src_tag']
    assert row['quality'] == 'secondary'


def test_closed_target_rejected_without_history(tmp_path):
    with pytest.raises(ValueError, match='CLOSED_SESSION_TARGET_REJECTED'):
        publish(tmp_path, records(), date(2026, 9, 25))
    assert not (tmp_path / 'history.csv').exists()


def test_early_future_target_rejected(tmp_path):
    with pytest.raises(ValueError, match='FUTURE_OR_UNCLOSED_SESSION_REJECTED'):
        publish(tmp_path, records(), date(2026, 10, 2))


def test_fred_preserves_observation_date(monkeypatch):
    collector = USTYieldCollector()
    monkeypatch.setattr(collector, '_request', lambda url: SimpleNamespace(text='observation_date,DGS2\n2026-09-30,3.5\n2026-10-01,.\n'))
    value, url = collector._fetch_fred('DGS2')
    frame = collector._build_frame(asset='UST2Y', value=value, source='fred', url=url,
                                    quality='secondary', note='ok', target=date(2026, 10, 2)).frame
    assert note_date(frame.iloc[0]['notes']) == date(2026, 9, 30)


def test_dxy_stooq_preserves_quote_date(monkeypatch):
    collector = DXYCollector()
    monkeypatch.setattr(collector, '_request', lambda url: SimpleNamespace(text='Date,Close\n2026-10-01,100.5\n'))
    value, url = collector._fetch_stooq(['https://example.org'])
    frame = collector._build_frame(value, source='stooq', quality='secondary', url=url,
                                   note='ok', target=date(2026, 10, 2)).frame
    assert note_date(frame.iloc[0]['notes']) == date(2026, 10, 1)


def test_undated_dxy_and_number_index_are_unknown():
    frame = DXYCollector()._build_frame(100, source='marketwatch', quality='secondary',
                                      url='https://example.org', note='ok', target=date(2026, 10, 2)).frame
    assert note_date(frame.iloc[0]['notes']) is None
    assert quote_day(5) is None


def test_kr_rates_frame_requires_actual_date():
    payload = {'value': 3.2, 'prev': None, 'source': 'investing', 'quality': 'secondary', 'url': '', 'note': 'html'}
    collector = KRXKorRates()
    assert note_date(collector._build_frame('KR3Y', date(2026, 10, 2), payload).iloc[0]['notes']) is None
    payload['source_date'] = date(2026, 9, 30)
    assert note_date(collector._build_frame('KR3Y', date(2026, 10, 2), payload).iloc[0]['notes']) == date(2026, 9, 30)


def test_kis_yield_value_and_date_share_one_row():
    collector = KRXKorRates()
    collector._kis_cache = pd.DataFrame({'ts_kst': pd.to_datetime(['2026-09-29', '2026-09-30', '2026-10-01']),
                                         'kr3y': [3.1, 3.2, None]})
    payload, _ = collector._fetch_kis(date(2026, 10, 2), 'KR3Y', '3년')
    assert payload['source_date'] == date(2026, 9, 30)
    assert payload['value'] == 3.2


def test_history_mapping_only_one_eod_function():
    frame = pd.DataFrame([{'asset': a, 'key': k} for a, k in update_history.LATEST_TO_HISTORY])
    assert set(pipeline.mark_eod(frame)['window']) == {'EOD'}


def test_dated_rate_fallback_is_preferred_to_undated_first_success(monkeypatch):
    collector = KRXKorRates()
    unknown = {'value': 3.8, 'source': 'kofia', 'quality': 'secondary', 'url': '', 'note': 'html'}
    known = dict(unknown, value=3.2, source='BOK_ECOS', source_date=date(2026, 10, 1))
    monkeypatch.setattr(collector, '_fetch_krx', lambda *a: (None, 'fixture'))
    monkeypatch.setattr(collector, '_fetch_kofia', lambda *a: (unknown, None))
    monkeypatch.setattr(collector, '_fetch_ecos', lambda *a: (known, None))
    out = collector.fetch(date(2026, 10, 2))
    assert out.frames['KR3Y'].iloc[0]['value'] == 3.2
    assert note_date(out.frames['KR3Y'].iloc[0]['notes']) == date(2026, 10, 1)


def test_naver_unparseable_quote_date_is_not_target(monkeypatch):
    collector = KRXKorRates()
    monkeypatch.setattr(collector, '_discover_naver_code', lambda *a: None)
    monkeypatch.setattr(collector._session, 'get', lambda *a, **k: SimpleNamespace(
        text='<table><tr><td>unverified</td><td>3.2</td></tr></table>', raise_for_status=lambda: None))
    payload, error = collector._fetch_naver(date(2026, 10, 2), 'KR3Y', '3년')
    assert error is None
    assert payload['source_date'] is None
    assert note_date(collector._build_frame('KR3Y', date(2026, 10, 2), payload).iloc[0]['notes']) is None


def test_treasury_fallback_retains_found_day(monkeypatch):
    collector = USTYieldCollector()
    html = '<table><tr><th>Date</th><th>2 Yr</th><th>10 Yr</th></tr><tr><td>2026-09-30</td><td>3.2</td><td>3.9</td></tr></table>'
    monkeypatch.setattr(collector, '_request', lambda *a: SimpleNamespace(text=html))
    result = collector._fetch_treasury_textview(date(2026, 10, 2))
    frame = collector._build_frame(asset='UST2Y', value=result['UST2Y'], source='treasury', url='',
                                    quality='final', note='fixture', target=date(2026, 10, 2)).frame
    assert note_date(frame.iloc[0]['notes']) == date(2026, 9, 30)


def test_kis_business_date_is_not_replaced_by_runtime():
    client = KISClient({})
    result = client._normalize_timeseries([{'xymd': '20261001', 'clos': '100'}], 120, {})
    assert result.iloc[0]['source_date'] == date(2026, 10, 1)


def test_merged_frame_missing_source_date_column_value_uses_original_timestamp():
    first = raw(day='2026-10-01')
    first['source_date'] = date(2026, 10, 1)
    second = raw(day='2026-09-30', field='advance', value=200)
    rows = compute.compute_records(datetime(2026, 10, 2, 17, tzinfo=KST),
                                   {'KOSPI': pd.concat([first, second], ignore_index=True)})
    row = next(r for r in rows if r['asset'] == 'KOSPI' and r['key'] == 'advance')
    assert note_date(row['notes']) == date(2026, 9, 30)


def test_safe_diagnostic_omits_exception_body(tmp_path):
    try:
        raise ValueError('SECRET_AUTH_BODY')
    except Exception as exc:
        out = write_failure(tmp_path / 'failure.json', exc, 'fixture')
    assert out['exception_type'] == 'ValueError'
    assert 'SECRET_AUTH_BODY' not in (tmp_path / 'failure.json').read_text()


def test_workflow_propagates_git_failure_without_force_or_new_schedule():
    text = (Path(__file__).resolve().parents[1] / '.github' / 'workflows' / 'data-pipeline.yml').read_text(encoding='utf-8')
    assert 'git push ||' not in text
    assert 'git pull --rebase origin main ||' not in text
    assert 'git add -A out/ ||' not in text
    assert '--force' not in text
    assert "cron: '30 22 * * 0-4'" in text
    assert "cron: '00 08 * * 1-5'" in text
    assert text.count('git push\n') == 2
    assert '  push:' not in text


def test_real_pipeline_entrypoint_midnight_fixture(monkeypatch, tmp_path):
    class Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 2, 1, 10, tzinfo=KST)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(pipeline, 'datetime', Frozen)
    monkeypatch.setattr(pipeline, 'parse_args', lambda: SimpleNamespace(phase='1700', reconcile=True))
    monkeypatch.setattr(pipeline, 'load_yaml', lambda p: {})
    seen = []
    def collect(config, phase, *, run_ts, target_date):
        seen.append(target_date)
        return {a: raw(a) for a in ['KOSPI', 'KOSDAQ']}, {}, {}
    monkeypatch.setattr(pipeline, 'collect_raw', collect)
    monkeypatch.setattr(pipeline, 'fetch_vix', lambda: pipeline._rec('VIX', 'spot', 21, 'pt', source_date=date(2026, 10, 1)))
    assert pipeline.main() == 0
    assert seen == [date(2026, 10, 1)]
    latest = pd.read_csv(tmp_path / 'out' / 'latest.csv')
    history = pd.read_csv(tmp_path / 'out' / 'history.csv')
    assert list(latest.columns) == SCHEMA_COLUMNS
    assert list(history['time_kst']) == ['2026-10-01 15:30:00']
