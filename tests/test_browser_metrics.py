import asyncio
import json
import sqlite3
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from avito_bot.config import feed_url, load_config
from avito_bot.db import DB
from avito_bot.delivery import DeliveryWorker
from avito_bot.metrics import comparison_report, detection_events
from avito_bot.monitor import Monitor
from avito_bot.scraper import Scraper
from avito_bot.sources import FetchStatus, SourceState


def raw_item(item_id):
    return dict(id=item_id, title='RTX 3060 Ti', price='18000',
                href=f'/kazan/tovary_dlya_kompyutera/gpu_{item_id}', date='1 минуту назад',
                ts=int(time.time()*1000))


def setup(tmp_path, engine, mode, raw):
    cfg = load_config()
    cfg.data_dir, cfg.city, cfg.night_hours = tmp_path, 'kazan', (0, 0)
    cfg.browser_engine, cfg.fetch_mode, cfg.direct_interface = engine, mode, None
    cfg.sections = ['tovary_dlya_kompyutera']
    cfg.requests_per_minute = 1
    source = Scraper(tmp_path/'profile', None, browser_engine=engine, fetch_mode=mode)
    source.light_ok = True
    response = SimpleNamespace(status=raw['status'], all_headers=AsyncMock(return_value={'retry-after': '3600'}))
    source.page = SimpleNamespace(evaluate=AsyncMock(return_value=raw), goto=AsyncMock(return_value=response),
                                  wait_for_selector=AsyncMock(), is_closed=lambda:False,
                                  url=feed_url('kazan', cfg.feed))
    source.set_window = AsyncMock()
    return cfg, source


@pytest.mark.parametrize('engine', ['playwright','patchright'])
@pytest.mark.parametrize('mode', ['light_fetch','page_navigation'])
def test_modes_persist_items_metrics_and_never_duplicate_delivery(tmp_path, engine, mode):
    async def scenario():
        raw = dict(status=200, catalog=True, items=[raw_item(100)])
        cfg, source = setup(tmp_path, engine, mode, raw)
        path = tmp_path/'db.sqlite3'
        db = DB(path)
        db.add_subscription('3060', None, None, [], 'gpu', ['kazan'])
        alert = AsyncMock()
        monitor = Monitor(cfg, db, source, alert, recipients=lambda:[1,2])
        assert await monitor.poll_once()  # Baseline is preserved.
        raw['items'] = [raw_item(100), raw_item(101)]
        db.set('next_poll:local_browser','0')
        assert await monitor.poll_once()
        assert db.queue_counts() == {'pending':2}
        event = next(detection_events(db.conn))
        assert event['browser_engine'] == engine and event['fetch_mode'] == mode
        assert event['actual_mode'] == mode and event['is_warmup'] is False
        assert event['published_at'] is not None
        send = AsyncMock()
        worker = DeliveryWorker(db, send, lambda _:True)
        assert await worker.deliver_once() and await worker.deliver_once()
        db.close()
        # Restart with the other engine and the other loading mode on the same database.
        other = 'patchright' if engine == 'playwright' else 'playwright'
        other_mode = 'page_navigation' if mode == 'light_fetch' else 'light_fetch'
        cfg2, source2 = setup(tmp_path, other, other_mode, raw)
        db = DB(path)
        db.set('next_poll:local_browser','0')
        restarted = Monitor(cfg2, db, source2, alert, recipients=lambda:[1,2])
        assert await restarted.poll_once()
        assert db.queue_counts() == {'sent':2} and db.seen_count() == 2
        assert not await DeliveryWorker(db, send, lambda _:True).deliver_once()
        assert send.await_count == 2
        metric = db.conn.execute('SELECT browser_engine,fetch_mode,http_status,raw_count,duration_sec,'
                                 'access_limited,budget_rpm FROM fetch_metrics ORDER BY id LIMIT 1').fetchone()
        assert metric[:4] == (engine,mode,200,1) and metric[4] >= 0 and metric[5:] == (0,1)
        report = comparison_report(db.conn)
        group = next(g for g in report['groups'] if g['browser_engine']==engine and g['fetch_mode']==mode)
        assert group['configuration_totals']['checks'] == 2 and group['configuration_totals']['success_share'] == 1
        assert group['configuration_totals']['unique_listings'] == 2 and group['configuration_totals']['listing_observations'] == 3
        assert group['detection_to_telegram_sec']['n'] == 2
        assert group['publication_to_telegram_sec']['n'] == 2
        assert group['configuration_totals']['requests'] == {'measured_checks':0,'total':None}
        assert group['windows'][0]['censored']
        other_group = next(g for g in report['groups'] if g['browser_engine']==other and g['fetch_mode']==other_mode)
        assert other_group['detection_to_telegram_sec'] is None
        db.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('mode', ['light_fetch', 'page_navigation'])
@pytest.mark.parametrize(('code','challenge'), [(403,False),(429,False),(200,True)])
def test_switch_and_restart_do_not_reset_restrictions_or_budget(tmp_path, mode, code, challenge):
    async def scenario():
        raw = dict(status=code, challenge=challenge, items=[])
        cfg, source = setup(tmp_path, 'playwright', mode, raw)
        path = tmp_path/'db.sqlite3'
        db = DB(path)
        db.add_subscription('3060', None, None, [], 'gpu', ['kazan','moskva'])
        monitor = Monitor(cfg, db, source, AsyncMock())
        await monitor.poll_once()
        until = db.source_states('local_browser')[0]['next_allowed_at']
        budget = db.get('next_poll:local_browser')
        db.close()
        cfg2, source2 = setup(tmp_path, 'patchright', 'page_navigation' if mode=='light_fetch' else 'light_fetch', raw)
        db = DB(path)
        restarted = Monitor(cfg2, db, source2, AsyncMock())
        assert not await restarted.poll_once()
        restarted.set_paused(False)
        if code != 429:
            # Resume explicitly clears indefinite waiting; don't do it before asserting persistence.
            assert db.get('next_poll:local_browser') == budget
        else:
            assert db.source_states('local_browser')[0]['next_allowed_at'] == until
            assert 'ещё не истекла' in await restarted.open_verification()
            assert not await restarted.poll_once()
        source2.page.goto.assert_not_awaited()
        source2.page.evaluate.assert_not_awaited()
        db.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('mode', ['light_fetch','page_navigation'])
def test_manual_verification_and_resume_share_restrictions_and_metrics(tmp_path, mode):
    async def scenario():
        raw = dict(status=200, challenge=True, items=[])
        cfg, source = setup(tmp_path, 'patchright', mode, raw)
        db = DB(tmp_path/'db.sqlite3')
        db.add_subscription('3060',None,None,[],'gpu',['kazan'])
        mon = Monitor(cfg,db,source,AsyncMock())
        await mon.poll_once()
        assert db.source_states(source.source_id)[0]['state'] == SourceState.WAITING_FOR_USER
        # A server-provided Retry-After remains binding even for a manual captcha check.
        with db.conn:
            db.conn.execute('UPDATE source_state SET next_allowed_at=0')
        before = source.page.evaluate.await_count
        assert not await mon.poll_once()
        assert source.page.evaluate.await_count == before
        raw.update(challenge=False, catalog=True, empty=True)
        assert 'обычная выдача подтверждена' in await mon.open_verification()
        assert not mon.paused  # an Avito restriction is not the user's pause
        assert db.conn.execute("SELECT operation FROM fetch_metrics ORDER BY id DESC LIMIT 1").fetchone()[0] == 'manual'
        assert db.source_states(source.source_id)[0]['state'] == SourceState.ACTIVE
        mon.set_paused(False)
        db.set('next_poll:local_browser','0')
        assert await mon.poll_once()
        report = comparison_report(db.conn)
        assert sum(g['configuration_totals']['checks'] for g in report['groups']) == 2
        group = next(g for g in report['groups'] if g['configuration_totals']['checks'])
        assert group['configuration_totals']['restrictions'] == 1 and group['configuration_totals']['success_share'] == .5
        assert group['windows'][0]['seconds_to_first_restriction'] >= 0
        assert not group['windows'][0]['censored']
        db.close()
    asyncio.run(scenario())


def test_additive_metrics_migration_preserves_legacy_rows_and_states(tmp_path):
    path = tmp_path/'legacy.sqlite3'
    with sqlite3.connect(path) as conn:
        conn.executescript('''
          CREATE TABLE fetch_metrics(id INTEGER PRIMARY KEY,source_id TEXT,city TEXT,fetched_at REAL,
            status TEXT,http_status INTEGER,raw_count INTEGER,matched_count INTEGER,overlap_count INTEGER,
            interval_sec REAL,cooldown_until REAL,item_ids_json TEXT);
          INSERT INTO fetch_metrics VALUES(3,'local_browser','kazan',100,'rate_limited',429,0,0,NULL,NULL,999,'[]');
          CREATE TABLE source_state(source_id TEXT,city TEXT,state TEXT,next_allowed_at REAL,reason TEXT,
            updated_at REAL,PRIMARY KEY(source_id,city));
          INSERT INTO source_state VALUES('local_browser','kazan','WAITING_FOR_USER',999,'HTTP 429',100);
        ''')
    for _ in range(2):
        db = DB(path)
        row = db.conn.execute('SELECT id,http_status,cooldown_until,browser_engine,fetch_mode,request_count FROM fetch_metrics').fetchone()
        assert row == (3,429,999,None,None,None)
        assert db.source_states('local_browser')[0]['state'] == 'WAITING_FOR_USER'
        report = comparison_report(db.conn)
        assert report['unattributed_checks'] == 1 and len(report['groups']) == 4
        assert all(g['configuration_totals']['success_share'] is None and g['configuration_totals']['checks']==0 for g in report['groups'])
        db.close()


def test_comparison_counts_only_automatic_samples_and_selected_window(tmp_path):
    db = DB(tmp_path/'db.sqlite3')
    for engine in ('playwright','patchright'):
        for mode in ('light_fetch','page_navigation'):
            for stamp,status,operation in [(100,'ok','automatic'),(110,'rate_limited','automatic'),(120,'ok','manual')]:
                db.record_fetch(source_id='local_browser',city='kazan',fetched_at=stamp,status=status,http_status=200,
                                raw_count=2,matched_count=1,overlap_count=None,interval_sec=None,cooldown_until=None,
                                item_ids_json='[1,2]',browser_engine=engine,fetch_mode=mode,run_id=engine+mode,
                                browser_channel='chrome',budget_rpm=1,comparison_context='same',duration_sec=2,
                                request_count=3,transferred_bytes=100,operation=operation,
                                actual_mode=mode,is_warmup=(stamp==100))
    report = comparison_report(db.conn)
    for group in report['groups']:
        assert group['configuration_totals']['checks']==2 and group['configuration_totals']['success_share']==.5 and group['configuration_totals']['unique_listings']==2
        assert group['configuration_totals']['requests']['total']==6 and group['configuration_totals']['transferred_bytes']['total']==200
        assert group['configuration_totals']['duration_sec']=={'n':2,'p50':2,'p95':2}
        assert group['windows'][0]['seconds_to_first_restriction']==12
    filtered = comparison_report(db.conn, since=110, until=120)
    assert all(g['configuration_totals']['checks']==1 and g['configuration_totals']['success_share']==0 for g in filtered['groups'])
    db.close()


def test_reports_cli_is_read_only_and_exports_attribution(tmp_path, monkeypatch, capsys):
    from avito_bot import metrics
    path = tmp_path/'test database.sqlite3'
    db = DB(path)
    db.close()
    original = path.read_bytes()
    monkeypatch.setattr('sys.argv', ['avito-metrics', '--db', str(path), '--compare'])
    metrics.main()
    assert len(json.loads(capsys.readouterr().out)['groups']) == 4
    monkeypatch.setattr('sys.argv', ['avito-metrics', '--db', str(path), '--events'])
    metrics.main()
    assert 'browser_engine,fetch_mode,run_id' in capsys.readouterr().out
    monkeypatch.setattr('sys.argv', ['avito-metrics', '--db', str(path)])
    metrics.main()
    assert json.loads(capsys.readouterr().out) == []
    assert path.read_bytes() == original


def record_sample(db, stamp, **overrides):
    values = dict(source_id='local_browser',city='kazan',fetched_at=stamp,status='ok',http_status=200,
                  raw_count=1,matched_count=0,overlap_count=None,interval_sec=None,cooldown_until=None,
                  item_ids_json='[123]',browser_engine='playwright',browser_channel='chrome',
                  fetch_mode='light_fetch',actual_mode='light_fetch',run_id='run',comparison_context='context',
                  budget_rpm=1,duration_sec=1,request_count=1,transferred_bytes=10,is_warmup=False)
    values.update(overrides)
    db.record_fetch(**values)


def test_warmup_and_unknown_phase_excluded_from_mechanisms_but_included_in_load(tmp_path):
    db = DB(tmp_path/'db.sqlite3')
    record_sample(db,100,is_warmup=True,actual_mode='page_navigation',duration_sec=40,request_count=40)
    record_sample(db,110)
    record_sample(db,120,operation='manual',actual_mode='page_navigation',duration_sec=99,request_count=50,
                  status='rate_limited',http_status=429)
    record_sample(db,130,is_warmup=None,actual_mode='page_navigation',duration_sec=300,request_count=300)
    # Navigation after failed/invalid initial loading is ordinary navigation, even in a light configuration.
    record_sample(db,140,actual_mode='page_navigation',duration_sec=2,request_count=2)
    report = comparison_report(db.conn)
    assert report['schema_version']==2
    group = report['groups'][0]
    assert group['configuration_totals']['checks']==4
    assert group['warmup']['checks']==1 and group['warmup']['duration_sec']['p50']==40
    assert group['unknown_phase']['checks']==1
    assert group['regular_by_actual_mode']['light_fetch']['duration_sec']['p50']==1
    assert group['regular_by_actual_mode']['page_navigation']['duration_sec']['p50']==2
    light, nav = report['mechanisms'][:2]
    assert light['totals']['requests']['total']==1 and nav['totals']['requests']['total']==2
    assert light['totals']['restrictions']==nav['totals']['restrictions']==0
    assert report['total_load']['requests']['total']==393
    assert report['automatic_load']['requests']['total']==343
    assert report['manual_load']['requests']['total']==50
    db.close()


def test_previous_browser_schema_readable_before_and_after_additive_migration(tmp_path):
    from avito_bot import metrics
    path = tmp_path/'previous.sqlite3'
    db = DB(path)
    record_sample(db,100,is_warmup=None,actual_mode='page_navigation')
    with db.conn:
        db.conn.execute('ALTER TABLE fetch_metrics DROP COLUMN is_warmup')
        db.conn.execute('ALTER TABLE fetch_metrics DROP COLUMN restriction_reason')
    original = db.conn.execute('SELECT * FROM fetch_metrics').fetchone()
    before = comparison_report(db.conn)
    assert before['groups'][0]['unknown_phase']['checks']==1
    assert all(m['totals']['checks']==0 for m in before['mechanisms'])
    db.close()
    for _ in range(2):
        db = DB(path)
        row = db.conn.execute('SELECT * FROM fetch_metrics').fetchone()
        assert row[:-2]==original and row[-2:]==(None,None)
        report = metrics.comparison_report(db.conn)
        assert report['groups'][0]['configuration_totals']['checks']==1
        assert report['groups'][0]['warmup']['checks']==0
        assert all(m['totals']['checks']==0 for m in report['mechanisms'])
        db.close()


def test_time_to_restriction_not_rebased_by_export_range_or_missing_duration(tmp_path):
    db = DB(tmp_path/'db.sqlite3')
    record_sample(db,100,is_warmup=True,duration_sec=10,actual_mode='page_navigation')
    record_sample(db,200,status='rate_limited',http_status=429)
    record_sample(db,300)
    full = comparison_report(db.conn)
    clipped = comparison_report(db.conn,since=250,until=400)
    assert full['groups'][0]['windows'][0]['seconds_to_first_restriction']==110
    win = clipped['groups'][0]['windows'][0]
    assert win['seconds_to_first_restriction']==110 and win['first_restriction_at']==200
    assert win['selected_checks']['checks']==1
    record_sample(db,500,run_id='unknown-duration',duration_sec=None,status='blocked',http_status=403)
    win = comparison_report(db.conn,since=450)['groups'][0]['windows'][0]
    assert win['started_at'] is None and win['seconds_to_first_restriction'] is None
    assert win['first_restriction_at']==500 and not win['censored']
    db.close()


def test_cohorts_and_contiguous_contexts_keep_different_workloads_separate(tmp_path):
    db = DB(tmp_path/'db.sqlite3')
    record_sample(db,100,is_warmup=True,actual_mode='page_navigation')
    record_sample(db,110)
    record_sample(db,120,comparison_context='other-network',budget_rpm=.5,city='moskva',browser_channel='chromium')
    record_sample(db,130)
    report = comparison_report(db.conn)
    assert len(report['groups'][0]['windows'])==3  # A -> B -> A is not one contiguous sample.
    cohorts = report['mechanisms'][0]['cohorts']
    assert len(cohorts)==2
    assert {(c['budget_rpm'],c['browser_channel'],c['city']) for c in cohorts} == {(1,'chrome','kazan'),(.5,'chromium','moskva')}
    assert all(c['context_complete'] for c in cohorts)
    db.close()


def test_unavailable_or_invalid_measurements_are_not_zeroed(tmp_path):
    db = DB(tmp_path/'db.sqlite3')
    record_sample(db,100,duration_sec=None,request_count=None,transferred_bytes=None)
    report = comparison_report(db.conn)
    stats = report['mechanisms'][0]['totals']
    assert stats['duration_sec'] is None
    assert stats['requests']==stats['transferred_bytes']=={'measured_checks':0,'total':None}
    record_sample(db,110,duration_sec=0,request_count=0,transferred_bytes=0)
    stats = comparison_report(db.conn)['mechanisms'][0]['totals']
    assert stats['duration_sec']=={'n':1,'p50':0,'p95':0}
    assert stats['requests']==stats['transferred_bytes']=={'measured_checks':1,'total':0}
    record_sample(db,120,duration_sec=-1,request_count=-1,transferred_bytes=-1)
    stats = comparison_report(db.conn)['mechanisms'][0]['totals']
    assert stats['duration_sec']['n']==1 and stats['requests']['measured_checks']==1
    db.close()


def test_mechanism_latency_excludes_warmup_and_manual_detection_and_keeps_origin(tmp_path):
    db = DB(tmp_path/'db.sqlite3')
    for item_id, warmup, operation, delta in [(1,True,'automatic',100),(2,False,'automatic',2),(3,False,'manual',200)]:
        collection = dict(browser_engine='playwright',fetch_mode='light_fetch',actual_mode='page_navigation',
                          is_warmup=warmup,operation=operation)
        payload = dict(item={'city':'kazan','published':99},collection=collection,subs=[])
        db.stage_items([(item_id,payload,[1])],100)
        job = db.claim_delivery(100)
        db.ack_delivery(job['id'],100+delta)
    report = comparison_report(db.conn)
    group = report['groups'][0]
    # Whole configuration retains automatic warm-up deliveries.
    assert group['detection_to_telegram_sec']['n']==2
    nav = report['mechanisms'][1]
    assert nav['actual_mode']=='page_navigation' and nav['detection_to_telegram_sec']=={'n':1,'p50':2,'p95':2}
    assert report['mechanisms'][0]['detection_to_telegram_sec'] is None
    db.close()


def test_invalid_delivery_clock_is_not_a_zero_latency_sample(tmp_path):
    db = DB(tmp_path/'db.sqlite3')
    payload = dict(item={'city':'kazan','published':50},subs=[],
                   collection=dict(browser_engine='playwright',fetch_mode='light_fetch',actual_mode='light_fetch',is_warmup=False))
    db.stage_items([(1,payload,[1])],100)
    job = db.claim_delivery(100)
    db.ack_delivery(job['id'],90)  # A clock correction is not a zero-second delivery.
    report = comparison_report(db.conn)
    assert report['groups'][0]['detection_to_telegram_sec'] is None
    assert report['groups'][0]['invalid_delivery_timestamps']==1
    assert report['mechanisms'][0]['detection_to_telegram_sec'] is None
    db.close()
