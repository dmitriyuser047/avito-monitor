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
        assert group['checks'] == 2 and group['success_share'] == 1
        assert group['unique_listings'] == 2 and group['listing_observations'] == 3
        assert group['detection_to_telegram_sec']['n'] == 2
        assert group['publication_to_telegram_sec']['n'] == 2
        assert group['requests'] == {'measured_checks':0,'total':None}
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
        assert 'Страница открыта' in await mon.open_verification()
        assert mon.paused
        assert db.conn.execute("SELECT operation FROM fetch_metrics ORDER BY id DESC LIMIT 1").fetchone()[0] == 'manual'
        assert db.source_states(source.source_id)[0]['state'] == SourceState.ACTIVE
        mon.set_paused(False)
        db.set('next_poll:local_browser','0')
        assert await mon.poll_once()
        report = comparison_report(db.conn)
        assert sum(g['checks'] for g in report['groups']) == 2
        group = next(g for g in report['groups'] if g['checks'])
        assert group['restrictions'] == 1 and group['success_share'] == .5
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
        assert all(g['success_share'] is None and g['checks']==0 for g in report['groups'])
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
                                request_count=3,transferred_bytes=100,operation=operation)
    report = comparison_report(db.conn)
    for group in report['groups']:
        assert group['checks']==2 and group['success_share']==.5 and group['unique_listings']==2
        assert group['requests']['total']==6 and group['transferred_bytes']['total']==200
        assert group['duration_sec']=={'n':2,'p50':2,'p95':2}
        assert group['windows'][0]['seconds_to_first_restriction']==12
    filtered = comparison_report(db.conn, since=110, until=120)
    assert all(g['checks']==1 and g['success_share']==0 for g in filtered['groups'])
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
