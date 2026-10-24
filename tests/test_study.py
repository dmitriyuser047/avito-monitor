import asyncio
import json
from dataclasses import asdict
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from avito_bot.config import load_config
from avito_bot.db import DB
from avito_bot.matcher import Item
from avito_bot.monitor import Monitor
from avito_bot.sources import FetchResult, FetchStatus, SourceState
from avito_bot.study import (StudyRecorder, begin_study, distribution, make_report, render_markdown,
                              write_report)


class Source:
    source_id='local_browser'
    closed=False
    def __init__(self):self.calls=0
    async def fetch(self,url):
        self.calls+=1
        return FetchResult(FetchStatus.RATE_LIMITED,[],self.source_id,'kazan',100,429)


@pytest.fixture
def setup(tmp_path):
    cfg=load_config();cfg.data_dir=tmp_path;cfg.night_hours=(0,0)
    db=DB(tmp_path/'x.sqlite3')
    db.add_subscription('3060',None,20000,[],'gpu',['kazan'])
    mon=Monitor(cfg,db,Source(),AsyncMock(),recipients=lambda:[1])
    yield cfg,db,mon
    db.close()


def test_passive_recording_never_requests_and_survives_restart(setup):
    cfg,db,mon=setup
    study_id=begin_study(db,cfg,hours=24,now=100)
    mon.study.tick(mon,now=100)
    mon.study.tick(mon,now=101)
    assert db.conn.execute('SELECT count(*) FROM study_samples').fetchone()[0]==1
    mon.set_paused(True)
    StudyRecorder(db,cfg.data_dir).tick(mon,now=102)
    assert db.conn.execute('SELECT state FROM study_samples ORDER BY sampled_at').fetchall()==[('ACTIVE',),('PAUSED',)]
    assert mon.scraper.calls==0
    assert make_report(db.conn,study_id,now=103)['status']=='running'
    with pytest.raises(ValueError,match='already running'):
        begin_study(db,cfg,hours=24,now=103)


def test_long_shutdown_is_unobserved_not_active_time(setup):
    cfg,db,mon=setup
    study_id=begin_study(db,cfg,hours=1,now=100)
    mon.study.tick(mon,now=100)
    report=make_report(db.conn,study_id,now=3700)
    assert report['sampled_state_seconds']['ACTIVE']==120
    assert report['sampled_state_seconds']['UNOBSERVED']==3480
    assert report['observer_coverage']<.04
    assert any('без наблюдателя' in s for s in report['warnings'])
    assert report['discovery_to_telegram']['n']==0
    assert report['successful_fetches']==0
    assert any('Нет успешных' in s for s in report['warnings'])


def test_end_exports_once_and_restores_after_reboot(setup):
    cfg,db,mon=setup
    study_id=begin_study(db,cfg,hours=1,now=100)
    mon.study.tick(mon,now=100)
    recorder=StudyRecorder(db,cfg.data_dir)
    recorder.tick(mon,now=3700)
    directory=cfg.data_dir/'studies'/str(study_id)
    assert (directory/'report.json').exists() and (directory/'report.md').exists()
    assert db.conn.execute('SELECT status FROM studies').fetchone()[0]=='complete'
    before=(directory/'report.json').read_bytes()
    recorder.tick(mon,now=4000)
    assert (directory/'report.json').read_bytes()==before
    assert mon.scraper.calls==0


def test_changed_filters_are_visible(setup):
    cfg,db,mon=setup
    study_id=begin_study(db,cfg,now=100)
    db.update_subscription(1,max_price=30000)
    mon.study.tick(mon,now=101)
    report=make_report(db.conn,study_id,now=102)
    assert report['sampled_state_seconds']['FILTERS_CHANGED']==1
    assert any('изменились фильтры' in s for s in report['warnings'])


def test_restrictions_have_no_hidden_requests_and_stop_after_second_429(setup):
    async def scenario():
        cfg,db,mon=setup
        begin_study(db,cfg,now=100)
        await mon.poll_once()
        assert mon.scraper.calls==1
        assert db.source_states('local_browser')[0]['state']==SourceState.COOLDOWN
        with db.conn:db.conn.execute('UPDATE source_state SET next_allowed_at=0')
        db.set('next_poll:local_browser','0')
        await mon.poll_once()
        assert mon.scraper.calls==2
        assert db.source_states('local_browser')[0]['state']==SourceState.WAITING_FOR_USER
        with db.conn:db.conn.execute('UPDATE source_state SET next_allowed_at=0')
        db.set('next_poll:local_browser','0')
        restarted=Monitor(cfg,db,mon.scraper,AsyncMock())
        for _ in range(3):assert not await restarted.poll_once()
        assert mon.scraper.calls==2
        restarted.set_paused(False)
        assert await restarted.poll_once()
        assert mon.scraper.calls==3
    asyncio.run(scenario())


def test_existing_cooldown_migration_counts_prior_429(setup):
    async def scenario():
        cfg,db,mon=setup
        db.record_fetch(source_id='local_browser',city='kazan',fetched_at=1,status='rate_limited',
                        http_status=429,raw_count=0,matched_count=0,overlap_count=None,interval_sec=None,
                        cooldown_until=0,item_ids_json='[]')
        await mon.poll_once()
        assert db.source_states('local_browser')[0]['state']==SourceState.WAITING_FOR_USER
        assert db.get('rate_limit_streak:local_browser')=='2'
    asyncio.run(scenario())


def test_study_failure_cannot_stop_the_source(setup,monkeypatch):
    async def scenario():
        cfg,db,mon=setup
        monkeypatch.setattr(mon.study,'tick',lambda *args:(_ for _ in ()).throw(OSError('disk full')))
        assert await mon.poll_once()
        assert mon.scraper.calls==1 and not mon._source_stopped
    asyncio.run(scenario())


def test_report_separates_baseline_and_new_listing_latency(setup):
    cfg,db,mon=setup
    study_id=begin_study(db,cfg,hours=1,now=100)
    with db.conn:
        db.conn.execute('INSERT INTO source_observations VALUES (?,?,?,?,?,?,?,?)',
                        ('local_browser','kazan',10,0,110,110,1,1))
        db.conn.execute('INSERT INTO source_observations VALUES (?,?,?,?,?,?,?,?)',
                        ('local_browser','kazan',11,105,120,120,1,0))
    for item_id,seen,pub in [(10,110,0),(11,120,105)]:
        payload={'item':asdict(Item(item_id,'RTX 3060',10000,'https://www.avito.ru/kazan/x/x_'+str(item_id),
                                    '',None,'','',city='kazan',published=pub,found_at=seen)),
                 'subs':[asdict(db.subscription(1))],'source_id':'local_browser'}
        db.stage_items([(item_id,payload,[1])],seen)
        job=db.claim_delivery(seen)
        db.ack_delivery(job['id'],seen+5)
    report=make_report(db.conn,study_id,now=200)
    assert report['observed_unique_ids']==2 and report['baseline_unique_ids']==1
    assert report['new_unique_ids']==1 and report['delivered_unique_ids']==1
    assert report['publish_to_discovery']['p50_sec']==15
    assert report['discovery_to_telegram']['p50_sec']==5
    assert report['publish_to_telegram']['p50_sec']==20
    assert report['discovery_to_telegram']['n']==1
    assert report['discovery_to_telegram']['share_within_60_sec']==1
    assert 'Публикация → Telegram' in render_markdown(report)


def test_observations_record_even_when_seen_already_exists(setup):
    async def scenario():
        cfg,db,mon=setup
        item=Item(10,'Видеокарта RTX 3060',10000,'https://www.avito.ru/kazan/tovary_dlya_kompyutera/x_10',
                  'только что',None,'','',feed='tovary_dlya_kompyutera',published=80)
        db.mark_seen([10])
        result=FetchResult(FetchStatus.OK,[item],'local_browser','kazan',100,200)
        await mon._handle(result,cfg.feed_url)
        assert db.conn.execute('SELECT item_id,baseline FROM source_observations').fetchall()==[(10,1)]
        assert db.queue_counts()=={}
    asyncio.run(scenario())


@pytest.mark.parametrize('hours',[0,-1,float('nan'),float('inf')])
def test_invalid_study_window(setup,hours):
    cfg,db,mon=setup
    with pytest.raises(ValueError):begin_study(db,cfg,hours=hours,now=100)


def test_distribution_does_not_invent_values_when_empty():
    assert distribution([])=={'n':0,'p50_sec':None,'p95_sec':None,'share_within_60_sec':None}
    assert distribution([5,10,20])['p95_sec']==20


def test_empty_baseline_does_not_hide_next_real_new_items(setup):
    async def scenario():
        cfg,db,mon=setup
        await mon._handle(FetchResult(FetchStatus.NO_RESULTS,[],'local_browser','kazan',100,200),cfg.feed_url)
        item=Item(20,'Видеокарта RTX 3060',10000,'https://www.avito.ru/kazan/tovary_dlya_kompyutera/x_20',
                  'только что',None,'','',feed='tovary_dlya_kompyutera',published=105)
        await mon._handle(FetchResult(FetchStatus.OK,[item],'local_browser','kazan',110,200),cfg.feed_url)
        assert db.conn.execute('SELECT baseline FROM source_observations WHERE item_id=20').fetchone()[0]==0
    asyncio.run(scenario())


def test_pause_and_cooldown_are_measured_without_resuming(setup):
    cfg,db,mon=setup
    study_id=begin_study(db,cfg,now=100)
    db.save_source_state('local_browser','kazan',SourceState.COOLDOWN,1000,'HTTP 429')
    mon.study.tick(mon,now=100)
    mon.study.tick(mon,now=160)
    mon.set_paused(True)
    mon.study.tick(mon,now=170)
    report=make_report(db.conn,study_id,now=200)
    assert report['sampled_state_seconds']['COOLDOWN']==70
    assert report['sampled_state_seconds']['PAUSED']==30
    assert mon.scraper.calls==0
    assert 'Наблюдение №' in mon.health_text()


def test_report_export_failure_is_retryable_and_never_marks_complete(setup,monkeypatch):
    cfg,db,mon=setup
    study_id=begin_study(db,cfg,hours=1,now=100)
    import avito_bot.study as study_module
    monkeypatch.setattr(study_module,'write_report',lambda *args:(_ for _ in ()).throw(OSError('disk full')))
    with pytest.raises(OSError):mon.study.tick(mon,now=3700)
    assert db.conn.execute('SELECT status FROM studies WHERE id=?',(study_id,)).fetchone()[0]=='running'


def test_redeploy_promotes_old_repeated_cooldown_without_extra_request(setup):
    async def scenario():
        cfg,db,mon=setup
        for stamp in (1,2):
            db.record_fetch(source_id='local_browser',city='kazan',fetched_at=stamp,status='rate_limited',
                            http_status=429,raw_count=0,matched_count=0,overlap_count=None,interval_sec=None,
                            cooldown_until=0,item_ids_json='[]')
        db.save_source_state('local_browser','kazan',SourceState.COOLDOWN,0,'HTTP 429')
        restarted=Monitor(cfg,db,mon.scraper,AsyncMock())
        assert not await restarted.poll_once()
        assert mon.scraper.calls==0
        assert db.source_states('local_browser')[0]['state']==SourceState.WAITING_FOR_USER
        assert restarted.alert.await_count==1
    asyncio.run(scenario())
