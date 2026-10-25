import asyncio
import time
from dataclasses import asdict
from unittest.mock import AsyncMock

import pytest

from avito_bot.config import feed_url, load_config
from avito_bot.db import DB
from avito_bot.delivery import DeliveryWorker
from avito_bot.matcher import Item
from avito_bot.monitor import Monitor
from avito_bot.sources import FetchResult, FetchStatus, SourceState


class FakeSource:
    source_id='test'
    closed=False
    def __init__(self,status=FetchStatus.OK,items=None,retry=None):
        self.calls=[]
        self.status,self.items,self.retry=status,items or [],retry
    async def fetch(self,url):
        self.calls.append(url)
        return FetchResult(self.status,self.items,self.source_id,url.split('/')[3],time.time(),
                           429 if self.status is FetchStatus.RATE_LIMITED else 200,self.retry)


@pytest.fixture
def env(tmp_path):
    cfg=load_config(); cfg.data_dir=tmp_path; cfg.city='kazan';cfg.night_hours=(0,0);cfg.requests_per_minute=1
    db=DB(tmp_path/'x.sqlite3')
    db.add_subscription('3060',None,None,[],'gpu',['kazan','moskva'])
    yield cfg,db
    db.close()


@pytest.mark.parametrize('status',[FetchStatus.BLOCKED,FetchStatus.CHALLENGE,FetchStatus.RATE_LIMITED])
def test_restrictions_stop_all_cities_and_survive_restart(env,status):
    async def scenario():
        cfg,db=env; source=FakeSource(status,retry=3600)
        alert=AsyncMock(); monitor=Monitor(cfg,db,source,alert)
        assert await monitor.poll_once()
        db.set('next_poll:test','0')
        for _ in range(3):
            assert not await monitor.poll_once()
        restarted=Monitor(cfg,db,source,alert)
        assert not await restarted.poll_once()
        # The restart reports the saved restriction once and never as a new answer from Avito.
        assert len(source.calls)==1 and alert.await_count==2
        assert alert.await_args.args[0].startswith('Сохранённое ограничение с прошлого запуска')
        assert 'сохранено с прошлого запуска' in alert.await_args.args[0]
        assert not await restarted.poll_once() and alert.await_count==2
        assert db.source_states('test')[0]['next_allowed_at']>time.time()+3500
    asyncio.run(scenario())


def test_cooldown_retries_only_after_allowed_time(env):
    async def scenario():
        cfg,db=env;source=FakeSource(FetchStatus.RATE_LIMITED,retry=900)
        monitor=Monitor(cfg,db,source,AsyncMock())
        await monitor.poll_once()
        assert not await monitor.poll_once()
        with db.conn:
            db.conn.execute('UPDATE source_state SET next_allowed_at=0')
        db.set('next_poll:test','0')
        source.status=FetchStatus.NO_RESULTS
        assert await monitor.poll_once()
        assert len(source.calls)==2
    asyncio.run(scenario())


def test_block_is_never_lifted_by_blind_requests(env):
    async def scenario():
        cfg,db=env;source=FakeSource(FetchStatus.BLOCKED)
        monitor=Monitor(cfg,db,source,AsyncMock())
        await monitor.poll_once()
        db.set('next_poll:test','0')
        source.status=FetchStatus.NO_RESULTS
        # This source cannot show a loaded page, so nothing can confirm access: no requests at all.
        for _ in range(5):
            assert not await monitor.poll_once()
        assert db.source_states('test')[0]['state']==SourceState.ACCESS_DENIED
        assert len(source.calls)==1
    asyncio.run(scenario())


def test_closed_browser_is_restarted_without_requests_or_lost_restrictions(env):
    async def scenario():
        cfg,db=env;source=FakeSource(FetchStatus.RATE_LIMITED,retry=3600);alert=AsyncMock()
        started=[]
        async def start_source():
            started.append(1); source.closed=False
        monitor=Monitor(cfg,db,source,alert,start_source=start_source)
        source.stop=AsyncMock()
        assert await monitor.poll_once()
        source.closed=True
        assert not await monitor.poll_once()
        assert 'RESTARTING' in monitor.status and monitor.source_down=='browser_closed'
        monitor._restart_at=0
        assert not await monitor.poll_once()  # restarted, but the saved 429 term still binds
        assert started==[1] and monitor.source_down is None
        assert db.source_states('test')[0]['state']==SourceState.RATE_LIMITED
        assert len(source.calls)==1
    asyncio.run(scenario())


def test_no_subscriptions_or_disabled_means_no_requests(env):
    async def scenario():
        cfg,db=env;source=FakeSource();monitor=Monitor(cfg,db,source,AsyncMock())
        for sub in db.subscriptions():
            db.update_subscription(sub.id,enabled=False)
        assert not await monitor.poll_once()
        for sub in db.subscriptions():
            db.delete_subscription(sub.id)
        assert not await monitor.poll_once()
        assert source.calls==[]
    asyncio.run(scenario())


def test_global_budget_is_shared_and_persisted(env):
    async def scenario():
        cfg,db=env;source=FakeSource(FetchStatus.NO_RESULTS)
        monitor=Monitor(cfg,db,source,AsyncMock())
        assert await monitor.poll_once()
        assert not await monitor.poll_once()
        restarted=Monitor(cfg,db,source,AsyncMock())
        assert not await restarted.poll_once()
        assert len(source.calls)==1
        db.set('next_poll:test','0')
        assert await monitor.poll_once()
        assert '/moskva/' in source.calls[-1]
        assert monitor.last_interval_sec is not None
    asyncio.run(scenario())


def listing(item_id):
    return Item(item_id,'Видеокарта RTX 3060',10000,'https://www.avito.ru/kazan/x/x_'+str(item_id),
                'только что',None,'','',feed='tovary_dlya_kompyutera',city='kazan',published=time.time())


def test_monitor_queues_and_sender_runs_when_source_stopped(env):
    async def scenario():
        cfg,db=env; source=FakeSource(FetchStatus.OK,[listing(10)])
        monitor=Monitor(cfg,db,source,AsyncMock(),recipients=lambda:[1,2])
        await monitor.poll_once()  # seed old snapshot
        assert db.queue_counts()=={}
        source.items=[listing(10),listing(11)]
        db.set('next_poll:test','0')
        # Next city would be Moscow; force Kazan for the repeated snapshot.
        monitor._turn=0
        await monitor.poll_once()
        assert db.queue_counts()=={'pending':2}
        assert db.found_today()==0
        source.status=FetchStatus.BLOCKED
        db.set('next_poll:test','0')
        assert await monitor.poll_once() and not await monitor.poll_once()  # the source stops; delivery does not
        send=AsyncMock();worker=DeliveryWorker(db,send,lambda _:True)
        await worker.deliver_once();await worker.deliver_once()
        assert db.queue_counts()=={'sent':2}
        assert db.found_today()==1
        assert db.latency_stats()['delivery']['n']==2
        assert db.latency_stats()['publish']['n']==2
    asyncio.run(scenario())


def test_missing_overlap_warns_once_and_never_adds_requests(env):
    async def scenario():
        cfg,db=env;source=FakeSource(FetchStatus.OK,[listing(10)]);alert=AsyncMock()
        monitor=Monitor(cfg,db,source,alert)
        for item_id in [10,11,12]:
            source.items=[listing(item_id)]
            monitor._turn=0;db.set('next_poll:test','0')
            await monitor.poll_once()
        assert len(source.calls)==3 and alert.await_count==1
        assert 'Возможны пропуски' in alert.call_args.args[0]
        rows=db.conn.execute('SELECT overlap_count FROM fetch_metrics ORDER BY id').fetchall()
        assert rows==[(None,),(0,),(0,)]
    asyncio.run(scenario())


def test_parse_changed_diagnostics_and_backoff(env):
    async def scenario():
        cfg,db=env; source=FakeSource(FetchStatus.PARSE_CHANGED)
        monitor=Monitor(cfg,db,source,AsyncMock())
        await monitor.poll_once()
        assert db.source_states('test')[0]['state']==SourceState.PARSER_ERROR
        db.set('next_poll:test','0')
        assert not await monitor.poll_once()
        health=monitor.health_text()
        assert 'parse_changed' in health and 'PARSER_ERROR' in health and 'очередь' in health
        assert 'Сервис: RUNNING' in health and 'затем автоматическая проверка' in health
    asyncio.run(scenario())
