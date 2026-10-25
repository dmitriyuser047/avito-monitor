"""Stage 2: the process, the scheduler and delivery recover by themselves."""
import asyncio
import logging
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from avito_bot.config import load_config
from avito_bot.db import DB
from avito_bot.delivery import DeliveryWorker
from avito_bot.monitor import Monitor
from avito_bot.sources import DeliveryState, FetchResult, FetchStatus, ServiceState
from avito_bot.watchdog import ComponentHung, HUNG_EXIT_CODE, Watchdog


def test_watchdog_reports_only_stale_components():
    beats = {'source': 100.0, 'delivery': 50.0}
    dog = Watchdog(60, {name: (lambda n=name: beats[n]) for name in beats})
    assert dog.stale(now=120) == ['delivery']
    beats['delivery'] = 119
    assert dog.stale(now=120) == []


def test_watchdog_raises_for_hung_loop():
    async def scenario():
        dog = Watchdog(0.01, {'source': lambda: time.monotonic() - 1})
        with pytest.raises(ComponentHung, match='source'):
            await asyncio.wait_for(dog.run(interval=0.01), 1)
    asyncio.run(scenario())


def test_hung_component_exits_with_restartable_code(tmp_path, monkeypatch):
    import avito_bot
    cfg = load_config(); cfg.data_dir = tmp_path
    monkeypatch.setattr(avito_bot, 'load_config', lambda: cfg)
    async def hung():
        raise ComponentHung(['delivery'])
    monkeypatch.setattr(avito_bot, 'run', hung)
    monkeypatch.setattr(logging, 'basicConfig', lambda **kw: None)
    with pytest.raises(SystemExit) as exit_info:
        avito_bot.main()
    assert exit_info.value.code == HUNG_EXIT_CODE
    from avito_bot.supervisor import restart_delay
    assert restart_delay(HUNG_EXIT_CODE, [], 0) is not None


class Source:
    source_id = 'local_browser'
    closed = False
    async def fetch(self, url):
        return FetchResult(FetchStatus.NO_RESULTS, [], self.source_id, 'kazan', time.time(), 200)


def test_scheduler_errors_back_off_and_never_stop_collection(tmp_path, monkeypatch):
    async def scenario():
        import avito_bot.monitor as monitor_module
        monkeypatch.setattr(monitor_module, 'SCHEDULER_RETRY_BASE', 0.001)
        cfg = load_config(); cfg.data_dir = tmp_path; cfg.night_hours = (0, 0)
        db = DB(tmp_path/'x.sqlite3')
        db.add_subscription('3060', None, None, [], 'gpu', ['kazan'])
        alert = AsyncMock()
        mon = Monitor(cfg, db, Source(), alert)
        calls = []
        real = mon.poll_once
        async def flaky():
            calls.append(1)
            if len(calls) <= monitor_module.FAILED_AFTER:
                raise OSError('database is locked')
            await real()
            raise asyncio.CancelledError
        mon.poll_once = flaky
        with pytest.raises(asyncio.CancelledError):
            await mon.run()
        assert len(calls) == monitor_module.FAILED_AFTER + 1  # it kept going
        assert alert.await_count == 1 and 'Сбор пытается восстановиться сам' in alert.await_args.args[0]
        assert db.conn.execute("SELECT count(*) FROM events WHERE kind='source_failed'").fetchone()[0] == 5
        assert mon.service_state == ServiceState.FAILED  # last loop raised before it could report success
        db.close()
    asyncio.run(scenario())


def test_delivery_states_and_loop_survives_storage_errors(tmp_path):
    async def scenario():
        db = DB(tmp_path/'x.sqlite3')
        worker = DeliveryWorker(db, AsyncMock(), lambda _: True)
        assert worker.state == DeliveryState.RUNNING
        for _ in range(9):
            worker._transient_failure()
        assert worker.state == DeliveryState.RETRYING
        worker._transient_failure()
        assert worker.state == DeliveryState.FAILED
        calls = []
        async def broken(now=None):
            calls.append(1)
            if len(calls) == 1:
                raise OSError('disk I/O error')
            raise asyncio.CancelledError
        worker.deliver_once = broken
        before = worker.heartbeat
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(worker.run(), 5)
        assert len(calls) == 2 and worker.heartbeat > before
        db.close()
    asyncio.run(scenario())


def test_delivery_success_records_events_and_returns_to_running(tmp_path):
    async def scenario():
        from dataclasses import asdict
        from avito_bot.matcher import Item
        db = DB(tmp_path/'x.sqlite3')
        item = Item(5, 'RTX 3060', 1000, 'https://www.avito.ru/kazan/x/x_5', '', None, '', '', city='kazan')
        db.stage_items([(5, {'item': asdict(item), 'subs': [], 'source_id': 'local_browser',
                             'correlation_id': 'abc'}, [1])], time.time())
        worker = DeliveryWorker(db, AsyncMock(), lambda _: True)
        worker.state = DeliveryState.RETRYING
        assert await worker.deliver_once()
        assert worker.state == DeliveryState.RUNNING
        kinds = db.conn.execute("SELECT kind,correlation_id FROM events ORDER BY id").fetchall()
        assert kinds == [('telegram_delivery_started', 'abc'), ('telegram_delivery_confirmed', 'abc')]
        db.close()
    asyncio.run(scenario())
