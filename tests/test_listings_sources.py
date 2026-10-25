"""Stage 3: replaceable sources share one scheduler, staging and restriction policy."""
import asyncio
import time
from unittest.mock import AsyncMock

from avito_bot.config import load_config
from avito_bot.db import DB
from avito_bot.matcher import Item
from avito_bot.monitor import Monitor
from avito_bot.scraper import Scraper
from avito_bot.sources import FetchResult, FetchStatus, ListingsSource, Search, SourceState
from avito_bot.sources.mock import MockListingsSource


def listing(item_id):
    return Item(item_id, 'Видеокарта RTX 3060', 10000, f'https://www.avito.ru/kazan/x/x_{item_id}',
                'только что', None, '', '', feed='tovary_dlya_kompyutera', city='kazan', published=time.time())


def env(tmp_path):
    cfg = load_config(); cfg.data_dir = tmp_path; cfg.city = 'kazan'; cfg.night_hours = (0, 0)
    db = DB(tmp_path/'x.sqlite3')
    db.add_subscription('3060', None, None, [], 'gpu', ['kazan'])
    return cfg, db


def ok(*ids):
    return FetchResult(FetchStatus.OK, [listing(i) for i in ids], 'mock', 'kazan', time.time(), 200)


def test_browser_and_mock_implement_the_listings_contract(tmp_path):
    for source in (Scraper(tmp_path/'profile', None), MockListingsSource()):
        assert all(callable(getattr(source, name)) for name in ('check', 'health', 'close'))
        assert isinstance(source.source_id, str)
    assert ListingsSource  # protocol is importable for new adapters


def test_scheduler_checks_one_search_per_city_for_all_subscriptions(tmp_path):
    async def scenario():
        cfg, db = env(tmp_path)
        db.add_subscription('rtx', None, None, [], 'gpu', ['kazan'])
        source = MockListingsSource([ok(1), ok(1, 2)])
        mon = Monitor(cfg, db, source, AsyncMock(), recipients=lambda: [1, 2])
        assert await mon.poll_once()
        db.set('next_poll:mock', '0')
        assert await mon.poll_once()
        assert source.checks == [Search('kazan', cfg.feed, f'https://www.avito.ru/kazan/{cfg.feed}?s=104')] * 2
        assert db.queue_counts() == {'pending': 2}
        health = await source.health()
        assert health.started and health.detail == {'queued': 0}
        db.close()
    asyncio.run(scenario())


def test_pushed_events_use_the_same_staging_and_restrictions(tmp_path):
    async def scenario():
        cfg, db = env(tmp_path)
        source = MockListingsSource()
        mon = Monitor(cfg, db, source, AsyncMock(), recipients=lambda: [1])
        await mon.ingest(ok(1))  # baseline
        await mon.ingest(ok(1, 2))
        assert db.queue_counts() == {'pending': 1}
        await mon.ingest(FetchResult(FetchStatus.RATE_LIMITED, [], 'mock', 'kazan', time.time(), 429, 600))
        assert db.source_states('mock')[0]['state'] == SourceState.RATE_LIMITED
        assert not await mon.poll_once() and source.checks == []  # the pushed 429 binds polling too
        db.close()
    asyncio.run(scenario())
