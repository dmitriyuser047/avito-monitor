"""Stage 5: price history, search links, history screens and the daily report."""
import asyncio
import time
from datetime import datetime
from unittest.mock import AsyncMock

from avito_bot import ui
from avito_bot.config import load_config
from avito_bot.db import DB
from avito_bot.delivery import DeliveryWorker
from avito_bot.matcher import Item
from avito_bot.monitor import Monitor
from avito_bot.search_link import parse_avito_link
from avito_bot.sources import FetchResult, FetchStatus
from avito_bot.sources.mock import MockListingsSource


def listing(item_id, price):
    return Item(item_id, 'Видеокарта RTX 3060', price, f'https://www.avito.ru/kazan/tovary_dlya_kompyutera/x_{item_id}',
                'только что', None, '', '', feed='tovary_dlya_kompyutera', city='kazan', published=time.time())


def ok(*items):
    return FetchResult(FetchStatus.OK, list(items), 'mock', 'kazan', time.time(), 200)


def env(tmp_path):
    cfg = load_config(); cfg.data_dir = tmp_path; cfg.city = 'kazan'; cfg.night_hours = (0, 0)
    cfg.sections = ['tovary_dlya_kompyutera']; cfg.daily_report_hour = -1
    db = DB(tmp_path/'x.sqlite3')
    db.add_subscription('3060', None, None, [], 'gpu', ['kazan'])
    return cfg, db


def test_price_drop_is_notified_once_with_history(tmp_path):
    async def scenario():
        cfg, db = env(tmp_path)
        source = MockListingsSource()
        mon = Monitor(cfg, db, source, AsyncMock(), recipients=lambda: [1, 2])
        for price in (18000, 18000, 17500, 15000, 15000):  # baseline, same, −3% (ignored), −14%, same
            await mon.ingest(ok(listing(100, price)))
        assert db.queue_counts() == {'pending': 2}
        assert [p for p, _ in db.price_history(100)] == [15000, 17500, 18000]
        sends = []
        async def send(item, subs, chat_id, note=None):
            sends.append((item.id, chat_id, note))
        worker = DeliveryWorker(db, send, lambda _: True)
        assert await worker.deliver_once() and await worker.deliver_once()
        assert {s[1] for s in sends} == {1, 2} and all(s[0] == 100 for s in sends)
        assert sends[0][2] == 'Цена снижена: 17 500 → 15 000 ₽ (−14%)'
        assert db.found_today() == 0  # a price change is not a new listing
        await mon.ingest(ok(listing(100, 16000)))  # a rise is recorded, never notified
        assert db.queue_counts() == {'sent': 2}
        db.close()
    asyncio.run(scenario())


def test_price_drop_can_be_disabled_and_ignores_unmatched(tmp_path):
    async def scenario():
        cfg, db = env(tmp_path)
        cfg.price_drop_percent = 0
        mon = Monitor(cfg, db, MockListingsSource(), AsyncMock(), recipients=lambda: [1])
        await mon.ingest(ok(listing(1, 20000)))
        await mon.ingest(ok(listing(1, 10000)))
        assert db.queue_counts() == {}
        cfg.price_drop_percent = 10
        other = Item(2, 'Монитор Samsung', 9000, 'https://www.avito.ru/kazan/tovary_dlya_kompyutera/m_2',
                     '', None, '', '', feed='tovary_dlya_kompyutera', city='kazan')
        await mon.ingest(ok(other))
        await mon.ingest(ok(Item(**{**other.__dict__, 'price': 5000})))
        assert db.queue_counts() == {}
        db.close()
    asyncio.run(scenario())


def test_search_link_parsing():
    link, problem = parse_avito_link(
        'https://www.avito.ru/kazan/tovary_dlya_kompyutera/komplektuyuschie/videokarty?q=rtx+3060&pmin=20000&pmax=10000')
    assert problem is None
    assert link == dict(query='rtx 3060', pmin=10000, pmax=20000, city='kazan',
                        section='tovary_dlya_kompyutera/komplektuyuschie/videokarty')
    for bad, reason in [('https://example.com/kazan?q=x', 'не ссылка'),
                        ('https://www.avito.ru/kazan/telefony', 'нет текста поиска'),
                        ('https://www.avito.ru/rossiya?q=rtx', 'всей России'),
                        ('ftp://avito.ru/kazan?q=x', 'не ссылка')]:
        assert parse_avito_link(bad)[0] is None and reason in parse_avito_link(bad)[1]
    note = ui.link_note(link, 'bytovaya_elektronika', 3, True)
    assert 'отдельных запросов по ссылке не будет' in note and 'подходит объявлений: 3' in note


def test_recent_and_error_screens(tmp_path):
    db = DB(tmp_path/'x.sqlite3')
    from dataclasses import asdict
    item = listing(7, 15000)
    db.stage_items([(7, {'item': asdict(item), 'subs': [{'id': 1}]}, [1]),
                    (8, {'item': asdict(listing(8, 1)), 'subs': []}, [])], time.time())
    for price in (18000, 15000):
        db.record_price(7, price, time.time())
    text = ui.recent_text([(i, d, s, db.price_history(i, 3)) for i, d, s in db.recent_items(10)])
    assert 'RTX 3060' in text and '15 000 ₽ (было 18 000)' in text and text.count('<a href') == 1
    db.record_event('source_rate_limited', city='kazan', http_status=429)
    db.record_event('source_recovered', city='kazan', recovery_sec=1200)
    db.record_event('source_checked', result='ok')
    errors = ui.errors_text(db.recent_events(ui.ERROR_EVENTS))
    assert 'восстановлено · kazan (длилось 20 мин)' in errors and 'HTTP 429 · kazan (HTTP 429)' in errors
    assert 'source_checked' not in errors
    assert 'не было' in ui.errors_text([])
    db.close()


def test_daily_report_is_sent_once_per_day(tmp_path):
    async def scenario():
        cfg, db = env(tmp_path)
        cfg.daily_report_hour = datetime.now().hour
        alert = AsyncMock()
        mon = Monitor(cfg, db, MockListingsSource([ok(listing(1, 100))]), alert)
        db.record_event('service_started', at=time.time() - 3600)
        await mon.poll_once()
        db.set('next_poll:mock', '0')
        await mon.poll_once()
        reports = [c.args[0] for c in alert.await_args_list if c.args[0].startswith('Ежедневный отчёт')]
        assert len(reports) == 1
        assert 'Источник: проверок' in reports[0] and 'не доказывает работу сбора' in reports[0]
        db.close()
    asyncio.run(scenario())
