"""Reproducible local benchmark of the scheduler, staging and delivery; never touches the network.

uv run python scripts/benchmark.py [--polls 300] [--page 50] [--new 2]
"""
import argparse
import asyncio
import math
import tempfile
import time
from pathlib import Path
from unittest.mock import AsyncMock

from avito_bot.config import load_config
from avito_bot.db import DB
from avito_bot.delivery import DeliveryWorker
from avito_bot.matcher import Item
from avito_bot.monitor import Monitor
from avito_bot.sources import FetchResult, FetchStatus
from avito_bot.sources.mock import MockListingsSource


def page(start: int, size: int) -> FetchResult:
    items = [Item(i, f'Видеокарта RTX 3060 #{i}', 15000 + i % 7, f'https://www.avito.ru/kazan/tovary_dlya_kompyutera/x_{i}',
                  'только что', None, '', '', feed='tovary_dlya_kompyutera', city='kazan', published=time.time())
             for i in range(start, start + size)]
    return FetchResult(FetchStatus.OK, items, 'mock', 'kazan', time.time(), 200)


def p95(values):
    values = sorted(values)
    return values[math.ceil(len(values) * .95) - 1]


async def main(polls: int, size: int, new: int, recipients: int) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        cfg = load_config(); cfg.data_dir = Path(tmp); cfg.city = 'kazan'; cfg.night_hours = (0, 0)
        cfg.sections = ['tovary_dlya_kompyutera']; cfg.daily_report_hour = -1
        db = DB(Path(tmp)/'bench.sqlite3')
        for query in ('3060', 'rtx', 'видеокарта'):
            db.add_subscription(query, None, None, [], 'gpu', ['kazan'])
        source = MockListingsSource([page(new * n, size) for n in range(polls)])
        mon = Monitor(cfg, db, source, AsyncMock(), recipients=lambda: list(range(1, recipients + 1)))
        durations = []
        for _ in range(polls):
            db.set('next_poll:mock', '0')
            started = time.perf_counter()
            assert await mon.poll_once()
            durations.append(time.perf_counter() - started)
        events = db.conn.execute('SELECT COUNT(*) FROM events').fetchone()[0]
        jobs = sum(db.queue_counts().values())
        worker = DeliveryWorker(db, AsyncMock(), lambda _: True)
        started = time.perf_counter()
        while await worker.deliver_once():
            pass
        delivery = time.perf_counter() - started
        size_kb = (Path(tmp)/'bench.sqlite3').stat().st_size / 1024
        print(f'polls={polls} page={size} new_per_poll={new} subscriptions=3 recipients={recipients}')
        print(f'poll processing: mean={sum(durations)/len(durations)*1000:.1f} ms, p95={p95(durations)*1000:.1f} ms')
        print(f'source checks={len(source.checks)} (one per poll for all subscriptions)')
        print(f'events={events} ({events/polls:.1f} per poll), delivery jobs={jobs}')
        print(f'delivery: {jobs/delivery:.0f} jobs/s without network; database {size_kb:.0f} KB')
        db.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--polls', type=int, default=300)
    parser.add_argument('--page', type=int, default=50)
    parser.add_argument('--new', type=int, default=2)
    parser.add_argument('--recipients', type=int, default=3)
    args = parser.parse_args()
    asyncio.run(main(args.polls, args.page, args.new, args.recipients))
