import asyncio
import json
import sqlite3
import time
from dataclasses import asdict
from unittest.mock import AsyncMock

import pytest
from aiogram.exceptions import (TelegramBadRequest, TelegramForbiddenError, TelegramNetworkError,
                                TelegramRetryAfter, TelegramServerError)
from aiogram.methods import SendMessage

from avito_bot.bot import TelegramBot
from avito_bot.db import DB, Subscription
from avito_bot.delivery import DeliveryWorker
from avito_bot.matcher import Item


def payload(i=10):
    return {'item': asdict(Item(i, 'RTX 3060', 20000, 'https://www.avito.ru/kazan/x/x_10',
                               'только что', None, '', '', published=time.time()-10, found_at=time.time())),
            'subs': [asdict(Subscription(1, '3060', None, None, []))], 'source_id': 'test'}


@pytest.fixture
def db(tmp_path):
    d = DB(tmp_path/'test.sqlite3')
    yield d
    d.close()


def test_seen_and_jobs_roll_back_together(db):
    with pytest.raises(TypeError):
        db.stage_items([(10, payload(), [1]), (11, {'unserializable': object()}, [1])], time.time())
    assert db.unseen([10, 11]) == {10, 11}
    assert db.queue_counts() == {}
    assert db.conn.execute('SELECT count(*) FROM items').fetchone()[0] == 0


def test_unique_per_recipient_and_payload_persists(db):
    p = payload()
    assert db.stage_items([(10, p, [1, 1, 2])], time.time()) == [10]
    assert db.stage_items([(10, p, [1, 2])], time.time()) == []
    assert db.queue_counts() == {'pending': 2}
    assert json.loads(db.claim_delivery(time.time())['payload_json']) == p
    assert db.found_today() == 0


@pytest.mark.parametrize('exc', [
    TelegramServerError(method=SendMessage(chat_id=1, text='x'), message='500'),
    TelegramNetworkError(method=SendMessage(chat_id=1, text='x'), message='timeout'),
    TimeoutError(),
])
def test_transient_failure_does_not_lose_seen_event(db, exc):
    db.stage_items([(10, payload(), [1])], time.time())
    sender = AsyncMock(side_effect=exc)
    worker = DeliveryWorker(db, sender, lambda _: True)
    assert asyncio.run(worker.deliver_once())
    assert db.queue_counts() == {'retry': 1}
    assert db.unseen([10]) == set()
    assert db.found_today() == 0
    sender.side_effect = None
    with db.conn:
        db.conn.execute('UPDATE notification_outbox SET next_attempt_at=0')
    asyncio.run(worker.deliver_once())
    assert db.queue_counts() == {'sent': 1}
    assert db.found_today() == 1


def test_telegram_429_blocks_all_recipients_until_retry_after(db):
    db.stage_items([(10, payload(), [1, 2])], time.time())
    sender = AsyncMock(side_effect=TelegramRetryAfter(method=SendMessage(chat_id=1, text='x'),
                                                     message='rate limit', retry_after=60))
    worker = DeliveryWorker(db, sender, lambda _: True)
    asyncio.run(worker.deliver_once())
    assert db.queue_counts() == {'pending': 1, 'retry': 1}
    assert not asyncio.run(worker.deliver_once())
    assert sender.await_count == 1
    assert float(db.get('telegram_next_allowed_at')) > time.time()+50


@pytest.mark.parametrize('exc', [TelegramForbiddenError(method=SendMessage(chat_id=1,text='x'), message='blocked'),
                                 TelegramBadRequest(method=SendMessage(chat_id=1,text='x'), message='invalid')])
def test_permanent_failures_terminal_and_visible(db, exc):
    db.stage_items([(10, payload(), [1])], time.time())
    asyncio.run(DeliveryWorker(db, AsyncMock(side_effect=exc), lambda _: True).deliver_once())
    assert db.queue_counts() == {'terminal': 1}
    assert db.found_today() == 0


def test_per_recipient_ack_and_no_duplicate_stats(db):
    db.stage_items([(10, payload(), [1, 2])], time.time())
    sender = AsyncMock()
    worker = DeliveryWorker(db, sender, lambda _: True)
    asyncio.run(worker.deliver_once())
    assert db.queue_counts() == {'pending': 1, 'sent': 1}
    assert db.found_today() == 1 and db.found_for(1) == 1
    asyncio.run(worker.deliver_once())
    assert db.queue_counts() == {'sent': 2}
    assert db.found_for(1) == 1
    assert not asyncio.run(worker.deliver_once())


def test_crash_after_send_before_ack_reclaims_lease_after_restart(tmp_path):
    path = tmp_path/'x.sqlite3'
    db = DB(path)
    db.stage_items([(10, payload(), [1])], 100)
    claimed = db.claim_delivery(100, lease_seconds=10)
    assert claimed['attempts'] == 1
    db.close()
    db = DB(path)
    assert db.claim_delivery(105) is None
    retry = db.claim_delivery(111)
    assert retry['id'] == claimed['id'] and retry['attempts'] == 2
    db.ack_delivery(retry['id'], 112)
    db.close()
    db = DB(path)
    assert db.claim_delivery(1000) is None
    assert db.queue_counts() == {'sent': 1}
    db.close()


def test_revoked_access_never_sends(db):
    db.stage_items([(10, payload(), [1])], time.time())
    send = AsyncMock()
    asyncio.run(DeliveryWorker(db, send, lambda _: False).deliver_once())
    send.assert_not_awaited()
    assert db.queue_counts() == {'terminal': 1}


def test_cancel_releases_lease(db):
    db.stage_items([(10, payload(), [1])], time.time())
    worker = DeliveryWorker(db, AsyncMock(side_effect=asyncio.CancelledError), lambda _: True)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(worker.deliver_once())
    assert db.queue_counts() == {'retry': 1}


def test_photo_fallback_propagates_text_failure_and_429(db):
    async def scenario():
        bot = TelegramBot('123456:TEST-token', db, 1)
        bot.fetch_image = AsyncMock(return_value=b'photo')
        bot.bot.send_photo = AsyncMock(side_effect=TelegramBadRequest(
            method=SendMessage(chat_id=1,text='x'), message='invalid photo'))
        bot.bot.send_message = AsyncMock()
        i = Item(**payload()['item']); i.image = 'https://example.test/photo'
        await bot.send_to_chat(i, [Subscription(1,'3060',None,None,[])], 1)
        bot.bot.send_message.assert_awaited_once()
        bot.bot.send_message.side_effect = TimeoutError()
        with pytest.raises(TimeoutError):
            await bot.send_to_chat(i, [], 1)
        bot.bot.send_message.reset_mock()
        bot.bot.send_photo.side_effect = TelegramRetryAfter(method=SendMessage(chat_id=1,text='x'),
                                                           message='rate',retry_after=20)
        with pytest.raises(TelegramRetryAfter):
            await bot.send_to_chat(i, [], 1)
        bot.bot.send_message.assert_not_awaited()
        await bot.bot.session.close()
    asyncio.run(scenario())


def test_legacy_migration_preserves_every_table(tmp_path):
    path = tmp_path/'old.sqlite3'
    conn = sqlite3.connect(path)
    conn.executescript('''
      CREATE TABLE subscriptions(id INTEGER PRIMARY KEY,query TEXT,min_price INTEGER,max_price INTEGER,minus_words TEXT);
      INSERT INTO subscriptions VALUES(7,'3060',100,20000,'майн');
      CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT); INSERT INTO settings VALUES('owner_id','123');
      CREATE TABLE seen(item_id INTEGER PRIMARY KEY,first_seen REAL); INSERT INTO seen VALUES(20,1);
      CREATE TABLE members(user_id INTEGER PRIMARY KEY,name TEXT,added_at REAL); INSERT INTO members VALUES(2,'friend',1);
      CREATE TABLE invites(code TEXT PRIMARY KEY,created_at REAL); INSERT INTO invites VALUES('test',1);
      CREATE TABLE notifications(item_id INTEGER,sub_id INTEGER,sent_at REAL); INSERT INTO notifications VALUES(20,7,1);
    ''')
    conn.close()
    db = DB(path)
    assert db.subscription(7).query == '3060'
    assert db.get('owner_id') == '123'
    assert db.members() == [(2,'friend')]
    assert db.unseen([20]) == set()
    assert db.found_for(7) == 1
    assert db.conn.execute('SELECT code FROM invites').fetchone()[0] == 'test'
    db.close()
    db = DB(path)
    assert db.subscription(7).min_price == 100
    assert db.conn.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
    db.close()
