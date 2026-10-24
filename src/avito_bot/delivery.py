"""Durable per-recipient delivery, independent of the browser and poll scheduler."""
import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable

from aiogram.exceptions import (
    TelegramBadRequest, TelegramForbiddenError, TelegramNotFound, TelegramRetryAfter,
)

from .db import DB, Subscription
from .matcher import Item

log = logging.getLogger(__name__)


class DeliveryWorker:
    def __init__(self, db: DB, send: Callable[[Item, list[Subscription], int], Awaitable[None]],
                 allowed: Callable[[int], bool]):
        self.db, self.send, self.allowed = db, send, allowed

    async def deliver_once(self, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        global_retry = float(self.db.get('telegram_next_allowed_at') or 0)
        if now < global_retry:
            return False
        job = self.db.claim_delivery(now)
        if not job:
            return False
        try:
            if not self.allowed(job['chat_id']):
                self.db.fail_delivery(job['id'], 'Recipient access revoked', now, terminal=True)
                return True
            payload = json.loads(job['payload_json'])
            item = Item(**payload['item'])
            subs = [Subscription(**s) for s in payload['subs']]
            await self.send(item, subs, job['chat_id'])
        except asyncio.CancelledError:
            self.db.fail_delivery(job['id'], 'Interrupted before acknowledgement', time.time())
            raise
        except TelegramRetryAfter as exc:
            next_at = time.time() + max(1, exc.retry_after)
            self.db.set('telegram_next_allowed_at', str(next_at))
            self.db.fail_delivery(job['id'], 'TelegramRetryAfter', next_at)
        except (TelegramForbiddenError, TelegramNotFound, TelegramBadRequest, ValueError, TypeError, KeyError) as exc:
            self.db.fail_delivery(job['id'], type(exc).__name__, time.time(), terminal=True)
            log.error('Terminal delivery failure job=%s type=%s', job['id'], type(exc).__name__)
        except Exception as exc:
            delay = min(900, 2 ** min(job['attempts'], 9))
            self.db.fail_delivery(job['id'], type(exc).__name__, time.time() + delay)
            log.warning('Delivery retry job=%s delay=%ss type=%s', job['id'], delay, type(exc).__name__)
        else:
            self.db.ack_delivery(job['id'], time.time())
            log.info('Delivered item=%s job=%s source=%s city=%s published_at=%s first_seen_at=%s delivered_at=%s',
                     item.id, job['id'], payload.get('source_id'), item.city, item.published,
                     item.found_at, time.time())
        return True

    async def run(self) -> None:
        while True:
            if not await self.deliver_once():
                await asyncio.sleep(.5)
