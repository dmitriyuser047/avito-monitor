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
from .sources import DeliveryState

log = logging.getLogger(__name__)

FAILED_AFTER = 10  # consecutive transient failures; retries continue, the state tells the owner


class DeliveryWorker:
    def __init__(self, db: DB, send: Callable[[Item, list[Subscription], int], Awaitable[None]],
                 allowed: Callable[[int], bool]):
        self.db, self.send, self.allowed = db, send, allowed
        self.state = DeliveryState.RUNNING
        self.heartbeat = time.monotonic()
        self.failures = 0

    def _transient_failure(self) -> None:
        self.failures += 1
        self.state = DeliveryState.FAILED if self.failures >= FAILED_AFTER else DeliveryState.RETRYING

    async def deliver_once(self, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        global_retry = float(self.db.get('telegram_next_allowed_at') or 0)
        if now < global_retry:
            self.state = DeliveryState.RETRYING
            return False
        job = self.db.claim_delivery(now)
        if not job:
            if self.state == DeliveryState.RETRYING and not self.failures:
                self.state = DeliveryState.RUNNING
            return False
        correlation_id = None
        try:
            if not self.allowed(job['chat_id']):
                self.db.fail_delivery(job['id'], 'Recipient access revoked', now, terminal=True)
                return True
            payload = json.loads(job['payload_json'])
            correlation_id = payload.get('correlation_id')
            self.db.record_event('telegram_delivery_started', correlation_id=correlation_id,
                                 source_id=payload.get('source_id'), result=str(job['item_id']),
                                 job_id=job['id'], attempt=job['attempts'])
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
            self.state = DeliveryState.RETRYING
        except (TelegramForbiddenError, TelegramNotFound, TelegramBadRequest, ValueError, TypeError, KeyError) as exc:
            self.db.fail_delivery(job['id'], type(exc).__name__, time.time(), terminal=True)
            log.error('Terminal delivery failure job=%s type=%s', job['id'], type(exc).__name__)
        except Exception as exc:
            delay = min(900, 2 ** min(job['attempts'], 9))
            self.db.fail_delivery(job['id'], type(exc).__name__, time.time() + delay)
            self._transient_failure()
            log.warning('Delivery retry job=%s delay=%ss type=%s', job['id'], delay, type(exc).__name__)
        else:
            self.db.ack_delivery(job['id'], time.time())
            self.failures = 0
            self.state = DeliveryState.RUNNING
            self.db.record_event('telegram_delivery_confirmed', correlation_id=correlation_id,
                                 source_id=payload.get('source_id'), result=str(item.id), job_id=job['id'])
            log.info('Delivered item=%s job=%s source=%s city=%s published_at=%s first_seen_at=%s delivered_at=%s',
                     item.id, job['id'], payload.get('source_id'), item.city, item.published,
                     item.found_at, time.time())
        return True

    async def run(self) -> None:
        errors = 0
        while True:
            self.heartbeat = time.monotonic()
            try:
                delivered = await self.deliver_once()
                errors = 0
            except asyncio.CancelledError:
                raise
            except Exception:
                # E.g. SQLite busy or I/O error: keep the worker alive and retry with a bounded pause.
                errors += 1
                self._transient_failure()
                log.exception('Delivery loop error #%s; retrying', errors)
                await asyncio.sleep(min(60, 2 ** min(errors, 6)))
                continue
            if not delivered:
                await asyncio.sleep(.5)
