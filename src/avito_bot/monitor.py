"""Цикл опроса лент: новые объявления → проверка подписок → уведомление.

Блокировки: окно браузера разворачивается, владелец получает сообщение и
проходит проверку руками; бот ждёт, а потом сам пробует снова с растущей паузой.
"""

import asyncio
import json
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from datetime import datetime

from .config import Config
from .db import DB, Subscription
from .matcher import Item, classify, is_fresh, matches
from .scraper import Scraper, Status

log = logging.getLogger(__name__)

BLOCK_RETRY_MINUTES = [15, 30, 60]
EMPTY_ALERT_AFTER = 3
ERROR_ALERT_AFTER = 5


class Monitor:
    def __init__(
        self,
        cfg: Config,
        db: DB,
        scraper: Scraper,
        alert: Callable[[str], Awaitable[None]],
        send_item: Callable[[Item, list[Subscription]], Awaitable[None]],
    ):
        self.cfg = cfg
        self.db = db
        self.scraper = scraper
        self.alert = alert
        self.send_item = send_item
        self.paused = db.get("paused") == "1"
        self.status = "запуск"
        self.last_ok: float | None = None
        self.last_cycle_sec: float | None = None
        self._empty = 0
        self._errors = 0
        self.last_items: list[Item] = []  # для /test

    def interval(self) -> float:
        start, end = self.cfg.night_hours
        hour = datetime.now().hour
        base = self.cfg.night_interval if start <= hour < end else self.cfg.poll_interval
        return max(3.0, base + random.uniform(-self.cfg.poll_jitter, self.cfg.poll_jitter))

    async def run(self) -> None:
        while True:
            if self.paused:
                self.status = "пауза"
                await asyncio.sleep(2)
                continue
            started = time.monotonic()
            for feed, url in zip(self.cfg.feeds, self.cfg.feed_urls):
                status, items = await self.scraper.fetch(url)
                for item in items:
                    item.feed = feed
                if status is Status.BLOCKED:
                    await self._wait_unblock(url)
                    break
                await self._handle(status, items, url)
                await asyncio.sleep(random.uniform(0.5, 1.5))
            self.last_cycle_sec = time.monotonic() - started
            await asyncio.sleep(self.interval())

    async def _handle(self, status: Status, items: list[Item], url: str) -> None:
        if status is Status.OK:
            self.status = "работает"
            self.last_ok = time.time()
            self._empty = self._errors = 0
            self.last_items = items
            self._dump(url, items)
            await self._process(items)
        elif status is Status.EMPTY:
            self._empty += 1
            if self._empty == EMPTY_ALERT_AFTER:
                await self.alert(
                    "Лента пустая\n"
                    f"{EMPTY_ALERT_AFTER} раза подряд на странице не нашлось объявлений:\n{url}\n\n"
                    "Скорее всего, Авито поменял вёрстку и парсер нужно поправить."
                )
        else:
            self._errors += 1
            if self._errors == ERROR_ALERT_AFTER:
                await self.alert(f"Нет связи с Авито\n{ERROR_ALERT_AFTER} ошибок загрузки подряд. Проверьте интернет на Mac.")

    async def _process(self, items: list[Item]) -> None:
        new_ids = self.db.unseen([i.id for i in items])
        if not new_ids:
            return
        self.db.mark_seen(list(new_ids))
        self._log_items([i for i in items if i.id in new_ids])
        subs = self.db.subscriptions(only_enabled=True)
        for item in items:
            if item.id not in new_ids:
                continue
            if not is_fresh(item):
                log.info("Пропуск (не свежее): %s | %s | %s", item.id, item.date, item.title)
                continue
            matched = [s for s in subs if matches(item, s)]
            log.info("Новое: %s | %s | %s ₽ | совпадений: %d", item.date, item.title, item.price, len(matched))
            if matched:
                await self.send_item(item, matched)
                self.db.record_notification(item.id, [s.id for s in matched])

    def _dump(self, url: str, items: list[Item]) -> None:
        """Последняя загруженная лента — для диагностики."""
        path = self.cfg.data_dir / "feeds" / (url.split("/", 4)[-1].split("?")[0].replace("/", "_") + ".json")
        path.parent.mkdir(exist_ok=True)
        path.write_text(json.dumps([asdict(i) for i in items], ensure_ascii=False, indent=1), encoding="utf-8")

    def _log_items(self, items: list[Item]) -> None:
        """Все новые объявления — в data/items.jsonl, чтобы улучшать поиск на реальных данных."""
        with open(self.cfg.data_dir / "items.jsonl", "a", encoding="utf-8") as f:
            for i in items:
                f.write(json.dumps({**asdict(i), "seen_at": time.time(), "categories": sorted(classify(i))},
                                   ensure_ascii=False) + "\n")

    def set_paused(self, paused: bool) -> None:
        self.paused = paused
        self.db.set("paused", "1" if paused else "0")

    async def _wait_unblock(self, url: str) -> None:
        self.status = "заблокирован Авито"
        log.warning("Авито ограничил доступ")
        await self.scraper.set_window("normal")
        await self.alert(
            "Авито ограничил доступ\n"
            "На компьютере открылось окно браузера: нажмите в нём «Продолжить» и решите капчу — "
            "слежение продолжится само.\n\n"
            f"Если ничего не делать, бот попробует снова через {BLOCK_RETRY_MINUTES[0]} мин."
        )
        attempt = 0
        next_retry = time.monotonic() + BLOCK_RETRY_MINUTES[0] * 60
        while True:
            await asyncio.sleep(5)
            status, items = await self.scraper.read_current()
            # не перезагружаем страницу, пока человек решает капчу
            if status is not Status.OK and time.monotonic() >= next_retry and not await self.scraper.captcha_open():
                status, items = await self.scraper.fetch(url)
                attempt += 1
                next_retry = time.monotonic() + BLOCK_RETRY_MINUTES[min(attempt, len(BLOCK_RETRY_MINUTES) - 1)] * 60
            if status is Status.OK:
                await self.scraper.set_window("minimized")
                self.status = "работает"
                await self.alert("Доступ восстановлен\nСлежение продолжается.")
                await self._handle(status, items, url)
                return
