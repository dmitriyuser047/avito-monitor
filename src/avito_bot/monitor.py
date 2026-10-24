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

from .config import Config, feed_url
from .db import DB, Subscription
from .matcher import Item, classify, is_fresh, matches
from .scraper import Scraper, Status

log = logging.getLogger(__name__)

BLOCK_RETRY_MINUTES = [15, 30, 60]
MIN_RPM = 1  # ниже бюджет после блокировок не опускаем
PASS_MINUTES = 15  # капча даёт пропуск ~20 мин; блок позже этого — истёк пропуск, а не частота
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
        self._unblocked_at: float | None = None

    # --- бюджет загрузок ---

    def budget(self) -> float:
        """Сколько загрузок ленты в минуту можно делать сейчас (ночью — меньше)."""
        start, end = self.cfg.night_hours
        if start <= datetime.now().hour < end:
            return self.cfg.night_requests_per_minute
        learned = self.db.get("rpm")  # снижен после блокировок
        return min(self.cfg.requests_per_minute, float(learned)) if learned else self.cfg.requests_per_minute

    def delay(self) -> float:
        base = 60 / max(self.budget(), 0.1)
        return base * random.uniform(0.8, 1.2)

    def city_period(self) -> float:
        """Раз в сколько секунд проверяется каждый город."""
        return len(self.cities()) * 60 / max(self.budget(), 0.1)

    def _slow_down(self) -> float | None:
        current = float(self.db.get("rpm") or self.cfg.requests_per_minute)
        lowered = max(MIN_RPM, current - 1)
        if lowered < current:
            self.db.set("rpm", f"{lowered:g}")
            return lowered
        return None

    # --- опрос ---

    async def run(self) -> None:
        turn = 0
        while True:
            if self.paused:
                self.status = "пауза"
                await asyncio.sleep(2)
                continue
            cities = self.cities()
            city = cities[turn % len(cities)]
            turn += 1
            url = feed_url(city, self.cfg.feed)
            started = time.monotonic()
            status, items = await self.scraper.fetch(url)
            for item in items:
                item.city = city
            if status is Status.BLOCKED:
                await self._wait_unblock(url)
                continue
            items = [i for i in items if i.feed in self.cfg.sections]  # только комплектующие и ПК
            await self._handle(status, items, url)
            self.last_cycle_sec = time.monotonic() - started
            await asyncio.sleep(self.delay())

    def cities(self) -> list[str]:
        """Города активных запросов (у запроса без городов — город из настроек)."""
        subs = self.db.subscriptions(only_enabled=True)
        cities = {c for s in subs for c in (s.cities or [self.cfg.city])}
        return sorted(cities, key=lambda c: (c != self.cfg.city, c)) or [self.cfg.city]

    async def _handle(self, status: Status, items: list[Item], url: str) -> None:
        if status is Status.OK:
            self.status = "работает"
            self.last_ok = time.time()
            self._empty = self._errors = 0
            self.last_items = items
            self._dump(url, items)
            if self.db.get(f"feed:{url}") is None:
                # новая лента (например, запрос в новом городе): то, что уже висит, не присылаем
                self.db.mark_seen([i.id for i in items])
                self.db.set(f"feed:{url}", "1")
                log.info("Новая лента %s: %d объявлений отмечены как просмотренные", url, len(items))
                return
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
        self._log_items([i for i in items if i.id in new_ids])  # до found_at: в логе есть seen_at
        subs = self.db.subscriptions(only_enabled=True)
        now = time.time()
        for item in items:
            city_subs = [s for s in subs if item.city in (s.cities or [self.cfg.city])]
            if item.id not in new_ids:
                continue
            item.found_at = now
            if not is_fresh(item):
                log.info("Пропуск (не свежее): %s | %s | %s", item.id, item.date, item.title)
                continue
            matched = [s for s in city_subs if matches(item, s)]
            published = datetime.fromtimestamp(item.published).strftime("%H:%M") if item.published else "?"
            log.info("Новое: %s (опубл. %s) | %s | %s ₽ | совпадений: %d",
                     item.date, published, item.title, item.price, len(matched))
            if matched:
                await self.send_item(item, matched)
                self.db.record_notification(item.id, [s.id for s in matched])

    def _dump(self, url: str, items: list[Item]) -> None:
        """Последняя загруженная лента — для диагностики."""
        path = self.cfg.data_dir / "feeds" / (url.split("/", 3)[-1].split("?")[0].replace("/", "_") + ".json")
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
        # блок через 15+ минут после капчи — истёк пропуск, частота тут ни при чём
        pass_expired = self._unblocked_at and time.time() - self._unblocked_at > PASS_MINUTES * 60
        lowered = None if pass_expired else self._slow_down()
        await self.scraper.set_window("normal")
        await self.alert(
            "Авито ограничил доступ\n"
            "На компьютере открылось окно браузера: нажмите в нём «Продолжить» и решите капчу — "
            "слежение продолжится само.\n\n"
            + (f"Чтобы это повторялось реже, бот снизил частоту до {lowered:g} загрузок в минуту.\n"
               if lowered else "")
            + f"Если ничего не делать, бот попробует снова через {BLOCK_RETRY_MINUTES[0]} мин."
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
                self._unblocked_at = time.time()
                await self.alert("Доступ восстановлен\nСлежение продолжается.")
                city = url.split("/")[3]
                for item in items:
                    item.city = city
                await self._handle(status, [i for i in items if i.feed in self.cfg.sections], url)
                return
