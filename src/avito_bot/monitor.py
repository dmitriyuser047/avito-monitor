"""Persistent source scheduler. Collection never waits for Telegram item delivery."""
import asyncio
import hashlib
import json
import logging
import time
from dataclasses import asdict
from datetime import datetime

from .config import Config, feed_url
from .db import DB
from .matcher import classify, is_fresh, matches
from .sources import FetchResult, FetchStatus, Source, SourceState
from .study import StudyRecorder

log = logging.getLogger(__name__)


class BrowserClosed(Exception):
    """Source stopped; Telegram and the outbox can keep working."""


class Monitor:
    def __init__(self, cfg: Config, db: DB, scraper: Source, alert, send_item=None, recipients=None):
        self.cfg, self.db, self.scraper, self.alert = cfg, db, scraper, alert
        self.source_id = getattr(scraper, 'source_id', 'local_browser')
        self.recipients = recipients or (lambda: [])
        self.paused = db.get('paused') == '1'
        self.status = 'запуск'
        previous_ok = db.conn.execute("SELECT MAX(fetched_at) FROM fetch_metrics WHERE source_id=?"
                                      " AND status IN ('ok','no_results')", (self.source_id,)).fetchone()[0]
        self.last_ok = previous_ok
        self.last_cycle_sec = None
        self.last_items = []
        self.last_interval_sec = None
        self._turn = 0
        self._last_started = None
        self._errors = {}
        self._source_stopped = False
        self._wake = asyncio.Event()
        self._source_lock = asyncio.Lock()
        self.study = StudyRecorder(db, cfg.data_dir)
        # Upgrade an already repeated cooldown from the previous release without
        # spending another request to discover the same restriction.
        self._restored_restriction = False
        if self._rate_limit_streak() >= 2:
            for state in db.source_states(self.source_id):
                if state['state'] == SourceState.COOLDOWN:
                    db.save_source_state(self.source_id, state['city'], SourceState.WAITING_FOR_USER,
                                         state['next_allowed_at'], 'repeated HTTP 429')
                    self._restored_restriction = True

    def _rate_limit_streak(self) -> int:
        saved = self.db.get(f'rate_limit_streak:{self.source_id}')
        if saved is not None:
            return int(saved)
        previous_ok_id = self.db.conn.execute(
            "SELECT COALESCE(MAX(id),0) FROM fetch_metrics WHERE source_id=? AND status IN ('ok','no_results')",
            (self.source_id,)).fetchone()[0]
        return self.db.conn.execute(
            "SELECT COUNT(*) FROM fetch_metrics WHERE source_id=? AND id>? AND status='rate_limited'",
            (self.source_id,previous_ok_id)).fetchone()[0]

    def budget(self) -> float:
        start, end = self.cfg.night_hours
        hour = datetime.now().hour
        night = start <= hour < end if start <= end else hour >= start or hour < end
        configured = self.cfg.night_requests_per_minute if night else self.cfg.requests_per_minute
        learned = self.db.get('rpm')
        return min(configured, float(learned)) if learned else configured

    def delay(self) -> float:
        return 60 / max(self.budget(), .01)

    def city_period(self) -> float:
        return len(self.cities()) * (self.delay() + (self.last_cycle_sec or 0))

    def _slow_down(self) -> float | None:
        """Compatibility for existing saved limits; not a recovery mechanism."""
        current = float(self.db.get('rpm') or self.cfg.requests_per_minute)
        lowered = max(1, current - 1)
        if lowered < current:
            self.db.set('rpm', f'{lowered:g}')
            return lowered
        return None

    def cities(self) -> list[str]:
        cities = {c for s in self.db.subscriptions(only_enabled=True) for c in (s.cities or [self.cfg.city])}
        return sorted(cities, key=lambda c: (c != self.cfg.city, c)) or [self.cfg.city]

    async def run(self) -> None:
        while True:
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception('Scheduler failure; collection stopped, Telegram remains available')
                self._source_stopped = True
                self.status = 'STOPPED: ошибка планировщика'
                self.db.save_source_state(self.source_id, '*', SourceState.STOPPED, reason='scheduler failure')
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), 1)
            except TimeoutError:
                pass

    async def poll_once(self) -> bool:
        async with self._source_lock:
            return await self._poll_once()

    async def _poll_once(self) -> bool:
        if self._restored_restriction:
            self._restored_restriction = False
            await self.alert('Повторные ограничения Авито\nОпрос остановлен по истории предыдущих попыток. '
                             'Проверьте /health. После обязательной паузы потребуется ручное возобновление.')
        try:
            self.study.tick(self)
        except Exception:
            log.exception('Passive study recording failed; source scheduling continues')
        if hasattr(self.scraper, 'cleanup_debug'):
            self.scraper.cleanup_debug()
        try:
            self._check_browser()
        except BrowserClosed:
            if not self._source_stopped:
                self._source_stopped = True
                self.status = 'STOPPED: браузер закрыт'
                self.db.save_source_state(self.source_id, '*', SourceState.STOPPED, reason='browser closed')
                await self.alert('Браузер закрыт\nСбор остановлен. Telegram и очередь доставки продолжают работать. '
                                 'Для возобновления запустите бот заново; автоматического перезапуска браузера нет.')
            return False
        if self._source_stopped:
            return False
        if self.paused:
            self.status = 'PAUSED: пауза'
            return False
        if not self.db.subscriptions(only_enabled=True):
            self.status = 'нет активных запросов'
            return False
        now = time.time()
        for state in self.db.source_states(self.source_id):
            if state['state'] == SourceState.WAITING_FOR_USER:
                self.status = 'WAITING_FOR_USER: требуется проверка Авито'
                return False
            if state['state'] in (SourceState.COOLDOWN, SourceState.DEGRADED):
                if (state['next_allowed_at'] or 0) > now:
                    self.status = state['state'] + ': ' + state['reason']
                    return False
        if float(self.db.get(f'next_poll:{self.source_id}') or 0) > now:
            return False
        cities = self.cities()
        city = cities[self._turn % len(cities)]
        self._turn += 1
        url = feed_url(city, self.cfg.feed)
        started = time.monotonic()
        if self._last_started:
            self.last_interval_sec = now - self._last_started
        self._last_started = now
        # Persist budget before I/O, so a crash/restart cannot immediately poll again.
        self.db.set(f'next_poll:{self.source_id}', str(now + self.delay()))
        result = await self.scraper.fetch(url)
        self.last_cycle_sec = time.monotonic() - started
        self.db.set(f'next_poll:{self.source_id}', str(time.time() + self.delay()))
        await self._handle(result, url)
        return True

    async def _handle(self, result: FetchResult, url: str, operation: str = 'automatic') -> None:
        status, city = result.status, result.city
        diagnostic_names = ('browser_engine', 'browser_channel', 'fetch_mode', 'actual_mode', 'run_id',
                            'duration_sec', 'request_count', 'transferred_bytes', 'access_limited',
                            'is_warmup', 'restriction_reason')
        diagnostics = {name: result.diagnostics.get(name) for name in diagnostic_names}
        context = dict(feed=self.cfg.feed, sections=self.cfg.sections,
                       subscriptions=[asdict(s) for s in self.db.subscriptions(only_enabled=True)],
                       city=self.cfg.city, rpm=self.cfg.requests_per_minute,
                       night_rpm=self.cfg.night_requests_per_minute, night_hours=self.cfg.night_hours,
                       cooldown=self.cfg.cooldown_seconds, headless=self.cfg.headless,
                       direct_interface=self.cfg.direct_interface)
        # A hash lets reports detect different filters/budgets without exporting query text.
        context_id = hashlib.sha256(json.dumps(context, sort_keys=True).encode()).hexdigest()[:16]
        collection = {name: diagnostics[name] for name in
                      ('browser_engine', 'browser_channel', 'fetch_mode', 'actual_mode', 'run_id', 'is_warmup')}
        collection.update(comparison_context=context_id, operation=operation)
        until = None
        matched = 0
        previous = self.db.latest_snapshot(self.source_id, city)
        ids = {i.id for i in result.items}
        overlap = len(ids & previous[0]) if previous else None
        if status in (FetchStatus.OK, FetchStatus.NO_RESULTS):
            self.status = 'ACTIVE: работает'
            self.last_ok = result.observed_at
            self._errors[city] = 0
            self.db.set(f'errors:{self.source_id}:{city}', '0')
            self.db.set(f'rate_limit_streak:{self.source_id}', '0')
            self.db.save_source_state(self.source_id, city, SourceState.ACTIVE)
            items = [i for i in result.items if i.feed in self.cfg.sections]
            for item in items:
                item.city = city
            subs = self.db.subscriptions(only_enabled=True)
            observation_key = f'observation_seed:{self.source_id}:{city}'
            baseline = self.db.get(observation_key) is None
            with self.db.conn:
                for item in items:
                    matched_now = is_fresh(item, result.observed_at) and any(
                        city in (s.cities or [self.cfg.city]) and matches(item, s) for s in subs)
                    self.db.conn.execute(
                        'INSERT INTO source_observations VALUES (?,?,?,?,?,?,?,?) '
                        'ON CONFLICT(source_id,city,item_id) DO UPDATE SET last_seen_at=excluded.last_seen_at',
                        (self.source_id, city, item.id, item.published, result.observed_at,
                         result.observed_at, int(matched_now), int(baseline)))
                self.db.conn.execute('INSERT OR IGNORE INTO settings VALUES (?,?)', (observation_key,'1'))
            self.last_items = items
            if self.db.get(f'feed:{url}') is None:
                # Seed existing listings; preserve the legacy baseline key.
                self.db.mark_seen([i.id for i in items])
                self.db.set(f'feed:{url}', '1')
            else:
                matched = self._process(items, result.observed_at, collection)
            if ids and previous and previous[0] and overlap == 0:
                key = f'coverage:{self.source_id}:{city}'
                if self.db.get(key) != 'gap':
                    await self.alert(f'Возможны пропуски в выдаче\nГород: {city}. '
                                     'У соседних снимков нет общих объявлений. Дополнительные запросы не выполняются.')
                self.db.set(key, 'gap')
            elif ids:
                self.db.set(f'coverage:{self.source_id}:{city}', 'ok')
        elif status is FetchStatus.RATE_LIMITED:
            until = time.time() + max(self.cfg.cooldown_seconds, result.retry_after_sec or 0)
            streak = self._rate_limit_streak() + 1
            self.db.set(f'rate_limit_streak:{self.source_id}', str(streak))
            state = SourceState.WAITING_FOR_USER if streak >= 2 else SourceState.COOLDOWN
            changed = self.db.save_source_state(self.source_id, city, state, until,
                                                'repeated HTTP 429' if streak >= 2 else 'HTTP 429')
            self.status = state + ': Авито ограничил частоту'
            if changed:
                await self.alert('Авито ограничил частоту\n'
                                 + ('Повторное ограничение: автоматический опрос полностью остановлен. '
                                    'После окончания паузы потребуется /unblock или /resume.\n' if streak >= 2 else '')
                                 + 'Пауза минимум до ' + datetime.fromtimestamp(until).strftime('%d.%m %H:%M')
                                 + '. Очередь Telegram работает.')
        elif status in (FetchStatus.BLOCKED, FetchStatus.CHALLENGE):
            until = time.time() + result.retry_after_sec if result.retry_after_sec else None
            changed = self.db.save_source_state(self.source_id, city, SourceState.WAITING_FOR_USER, until, status.value)
            self.status = 'WAITING_FOR_USER: Авито ограничил доступ'
            if changed:
                if hasattr(self.scraper, 'set_window'):
                    await self.scraper.set_window('normal')
                await self.alert('Авито ограничил доступ\nОпрос полностью остановлен и сам не возобновится. '
                                 'Команда /unblock откроет страницу проверки по вашему запросу. '
                                 'Пройдите проверку в браузере, затем /resume. Очередь Telegram работает.')
        else:
            self._errors[city] = int(self.db.get(f'errors:{self.source_id}:{city}') or 0) + 1
            self.db.set(f'errors:{self.source_id}:{city}', str(self._errors[city]))
            delay = min(3600, 60 * 2 ** min(self._errors[city] - 1, 6))
            if status is FetchStatus.PARSE_CHANGED:
                delay = max(900, delay)
            until = time.time() + max(delay, result.retry_after_sec or 0)
            changed = self.db.save_source_state(self.source_id, city, SourceState.DEGRADED, until, status.value)
            self.status = 'DEGRADED: ' + status.value
            if changed:
                await self.alert('Источник недоступен\n' + ('Изменилась структура выдачи.' if status is FetchStatus.PARSE_CHANGED
                                                        else 'Ошибка сети или сервера.') + ' Повторы ограничены паузой.')
        self.db.record_fetch(source_id=self.source_id, city=city, fetched_at=result.observed_at,
                             status=status.value, http_status=result.http_status,
                             raw_count=result.diagnostics.get('raw_count', len(result.items)), matched_count=matched,
                             overlap_count=overlap, interval_sec=(result.observed_at - previous[1]) if previous else None,
                             cooldown_until=until, item_ids_json=json.dumps(sorted(ids)),
                             **diagnostics, budget_rpm=self.budget(), comparison_context=context_id,
                             operation=operation)
        log.info('Fetch source=%s city=%s status=%s http=%s raw=%s matched=%s overlap=%s cooldown_until=%s',
                 self.source_id, city, status.value, result.http_status,
                 result.diagnostics.get('raw_count', len(result.items)), matched, overlap, until)

    def _process(self, items, observed_at: float, collection: dict | None = None) -> int:
        subs = self.db.subscriptions(only_enabled=True)
        entries, matched_count = [], 0
        unseen = self.db.unseen([i.id for i in items])
        recipients = self.recipients()
        for item in items:
            if item.id not in unseen:
                continue
            item.found_at = observed_at
            matched = [s for s in subs if item.city in (s.cities or [self.cfg.city]) and matches(item, s)] if is_fresh(item) else []
            matched_count += bool(matched)
            payload = {'item': asdict(item), 'subs': [asdict(s) for s in matched], 'source_id': self.source_id}
            if collection:
                payload['collection'] = collection
            entries.append((item.id, payload, recipients if matched else []))
        inserted = self.db.stage_items(entries, observed_at)
        # Diagnostic files are never part of the delivery transaction.
        try:
            with (self.cfg.data_dir / 'items.jsonl').open('a', encoding='utf-8') as f:
                for item in items:
                    if item.id in inserted:
                        f.write(json.dumps({**asdict(item), 'source_id': self.source_id, 'seen_at': observed_at,
                                            'categories': sorted(classify(item))}, ensure_ascii=False) + '\n')
        except OSError:
            log.warning('Cannot append diagnostic items; durable events are already stored')
        return matched_count

    def _check_browser(self) -> None:
        if self.scraper.closed:
            raise BrowserClosed()

    def set_paused(self, paused: bool) -> None:
        self.paused = paused
        self.db.set('paused', '1' if paused else '0')
        if not paused:
            # Explicit user permission to try once; Retry-After always remains binding.
            now = time.time()
            for state in self.db.source_states(self.source_id):
                if state['state'] == SourceState.WAITING_FOR_USER and (state['next_allowed_at'] or 0) <= now:
                    self.db.save_source_state(self.source_id, state['city'], SourceState.ACTIVE)
                    self.db.set(f'rate_limit_streak:{self.source_id}', '0')
        self._wake.set()

    async def open_verification(self) -> str:
        async with self._source_lock:
            return await self._open_verification()

    async def _open_verification(self) -> str:
        self._check_browser()
        states = self.db.source_states(self.source_id)
        if any((s['next_allowed_at'] or 0) > time.time() for s in states):
            return 'Пауза источника ещё не истекла. Проверьте /health.'
        waiting = [s for s in states if s['state'] == SourceState.WAITING_FOR_USER]
        if not waiting:
            return 'Ограничений, требующих ручной проверки, нет. Проверьте /health.'
        if not hasattr(self.scraper, 'load'):
            return 'Этот источник не поддерживает проверку в браузере.'
        self.paused = True
        self.db.set('paused', '1')
        await self.scraper.set_window('normal')
        city = waiting[0]['city']
        result = await self.scraper.load(feed_url(city, self.cfg.feed))
        self.db.set(f'next_poll:{self.source_id}', str(time.time() + self.delay()))
        await self._handle(result, feed_url(city, self.cfg.feed), operation='manual')
        return 'Страница открыта по вашему запросу. Пройдите проверку в браузере, затем отправьте /resume.'

    def health_text(self) -> str:
        import html
        states = self.db.source_states(self.source_id)
        lines = ['<b>Здоровье бота</b>', html.escape(self.status),
                 f'Источник: {self.source_id}', f'Бюджет: {self.budget():g}/мин',
                 'Последняя проверка: ' + (datetime.fromtimestamp(self.last_ok).strftime('%d.%m %H:%M:%S') if self.last_ok else 'ещё не было'),
                 f'Фактический интервал: {self.last_interval_sec:.1f} с' if self.last_interval_sec else 'Фактический интервал: ещё не измерен',
                 'Очередь: ' + str(self.db.queue_counts())]
        if hasattr(self.scraper, 'browser_engine'):
            lines.append(f'Браузер: {self.scraper.browser_engine}/{self.scraper.browser_channel}, '
                         f'режим: {self.scraper.fetch_mode}')
        for s in states:
            until = datetime.fromtimestamp(s['next_allowed_at']).strftime('%d.%m %H:%M:%S') if s['next_allowed_at'] else '—'
            lines.append(html.escape(f"{s['city']}: {s['state']} ({s['reason']}), до {until}"))
        latency = self.db.latency_stats()
        for metric, label in (('delivery', 'Обнаружение → Telegram'), ('publish', 'Публикация → Telegram')):
            stats = latency[metric]
            if stats:
                lines.append(f"{label}: n={stats['n']}, p50={stats['p50']:.1f} с, p95={stats['p95']:.1f} с, ≤60 с: {stats['within_60']:.0%}")
        study = self.db.conn.execute('SELECT id,ends_at FROM studies WHERE source_id=? AND status=\'running\'',
                                     (self.source_id,)).fetchone()
        if study:
            lines.append('Наблюдение №' + str(study[0]) + ': идёт до '
                         + datetime.fromtimestamp(study[1]).strftime('%d.%m %H:%M'))
        return '\n'.join(lines)
