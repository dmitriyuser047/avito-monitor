"""Persistent source scheduler. Collection never waits for Telegram item delivery."""
import asyncio
import hashlib
import json
import logging
import time
from dataclasses import asdict
from datetime import datetime
from urllib.parse import urlparse

from .config import Config, feed_url
from .db import DB
from .matcher import classify, is_fresh, matches
from .sources import FetchResult, FetchStatus, Source, SourceState
from .study import StudyRecorder

log = logging.getLogger(__name__)

# A captcha or block page is lifted only by a confirmed catalog, never by /resume alone.
VERIFICATION_REASONS = ('blocked', 'challenge')
MAX_LOCAL_BACKOFF = 6 * 3600
REASON_TEXT = {
    'blocked': 'Авито ограничил доступ (403 или страница блокировки)',
    'challenge': 'капча или проверка браузера',
    'HTTP 429': 'HTTP 429: превышена частота запросов',
    'repeated HTTP 429': 'повторный HTTP 429 подряд',
    'parse_changed': 'изменилась структура выдачи',
    'network_error': 'ошибка сети или сервера',
}


def rate_limit_terms(now: float, retry_after: float | None, base: float, streak: int):
    """Server Retry-After binds as given; local backoff applies only when the server named no term."""
    if retry_after:
        return now + retry_after, now + retry_after
    return None, now + min(MAX_LOCAL_BACKOFF, base * 2 ** max(0, streak - 1))


def _when(ts: float | None) -> str:
    return datetime.fromtimestamp(ts).strftime('%d.%m %H:%M:%S') if ts else '—'


def _left(seconds: float) -> str:
    minutes = max(1, round(seconds / 60))
    return f'{minutes // 60} ч {minutes % 60} мин' if minutes >= 60 else f'{minutes} мин'


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
        # Anything set before this moment was restored from SQLite, not answered by Avito in this run.
        # Saved states are kept with their original time and code; startup makes no request.
        self.started_at = time.time()
        self.collection_confirmed = False
        if self._rate_limit_streak() >= 2:
            # Two 429 in a row already require /resume; a cooldown saved by an earlier release keeps
            # that policy, with its original time and code, so it is reported as old, not as a new answer.
            for state in db.source_states(self.source_id):
                if state['state'] == SourceState.COOLDOWN:
                    db.save_source_state(self.source_id, state['city'], SourceState.WAITING_FOR_USER,
                                         state['next_allowed_at'], 'repeated HTTP 429', state['http_status'],
                                         state['server_until'], since=state['since'])
        self._restored = [s for s in db.source_states(self.source_id) if self._restricting(s, self.started_at)]
        for s in self._restored:
            log.info('Restored restriction city=%s state=%s reason=%s http=%s since=%s until=%s',
                     s['city'], s['state'], s['reason'], self._http_status(s), s['since'], s['next_allowed_at'])

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

    @staticmethod
    def _restricting(state: dict, now: float) -> bool:
        if state['state'] == SourceState.WAITING_FOR_USER:
            return True
        return state['state'] in (SourceState.COOLDOWN, SourceState.DEGRADED) and (state['next_allowed_at'] or 0) > now

    def _http_status(self, state: dict) -> int | None:
        if state.get('http_status') is not None:
            return state['http_status']
        # Rows saved by earlier releases: take the answer that set the restriction.
        row = self.db.conn.execute("SELECT http_status FROM fetch_metrics WHERE source_id=? AND city=?"
                                   " AND fetched_at<=? ORDER BY id DESC LIMIT 1",
                                   (self.source_id, state['city'], state['since'] + 1)).fetchone()
        return row[0] if row else None

    def _last_request_at(self, city: str) -> float | None:
        return self.db.conn.execute("SELECT MAX(fetched_at) FROM fetch_metrics WHERE source_id=? AND city=?",
                                    (self.source_id, city)).fetchone()[0]

    def restriction_label(self, state: dict) -> str:
        parts = []
        if state['since'] < self.started_at:
            parts.append('старая сохранённая блокировка')
        elif 'HTTP 429' in state['reason']:
            parts.append('новый HTTP 429')
        elif state['state'] == SourceState.DEGRADED:
            parts.append('локальная пауза после ошибки')
        else:
            parts.append('новое ограничение Авито')
        if state['state'] == SourceState.WAITING_FOR_USER:
            parts.append('ожидается проверка' if state['reason'] in VERIFICATION_REASONS
                         else 'автоопрос остановлен, нужен /resume')
        return ' · '.join(parts)

    def restriction_details(self, state: dict, now: float | None = None) -> list[str]:
        now = time.time() if now is None else now
        code = self._http_status(state)
        until, server = state['next_allowed_at'], state.get('server_until')
        if until and until > now:
            kind = 'срок Авито (Retry-After)' if server and server >= until - 1 else 'локальная пауза бота'
            wait = f'{kind} до {_when(until)}, осталось {_left(until - now)}'
        elif state['state'] == SourceState.WAITING_FOR_USER and state['reason'] in VERIFICATION_REASONS:
            wait = 'срока нет: нужна ручная проверка страницы (/unblock, затем /resume)'
        elif state['state'] == SourceState.WAITING_FOR_USER:
            wait = 'срок истёк: опрос возобновится только после /resume'
        else:
            wait = 'срок истёк'
        return [f"Причина: {REASON_TEXT.get(state['reason'], state['reason'] or '—')}",
                'HTTP-статус: ' + (str(code) if code is not None else 'нет (ответ без HTTP-кода)'),
                'Последний запрос к Авито: ' + _when(self._last_request_at(state['city'])),
                'Ограничение установлено: ' + _when(state['since']),
                'Ожидание: ' + wait]

    def photos_allowed(self) -> bool:
        """Photo requests go to Avito too: only after a confirmed catalog in this run and with no restriction."""
        if self.paused or self._source_stopped or not self.collection_confirmed:
            return False
        now = time.time()
        return not any(self._restricting(s, now) for s in self.db.source_states(self.source_id))

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

    def _restored_text(self, states: list[dict]) -> str:
        engine = getattr(self.scraper, 'browser_engine', None)
        lines = ['Сохранённое ограничение с прошлого запуска',
                 'При запуске новых запросов к Авито не было: это состояние из базы, а не ответ '
                 + (f'текущего браузера {engine}.' if engine else 'текущего запуска.')]
        for s in states:
            lines.append(f"\n{s['city']}: {self.restriction_label(s)}")
            lines.extend(self.restriction_details(s))
        if self.paused:
            lines.append('\nДополнительно включена ручная пауза (/resume).')
        return '\n'.join(lines)

    def _waiting_status(self, state: dict) -> str:
        return f"{state['state']}: {self.restriction_label(state)}"

    def _stop_reason(self, now: float) -> str | None:
        """Why the next scheduled check cannot run; None when it may."""
        if self.paused:
            return 'PAUSED: пауза'
        if not self.db.subscriptions(only_enabled=True):
            return 'нет активных запросов'
        for state in self.db.source_states(self.source_id):
            if self._restricting(state, now):
                return self._waiting_status(state)
        return None

    def refresh_status(self) -> None:
        """Show the effect of a command at once instead of waiting for the next poll."""
        if self._source_stopped:
            return
        stop = self._stop_reason(time.time())
        if stop:
            self.status = stop
        elif not self.status.startswith('ACTIVE'):
            self.status = 'ACTIVE: ожидает следующей проверки'

    async def _poll_once(self) -> bool:
        if self._restored:
            restored, self._restored = self._restored, []
            await self.alert(self._restored_text(restored))
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
        now = time.time()
        stop = self._stop_reason(now)
        if stop:
            self.status = stop
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
        engine = diagnostics['browser_engine'] or getattr(self.scraper, 'browser_engine', None)
        answered = f'Ответ получен сейчас{" браузером " + engine if engine else ""}. '
        if status in (FetchStatus.OK, FetchStatus.NO_RESULTS):
            self.status = 'ACTIVE: работает'
            self.collection_confirmed = True
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
            streak = self._rate_limit_streak() + 1
            self.db.set(f'rate_limit_streak:{self.source_id}', str(streak))
            server_until, until = rate_limit_terms(time.time(), result.retry_after_sec,
                                                   self.cfg.cooldown_seconds, streak)
            state = SourceState.WAITING_FOR_USER if streak >= 2 else SourceState.COOLDOWN
            changed = self.db.save_source_state(self.source_id, city, state, until,
                                                'repeated HTTP 429' if streak >= 2 else 'HTTP 429',
                                                result.http_status, server_until)
            self.status = state + ': новый HTTP 429'
            if changed:
                term = ('срок Авито (Retry-After)' if server_until else 'локальная пауза бота, Авито срок не указал')
                await self.alert('Авито ограничил частоту: новый HTTP 429\n' + answered
                                 + ('Повторное ограничение: автоматический опрос полностью остановлен. '
                                    'После окончания паузы потребуется /resume.\n' if streak >= 2 else '')
                                 + f'Пауза до {datetime.fromtimestamp(until).strftime("%d.%m %H:%M")}: {term}. '
                                 'Повторных запросов до этого времени нет. Очередь Telegram работает.')
        elif status in (FetchStatus.BLOCKED, FetchStatus.CHALLENGE):
            server_until = until = time.time() + result.retry_after_sec if result.retry_after_sec else None
            changed = self.db.save_source_state(self.source_id, city, SourceState.WAITING_FOR_USER, until,
                                                status.value, result.http_status, server_until)
            self.status = 'WAITING_FOR_USER: новое ограничение Авито · ожидается проверка'
            if changed:
                if hasattr(self.scraper, 'set_window'):
                    await self.scraper.set_window('normal')
                code = f'HTTP {result.http_status}' if result.http_status else 'без HTTP-кода'
                await self.alert(f'Авито ограничил доступ: {REASON_TEXT[status.value]}, {code}\n' + answered
                                 + 'Опрос полностью остановлен и сам не возобновится. '
                                 'Команда /unblock откроет страницу проверки по вашему запросу. '
                                 'Пройдите проверку в браузере, затем /resume: бот прочитает уже открытую '
                                 'страницу без нового запроса. Очередь Telegram работает.')
        else:
            self._errors[city] = int(self.db.get(f'errors:{self.source_id}:{city}') or 0) + 1
            self.db.set(f'errors:{self.source_id}:{city}', str(self._errors[city]))
            delay = min(3600, 60 * 2 ** min(self._errors[city] - 1, 6))
            if status is FetchStatus.PARSE_CHANGED:
                delay = max(900, delay)
            server_until = time.time() + result.retry_after_sec if result.retry_after_sec else None
            until = max(time.time() + delay, server_until or 0)
            changed = self.db.save_source_state(self.source_id, city, SourceState.DEGRADED, until, status.value,
                                                result.http_status, server_until)
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
        """Only the user's own pause; Avito restrictions are separate states."""
        self.paused = paused
        self.db.set('paused', '1' if paused else '0')
        if not paused:
            self._release_expired_rate_limits()
        self.refresh_status()
        self._wake.set()

    def _release_expired_rate_limits(self) -> None:
        # Explicit permission to try once after repeated 429; every term stays binding and a captcha
        # or block page needs a confirmed catalog. A further 429 stops collection again at once.
        now = time.time()
        for state in self.db.source_states(self.source_id):
            if (state['state'] == SourceState.WAITING_FOR_USER and state['reason'] not in VERIFICATION_REASONS
                    and (state['next_allowed_at'] or 0) <= now):
                self.db.save_source_state(self.source_id, state['city'], SourceState.ACTIVE)
                self.db.set(f'rate_limit_streak:{self.source_id}', '1')

    async def resume(self) -> str:
        async with self._source_lock:
            try:
                return await self._resume()
            finally:
                self.refresh_status()

    async def _resume(self) -> str:
        self.set_paused(False)
        now = time.time()
        states = [s for s in self.db.source_states(self.source_id) if self._restricting(s, now)]
        checks = [s for s in states if s['state'] == SourceState.WAITING_FOR_USER
                  and s['reason'] in VERIFICATION_REASONS]
        if checks:
            return await self._confirm_loaded_page(checks)
        if states:
            return ('Ручная пауза снята, но ограничение Авито ещё действует и не сбрасывается командой:\n'
                    + '\n'.join(f"{s['city']}: {self.restriction_label(s)}. "
                                 + self.restriction_details(s, now)[-1] for s in states))
        return 'Опрос возобновлён.'

    async def _confirm_loaded_page(self, checks: list[dict]) -> str:
        """Read the page already open in the browser; a confirmed catalog lifts the check without a request."""
        if not hasattr(self.scraper, 'read_current') or self.scraper.closed:
            return 'Проверка Авито не подтверждена: браузер недоступен. Опрос остаётся остановленным.'
        page_url = getattr(getattr(self.scraper, 'page', None), 'url', '') or ''
        if (urlparse(page_url).hostname or '').removeprefix('www.') != 'avito.ru':
            return ('Проверка Авито не подтверждена: в браузере не открыта выдача Авито. '
                    'Отправьте /unblock, пройдите проверку и повторите /resume.')
        result = await self.scraper.read_current()
        if result.status not in (FetchStatus.OK, FetchStatus.NO_RESULTS):
            return ('Проверка Авито не подтверждена: на открытой странице нет обычной выдачи '
                    f'({result.status.value}). Опрос остаётся остановленным, новых запросов не было.')
        result.diagnostics.update(actual_mode='loaded_dom', is_warmup=False,
                                  browser_engine=getattr(self.scraper, 'browser_engine', None),
                                  browser_channel=getattr(self.scraper, 'browser_channel', None),
                                  fetch_mode=getattr(self.scraper, 'fetch_mode', None),
                                  run_id=getattr(self.scraper, 'run_id', None))
        # Same feed key as automatic polling, so the confirmed page is not mistaken for a new baseline.
        feed = feed_url(result.city, self.cfg.feed)
        same_feed = urlparse(page_url).path.strip('/') == urlparse(feed).path.strip('/')
        await self._handle(result, feed if same_feed else page_url, operation='manual')
        now, waiting = time.time(), []
        for state in checks:
            server = state.get('server_until')
            if server and server > now:
                # The catalog is confirmed, but the server-given term still binds.
                self.db.save_source_state(self.source_id, state['city'], SourceState.COOLDOWN, server,
                                          'проверка пройдена, действует срок Авито', state['http_status'], server)
                waiting.append(f"{state['city']}: до {_when(server)}")
            else:
                self.db.save_source_state(self.source_id, state['city'], SourceState.ACTIVE)
        self._wake.set()
        if waiting:
            return ('Обычная выдача подтверждена по уже открытой странице. Сбор начнётся после срока Авито: '
                    + ', '.join(waiting))
        return 'Обычная выдача подтверждена по уже открытой странице, без нового запроса. Сбор возобновлён.'

    async def open_verification(self) -> str:
        async with self._source_lock:
            try:
                return await self._open_verification()
            finally:
                self.refresh_status()

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
        # WAITING_FOR_USER already stops polling; the user's pause flag is not touched.
        await self.scraper.set_window('normal')
        city = waiting[0]['city']
        result = await self.scraper.load(feed_url(city, self.cfg.feed))
        self.db.set(f'next_poll:{self.source_id}', str(time.time() + self.delay()))
        await self._handle(result, feed_url(city, self.cfg.feed), operation='manual')
        if result.status in (FetchStatus.OK, FetchStatus.NO_RESULTS):
            # No term is active here (checked above); a confirmed catalog ends every manual wait.
            for state in waiting:
                self.db.save_source_state(self.source_id, state['city'], SourceState.ACTIVE)
            self._wake.set()
            return 'Страница открыта, обычная выдача подтверждена. Сбор возобновлён.'
        if result.status is FetchStatus.RATE_LIMITED:
            return 'Авито ответил HTTP 429. Опрос остановлен до окончания паузы, подробности в /health.'
        return ('Страница открыта по вашему запросу. Пройдите проверку в окне браузера, затем отправьте /resume: '
                'бот прочитает уже открытую страницу без нового запроса.')

    def health_text(self) -> str:
        import html
        now = time.time()
        states = self.db.source_states(self.source_id)
        lines = ['<b>Здоровье бота</b>', html.escape(self.status),
                 f'Источник: {self.source_id}', f'Бюджет: {self.budget():g}/мин',
                 'Последняя проверка: ' + (datetime.fromtimestamp(self.last_ok).strftime('%d.%m %H:%M:%S') if self.last_ok else 'ещё не было'),
                 f'Фактический интервал: {self.last_interval_sec:.1f} с' if self.last_interval_sec else 'Фактический интервал: ещё не измерен',
                 'Очередь: ' + str(self.db.queue_counts())]
        if hasattr(self.scraper, 'browser_engine'):
            lines.append(f'Браузер: {self.scraper.browser_engine}/{self.scraper.browser_channel}, '
                         f'режим: {self.scraper.fetch_mode}')
        if self.paused:
            lines.append('Ручная пауза: включена пользователем, снимается /resume')
        for s in states:
            if self._restricting(s, now):
                lines.append(html.escape(f"{s['city']}: {self.restriction_label(s)} ({s['state']}, {s['reason']})"))
                lines.extend(html.escape('  ' + line) for line in self.restriction_details(s, now))
            else:
                until = _when(s['next_allowed_at']) if s['next_allowed_at'] else '—'
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
