"""Persistent source scheduler. Collection never waits for Telegram item delivery.

There is no user pause and no /resume: a term ends by itself, a passed check is confirmed from the
page already open in the browser, and a closed browser is started again. Restrictions answered by
the source (429/Retry-After, 403, captcha) always stay binding and are never retried blindly.
"""
import asyncio
import hashlib
import json
import logging
import time
import uuid
from dataclasses import asdict
from datetime import datetime
from urllib.parse import urlparse

from .config import Config, feed_url
from .db import DB
from .matcher import classify, is_fresh, matches
from .sources import (FetchResult, FetchStatus, Search, ServiceState, Source, SourceState,
                      TIMED_STATES, USER_STATES)
from .study import StudyRecorder
from .verification import VerificationCoordinator

log = logging.getLogger(__name__)

REASON_TEXT = {
    'blocked': 'Авито запретил доступ (403 или страница блокировки)',
    'challenge': 'капча или проверка браузера',
    'HTTP 429': 'HTTP 429: превышена частота запросов',
    'repeated HTTP 429': 'повторный HTTP 429',
    'parse_changed': 'страница не распознана',
    'network_error': 'ошибка сети или сервера',
    'verification passed; Retry-After': 'проверка пройдена, действует срок Авито',
}
STATE_TEXT = {
    SourceState.RATE_LIMITED: 'ограничение частоты',
    SourceState.VERIFICATION_REQUIRED: 'ожидается проверка в браузере',
    SourceState.ACCESS_DENIED: 'доступ запрещён',
    SourceState.PARSER_ERROR: 'страница не распознана',
    SourceState.NETWORK_UNAVAILABLE: 'сеть или сервер недоступны',
    SourceState.RECOVERING: 'восстановление',
}
# Browser restarts: bounded delays, never a loop that hammers the source (start makes no request).
RESTART_DELAYS = (5, 30, 60, 300, 600)
FAILED_AFTER = 5


def backoff(base: float, ceiling: float, level: int) -> float:
    return min(ceiling, base * 2 ** max(0, level - 1))


def rate_limit_terms(now: float, retry_after: float | None, base: float, level: int,
                     ceiling: float = 6 * 3600):
    """Server Retry-After binds as given; the local backoff applies only when the server named no term."""
    if retry_after:
        return now + retry_after, now + retry_after
    return None, now + backoff(base, ceiling, level)


def _when(ts: float | None) -> str:
    return datetime.fromtimestamp(ts).strftime('%d.%m %H:%M:%S') if ts else '—'


def _left(seconds: float) -> str:
    minutes = max(1, round(seconds / 60))
    return f'{minutes // 60} ч {minutes % 60} мин' if minutes >= 60 else f'{minutes} мин'


class Monitor:
    def __init__(self, cfg: Config, db: DB, scraper: Source, alert, send_item=None, recipients=None,
                 start_source=None):
        self.cfg, self.db, self.scraper, self.alert = cfg, db, scraper, alert
        self.source_id = getattr(scraper, 'source_id', 'local_browser')
        self.recipients = recipients or (lambda: [])
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
        self._wake = asyncio.Event()
        self._source_lock = asyncio.Lock()
        self.study = StudyRecorder(db, cfg.data_dir)
        self.verification = VerificationCoordinator(self, cfg.verification_check_seconds)
        self.delivery = None  # set by the application, shown in /health
        # Anything set before this moment was restored from SQLite, not answered in this run.
        self.started_at = time.time()
        self.heartbeat = time.monotonic()
        self.service_state = ServiceState.RUNNING
        self.collection_confirmed = False
        self.start_source = start_source or self._start_browser
        self.source_down: str | None = None
        self._restart_at = 0.0
        self._restart_attempts = 0
        self._loop_errors = 0
        self._restored = [s for s in db.source_states(self.source_id) if self._restricting(s, self.started_at)]
        for s in self._restored:
            log.info('Restored restriction city=%s state=%s reason=%s http=%s since=%s until=%s',
                     s['city'], s['state'], s['reason'], self._http_status(s), s['since'], s['next_allowed_at'])

    # ---------- state helpers ----------

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
        if state['state'] in USER_STATES:
            return True
        return state['state'] in TIMED_STATES and (state['next_allowed_at'] or 0) > now

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
        text = STATE_TEXT.get(state['state'], state['state'])
        if state['since'] < self.started_at:
            return f'{text} · сохранено с прошлого запуска'
        if state['state'] == SourceState.RATE_LIMITED and 'HTTP 429' in state['reason']:
            return 'новый HTTP 429'
        return text

    def restriction_details(self, state: dict, now: float | None = None) -> list[str]:
        now = time.time() if now is None else now
        code = self._http_status(state)
        until, server = state['next_allowed_at'], state.get('server_until')
        if state['state'] in USER_STATES:
            wait = 'нужна проверка в окне Chrome бота; после неё сбор продолжится сам, команды не нужны'
            if server and server > now:
                wait += f'; затем срок Авито до {_when(server)}'
        elif until and until > now:
            kind = 'срок Авито (Retry-After)' if server and server >= until - 1 else 'локальная пауза бота'
            wait = f'{kind} до {_when(until)}, осталось {_left(until - now)}; затем автоматическая проверка'
        else:
            wait = 'срок истёк, ближайшая проверка по расписанию'
        return [f"Причина: {REASON_TEXT.get(state['reason'], state['reason'] or '—')}",
                'HTTP-статус: ' + (str(code) if code is not None else 'нет (ответ без HTTP-кода)'),
                'Последний запрос к Авито: ' + _when(self._last_request_at(state['city'])),
                'Ограничение установлено: ' + _when(state['since']),
                'Ожидание: ' + wait]

    def photos_allowed(self) -> bool:
        """Photo requests go to Avito too: only after a confirmed catalog in this run and with no restriction."""
        if self.source_down or not self.collection_confirmed:
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

    def _event(self, kind: str, **fields) -> None:
        try:
            self.db.record_event(kind, source_id=self.source_id, **fields)
        except Exception:
            log.exception('Cannot record diagnostic event %s', kind)

    # ---------- main loop ----------

    async def run(self) -> None:
        while True:
            self.heartbeat = time.monotonic()
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Never a permanent stop: back off, keep Telegram running and try again.
                self._loop_errors += 1
                delay = min(600, 5 * 2 ** min(self._loop_errors - 1, 7))
                log.exception('Scheduler failure #%s; retry in %ss, Telegram remains available',
                              self._loop_errors, delay)
                self.status = f'RECOVERING: ошибка планировщика ({type(exc).__name__}), повтор через {delay} с'
                self._event('source_failed', result='scheduler_error', error=type(exc).__name__,
                            attempt=self._loop_errors)
                if self._loop_errors == FAILED_AFTER:
                    self.service_state = ServiceState.FAILED
                    try:
                        await self.alert(f'Ошибка планировщика повторяется ({type(exc).__name__}).\n'
                                         'Сбор пытается восстановиться сам; Telegram работает. Проверьте /health.')
                    except Exception:
                        log.exception('Cannot send scheduler alert')
                await asyncio.sleep(delay)
                continue
            if self._loop_errors:
                self._loop_errors = 0
                if self.service_state == ServiceState.FAILED and not self.source_down:
                    self.service_state = ServiceState.RUNNING
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
                 + (f'текущего браузера {engine}.' if engine else 'текущего запуска.')
                 + ' Сбор продолжится сам, когда это будет разрешено.']
        for s in states:
            lines.append(f"\n{s['city']}: {self.restriction_label(s)}")
            lines.extend(self.restriction_details(s))
        return '\n'.join(lines)

    def _stop_reason(self, now: float) -> str | None:
        """Why the next scheduled check cannot run; None when it may."""
        if self.source_down:
            return f'RESTARTING: браузер перезапускается ({self.source_down})'
        if not self.db.subscriptions(only_enabled=True):
            return 'IDLE: нет активных запросов'
        for state in self.db.source_states(self.source_id):
            if self._restricting(state, now):
                return f"{state['state']}: {self.restriction_label(state)}"
        return None

    def refresh_status(self) -> None:
        stop = self._stop_reason(time.time())
        if stop:
            self.status = stop
        elif not self.status.startswith('AVAILABLE'):
            self.status = 'AVAILABLE: ожидает следующей проверки'

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
        if not await self._ensure_browser():
            return False
        await self.verification.tick()
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
        search = Search(city, self.cfg.feed, feed_url(city, self.cfg.feed))
        started = time.monotonic()
        if self._last_started:
            self.last_interval_sec = now - self._last_started
        self._last_started = now
        # Persist budget before I/O, so a crash/restart cannot immediately poll again.
        self.db.set(f'next_poll:{self.source_id}', str(now + self.delay()))
        self.status = f'{SourceState.CHECKING}: {city}'
        result = await (self.scraper.check(search) if hasattr(self.scraper, 'check')
                        else self.scraper.fetch(search.url))
        self.last_cycle_sec = time.monotonic() - started
        self.db.set(f'next_poll:{self.source_id}', str(time.time() + self.delay()))
        await self._handle(result, search.url)
        return True

    # ---------- browser lifecycle ----------

    async def _start_browser(self) -> None:
        await self.scraper.start()

    def source_failed(self, reason: str) -> None:
        """Startup or crash of the browser/proxy: not a restriction of the source."""
        if self.source_down is None:
            self._restart_attempts = 0
            self._restart_at = time.monotonic() + RESTART_DELAYS[0]
        self.source_down = reason
        self.service_state = ServiceState.RESTARTING
        self.status = f'RESTARTING: браузер перезапускается ({reason})'

    async def _ensure_browser(self) -> bool:
        if self.source_down is None:
            if not getattr(self.scraper, 'closed', False):
                return True
            self.source_failed('browser_closed')
            self._event('source_failed', result='browser_closed')
            await self.alert('Браузер бота закрылся\nПерезапускаю его автоматически. Telegram и очередь работают, '
                             'сохранённые ограничения Авито не сбрасываются.')
        if time.monotonic() < self._restart_at:
            return False
        try:
            try:
                await self.scraper.stop()
            except Exception:
                log.debug('Browser stop before restart failed', exc_info=True)
            await self.start_source()
        except Exception as exc:
            self._restart_attempts += 1
            delay = RESTART_DELAYS[min(self._restart_attempts, len(RESTART_DELAYS) - 1)]
            self._restart_at = time.monotonic() + delay
            reason = f'{type(exc).__name__}, попытка {self._restart_attempts}'
            self.source_down = reason
            self.status = f'RESTARTING: браузер не запустился ({reason}), повтор через {delay} с'
            log.error('Browser restart failed: %s; retry in %ss', type(exc).__name__, delay)
            self._event('source_failed', result='browser_start_failed', error=type(exc).__name__,
                        attempt=self._restart_attempts)
            if self._restart_attempts == FAILED_AFTER:
                self.service_state = ServiceState.FAILED
                await self.alert(f'Браузер не запускается ({type(exc).__name__}).\nПопытки продолжаются '
                                 f'каждые {RESTART_DELAYS[-1] // 60} мин. Проверьте установку Chrome и /health.')
            return False
        attempts, self._restart_attempts = self._restart_attempts, 0
        self.source_down = None
        self.service_state = ServiceState.RUNNING
        self.collection_confirmed = False
        log.info('Browser started again after %s failed attempts', attempts)
        self._event('source_recovered', result='browser_started', attempts=attempts)
        self.refresh_status()
        return True

    # ---------- results ----------

    async def _handle(self, result: FetchResult, url: str, operation: str = 'automatic',
                      announce: bool = True) -> None:
        status, city = result.status, result.city
        correlation_id = uuid.uuid4().hex[:12]
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
        before = next((s for s in self.db.source_states(self.source_id) if s['city'] == city), None)
        until = None
        matched = 0
        previous = self.db.latest_snapshot(self.source_id, city)
        ids = {i.id for i in result.items}
        overlap = len(ids & previous[0]) if previous else None
        engine = diagnostics['browser_engine'] or getattr(self.scraper, 'browser_engine', None)
        answered = f'Ответ получен сейчас{" браузером " + engine if engine else ""}. '
        self._event('source_checked', correlation_id=correlation_id, city=city, result=status.value,
                    http_status=result.http_status, operation=operation, actual_mode=diagnostics['actual_mode'],
                    duration_sec=diagnostics['duration_sec'], request_count=diagnostics['request_count'],
                    transferred_bytes=diagnostics['transferred_bytes'])
        if status in (FetchStatus.OK, FetchStatus.NO_RESULTS):
            self.status = f'{SourceState.AVAILABLE}: работает'
            self.collection_confirmed = True
            self.last_ok = result.observed_at
            self._errors[city] = 0
            self.db.set(f'errors:{self.source_id}:{city}', '0')
            self.db.set(f'rate_limit_streak:{self.source_id}', '0')
            self.db.save_source_state(self.source_id, city, SourceState.AVAILABLE)
            self._event('catalog_received', correlation_id=correlation_id, city=city, result=status.value,
                        raw_count=len(result.items))
            if before and before['state'] not in (SourceState.AVAILABLE, SourceState.CHECKING):
                recovery = time.time() - before['since']
                self._event('source_recovered', correlation_id=correlation_id, city=city,
                            result=before['state'], recovery_sec=round(recovery, 1))
                if announce:
                    await self.alert(f'Доступ к Авито восстановлен\nГород: {city}. Сбор продолжается; '
                                     f'ограничение длилось {_left(recovery)}.')
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
                matched = self._process(items, result.observed_at, collection, correlation_id)
            if ids and previous and previous[0] and overlap == 0:
                key = f'coverage:{self.source_id}:{city}'
                if self.db.get(key) != 'gap':
                    await self.alert(f'Возможны пропуски в выдаче\nГород: {city}. '
                                     'У соседних снимков нет общих объявлений. Дополнительные запросы не выполняются.')
                self.db.set(key, 'gap')
            elif ids:
                self.db.set(f'coverage:{self.source_id}:{city}', 'ok')
        elif status is FetchStatus.RATE_LIMITED:
            level = self._rate_limit_streak() + 1
            self.db.set(f'rate_limit_streak:{self.source_id}', str(level))
            server_until, until = rate_limit_terms(time.time(), result.retry_after_sec, self.cfg.cooldown_seconds,
                                                   level, self.cfg.max_backoff_seconds)
            changed = self.db.save_source_state(self.source_id, city, SourceState.RATE_LIMITED, until,
                                                'HTTP 429' if level == 1 else 'repeated HTTP 429',
                                                result.http_status, server_until)
            self.status = f'{SourceState.RATE_LIMITED}: новый HTTP 429'
            self._event('source_rate_limited', correlation_id=correlation_id, city=city, result='http_429',
                        http_status=result.http_status, retry_after_sec=result.retry_after_sec,
                        until=until, level=level, term='server' if server_until else 'local')
            if changed:
                term = ('срок Авито (Retry-After)' if server_until
                        else f'локальная пауза бота {_left(until - time.time())}, Авито срок не указал')
                await self.alert('Авито ограничил частоту: новый HTTP 429\n' + answered
                                 + f'Пауза до {datetime.fromtimestamp(until).strftime("%d.%m %H:%M")}: {term}. '
                                 'Повторных запросов до этого времени нет, затем сбор продолжится сам. '
                                 'Очередь Telegram работает.')
            else:
                log.warning('Repeated HTTP 429 level=%s; local pause until %s', level, _when(until))
        elif status in (FetchStatus.BLOCKED, FetchStatus.CHALLENGE):
            state = SourceState.VERIFICATION_REQUIRED if status is FetchStatus.CHALLENGE else SourceState.ACCESS_DENIED
            server_until = until = time.time() + result.retry_after_sec if result.retry_after_sec else None
            changed = self.db.save_source_state(self.source_id, city, state, until,
                                                status.value, result.http_status, server_until)
            saved = next(s for s in self.db.source_states(self.source_id) if s['city'] == city)
            # The open document shows the restriction only when it was answered by a navigation.
            self.verification.note_restriction(
                city, saved['since'], diagnostics['actual_mode'] in ('page_navigation', 'loaded_dom'))
            self.status = f'{state}: {STATE_TEXT[state]}'
            self._event('verification_required', correlation_id=correlation_id, city=city, result=status.value,
                        http_status=result.http_status, reason=diagnostics['restriction_reason'])
            if changed:
                if hasattr(self.scraper, 'set_window'):
                    await self.scraper.set_window('normal')
                code = f'HTTP {result.http_status}' if result.http_status else 'без HTTP-кода'
                shown = diagnostics['actual_mode'] == 'page_navigation'
                where = ('Страница проверки уже открыта в окне Chrome бота.' if shown else
                         'Разверните окно Chrome бота и обновите страницу (или отправьте /unblock), '
                         'чтобы увидеть проверку.')
                await self.alert(f'Нужна проверка Авито: {REASON_TEXT[status.value]}, {code}\n' + answered
                                 + 'Автоматические запросы остановлены, повторов вслепую нет. ' + where
                                 + ' Пройдите её — бот сам увидит обычную выдачу и продолжит сбор, '
                                 'команды отправлять не нужно. Очередь Telegram работает.')
        else:
            self._errors[city] = int(self.db.get(f'errors:{self.source_id}:{city}') or 0) + 1
            self.db.set(f'errors:{self.source_id}:{city}', str(self._errors[city]))
            delay = min(3600, 60 * 2 ** min(self._errors[city] - 1, 6))
            state = SourceState.NETWORK_UNAVAILABLE
            if status is FetchStatus.PARSE_CHANGED:
                delay, state = max(900, delay), SourceState.PARSER_ERROR
            server_until = time.time() + result.retry_after_sec if result.retry_after_sec else None
            until = max(time.time() + delay, server_until or 0)
            changed = self.db.save_source_state(self.source_id, city, state, until, status.value,
                                                result.http_status, server_until)
            self.status = f'{state}: {STATE_TEXT[state]}'
            if changed:
                await self.alert('Источник временно недоступен\n'
                                 + ('Страница Авито не распознана.' if status is FetchStatus.PARSE_CHANGED
                                    else 'Ошибка сети или сервера.')
                                 + f' Следующая попытка после {_when(until)}, сбор продолжится сам.')
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

    def _process(self, items, observed_at: float, collection: dict | None = None,
                 correlation_id: str | None = None) -> int:
        subs = self.db.subscriptions(only_enabled=True)
        entries, matched_count = [], 0
        unseen = self.db.unseen([i.id for i in items])
        recipients = self.recipients()
        matched_ids = set()
        for item in items:
            if item.id not in unseen:
                continue
            item.found_at = observed_at
            matched = [s for s in subs if item.city in (s.cities or [self.cfg.city]) and matches(item, s)] if is_fresh(item) else []
            matched_count += bool(matched)
            if matched:
                matched_ids.add(item.id)
            payload = {'item': asdict(item), 'subs': [asdict(s) for s in matched], 'source_id': self.source_id,
                       'correlation_id': correlation_id}
            if collection:
                payload['collection'] = collection
            entries.append((item.id, payload, recipients if matched else []))
        inserted = self.db.stage_items(entries, observed_at)
        for item_id, _, _ in entries:
            self._event('listing_discovered', correlation_id=correlation_id, result=str(item_id))
            if item_id in matched_ids:
                self._event('listing_matched', correlation_id=correlation_id, result=str(item_id))
                if item_id in inserted:
                    self._event('listing_staged', correlation_id=correlation_id, result=str(item_id),
                                recipients=len(recipients))
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

    # ---------- verification ----------

    async def confirm_verification(self, result: FetchResult, page_url: str, states: list[dict]) -> None:
        """A normal catalog in the loaded page ends a pending check; a server term still binds."""
        result.diagnostics.update(actual_mode='loaded_dom', is_warmup=False,
                                  browser_engine=getattr(self.scraper, 'browser_engine', None),
                                  browser_channel=getattr(self.scraper, 'browser_channel', None),
                                  fetch_mode=getattr(self.scraper, 'fetch_mode', None),
                                  run_id=getattr(self.scraper, 'run_id', None))
        now = time.time()
        # The confirmed page is an observation: the next scheduled check keeps the normal interval.
        self.db.set(f'next_poll:{self.source_id}', str(now + self.delay()))
        terms = {s['city']: s['server_until'] for s in states if (s.get('server_until') or 0) > now}
        self._event('verification_confirmed', city=result.city, result=result.status.value,
                    cities=[s['city'] for s in states], server_term=bool(terms))
        # Same feed key as automatic polling, so the confirmed page is not mistaken for a new baseline.
        feed = feed_url(result.city, self.cfg.feed)
        same_feed = urlparse(page_url).path.strip('/') == urlparse(feed).path.strip('/')
        await self._handle(result, feed if same_feed else page_url, operation='verification',
                           announce=not terms)
        for state in states:
            if state['city'] in terms:
                self.db.save_source_state(self.source_id, state['city'], SourceState.RATE_LIMITED,
                                          terms[state['city']], 'verification passed; Retry-After',
                                          state['http_status'], terms[state['city']])
            else:
                self.db.save_source_state(self.source_id, state['city'], SourceState.AVAILABLE)
        if terms:
            await self.alert('Проверка пройдена\nОбычная выдача подтверждена по открытой странице. Авито указал срок '
                             f'до {_when(max(terms.values()))} — сбор продолжится сам после него.')
        self.refresh_status()
        self._wake.set()

    async def open_verification(self) -> str:
        async with self._source_lock:
            try:
                return await self._open_verification()
            finally:
                self.refresh_status()

    async def _open_verification(self) -> str:
        if self.source_down or self.scraper.closed:
            return 'Браузер перезапускается. Проверьте /health через минуту.'
        states = self.db.source_states(self.source_id)
        if any((s['next_allowed_at'] or 0) > time.time() for s in states):
            return 'Срок ограничения ещё не истёк, страница не открывается. Подробности в /health.'
        waiting = [s for s in states if s['state'] in USER_STATES]
        if not waiting:
            return 'Проверка не требуется. Подробности в /health.'
        if not hasattr(self.scraper, 'load'):
            return 'Этот источник не поддерживает проверку в браузере.'
        await self.scraper.set_window('normal')
        city = waiting[0]['city']
        result = await self.scraper.load(feed_url(city, self.cfg.feed))
        self.db.set(f'next_poll:{self.source_id}', str(time.time() + self.delay()))
        await self._handle(result, feed_url(city, self.cfg.feed), operation='manual')
        if result.status in (FetchStatus.OK, FetchStatus.NO_RESULTS):
            # No term is active here (checked above); a confirmed catalog ends every pending check.
            for state in waiting:
                self.db.save_source_state(self.source_id, state['city'], SourceState.AVAILABLE)
            self._wake.set()
            return 'Страница открыта, обычная выдача подтверждена. Сбор продолжается.'
        if result.status is FetchStatus.RATE_LIMITED:
            return 'Авито ответил HTTP 429. Сбор продолжится сам после паузы, подробности в /health.'
        return ('Страница проверки открыта в окне Chrome бота. Пройдите проверку — сбор продолжится сам, '
                'команды отправлять не нужно.')

    # ---------- diagnostics ----------

    def health_text(self) -> str:
        import html
        now = time.time()
        states = self.db.source_states(self.source_id)
        delivery = self.delivery
        lines = ['<b>Здоровье бота</b>',
                 f'Сервис: {self.service_state}, работает {_left(now - self.started_at)}',
                 'Источник: ' + html.escape(self.status),
                 'Доставка Telegram: ' + (f'{delivery.state}' if delivery else 'не запущена')
                 + ', очередь: ' + str(self.db.queue_counts()),
                 f'Бюджет: {self.budget():g}/мин',
                 'Последняя успешная проверка: ' + (_when(self.last_ok) if self.last_ok else 'ещё не было'),
                 f'Фактический интервал: {self.last_interval_sec:.1f} с' if self.last_interval_sec else 'Фактический интервал: ещё не измерен']
        if hasattr(self.scraper, 'browser_engine'):
            lines.append(f'Браузер: {self.scraper.browser_engine}/{self.scraper.browser_channel}, '
                         f'режим: {self.scraper.fetch_mode}'
                         + (f'; перезапуск: {html.escape(self.source_down)}' if self.source_down else ''))
        for s in states:
            if self._restricting(s, now):
                lines.append(html.escape(f"{s['city']}: {self.restriction_label(s)} ({s['state']}, {s['reason']})"))
                lines.extend(html.escape('  ' + line) for line in self.restriction_details(s, now))
            else:
                lines.append(html.escape(f"{s['city']}: {s['state']}"))
        if any(s['state'] in USER_STATES for s in states):
            lines.append('Проверка в браузере: ' + html.escape(self.verification.describe()))
        lines.append(f'Политика 429 без Retry-After: пауза {_left(self.cfg.cooldown_seconds)}, '
                     f'удваивается до {_left(self.cfg.max_backoff_seconds)}; Retry-After соблюдается как есть')
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
