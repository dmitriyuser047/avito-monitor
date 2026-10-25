"""Persistent browser source with selectable engine and collection mode.

Restrictions are handled by Monitor using one source identity across all settings.
Browser/profile selection is explicit; launch failures never trigger a fallback.
"""

import logging
import importlib
import uuid
import asyncio
import math
import time
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse
from pathlib import Path

from .runtime import InstanceLock

from .matcher import Item
from .sources import FetchResult, FetchStatus

log = logging.getLogger(__name__)


# Достаёт объявления из документа: и из открытой страницы, и из HTML, полученного fetch
EXTRACT_FN = """(root) => [...root.querySelectorAll('[data-marker="catalog-serp"] [data-marker="item"]')].map(e => {
  const q = s => e.querySelector(s);
  const link = q('[data-marker="item-title"]') || q('a[itemprop="url"]');
  return {
    id: e.dataset.itemId,
    title: q('[itemprop="name"]')?.textContent?.trim() || '',
    price: q('[itemprop="price"]')?.getAttribute('content'),
    href: link?.getAttribute('href') || '',
    date: q('[data-marker="item-date"]')?.textContent?.trim() || '',
    image: q('img')?.getAttribute('src'),
    location: q('[data-marker="item-location"]')?.textContent?.replace(/\\s+/g, ' ').trim() || '',
    // описание — абзац в 4 строки; цена, район и продавец — другие абзацы
    description: (q('p[style*="max-lines-size:4"]') || q('[class*="description"]'))
      ?.textContent?.trim()?.slice(0, 600) || '',
  };
})"""

# Точное время публикации есть только в служебном JSON страницы (script type="mime/invalid"):
# у объявлений поле sortTimeStamp, мс. Ищем все объекты с id и sortTimeStamp.
TIMES_FN = """(root) => {
  const out = {};
  const walk = (o, depth) => {
    if (!o || typeof o !== 'object' || depth > 25) return;
    if (Array.isArray(o)) { for (const x of o) walk(x, depth + 1); return; }
    if (o.id && o.sortTimeStamp) out[o.id] = o.sortTimeStamp;
    for (const k in o) walk(o[k], depth + 1);
  };
  for (const s of root.querySelectorAll('script[type="mime/invalid"]')) {
    try { walk(JSON.parse(s.textContent), 0); } catch (e) {}
  }
  return out;
}"""

# Лёгкий опрос: один запрос HTML ленты из уже открытой страницы Авито (те же cookies и
# отпечаток настоящего браузера), без загрузки 350 картинок, скриптов и аналитики.
INSPECT_FN = f"""(root) => {{
  const items = ({EXTRACT_FN})(root), times = ({TIMES_FN})(root);
  for (const i of items) i.ts = times[i.id] || null;
  const body = root.body?.cloneNode(true);
  body?.querySelectorAll('script,style').forEach(e => e.remove());
  const text = (body?.textContent || '').replace(/\\s+/g, ' ').slice(0, 10000);
  return {{title: root.title || '', items,
    catalog: !!root.querySelector('[data-marker="catalog-serp"]'),
    empty: !!root.querySelector('[data-marker="search-results/empty"], [data-marker="catalog-serp/empty"]')
      || /ничего не найдено|объявлений не найдено/i.test(text),
    blocked: /доступ ограничен|проблема с ip|доступ временно ограничен/i.test(root.title + ' ' + text),
    challenge: /проверка безопасности|подтвердите, что вы не робот|captcha|капч[ау]/i.test(root.title)
      || /подтвердите, что вы не робот|пройдите проверку безопасности/i.test(text)
      || !!root.querySelector('iframe[src*="hcaptcha"], iframe[src*="geetest"], [data-marker="captcha"]')
  }};
}}"""

LIGHT_JS = f"""async ([url, wantHtml]) => {{
  const r = await fetch(url, {{credentials: 'include', cache: 'no-store',
    signal: AbortSignal.timeout(25000), headers: {{'accept': 'text/html,application/xhtml+xml'}}}});
  const meta = {{status: r.status, url: r.url, retry_after: r.headers.get('retry-after')}};
  if ([403, 429, 439].includes(r.status)) return {{...meta, items: []}};
  const doc = new DOMParser().parseFromString(await r.text(), 'text/html');
  const out = ({INSPECT_FN})(doc);
  // Debug output deliberately excludes scripts, form values, cookies and full HTML.
  return {{...out, ...meta,
    html: wantHtml ? '<html><head><title>' + doc.title.replace(/[<>]/g, '') + '</title></head><body>'
      + '<p>catalog=' + out.catalog + ', items=' + out.items.length + '</p></body></html>' : null}};
}}"""

# Compatibility name for utilities; new code uses FetchStatus.
Status = FetchStatus


def retry_after_seconds(value: str | None, now: float | None = None) -> float | None:
    if not value:
        return None
    try:
        seconds = float(value)
        return max(0, seconds) if math.isfinite(seconds) else None
    except ValueError:
        try:
            return max(0, parsedate_to_datetime(value).timestamp() - (time.time() if now is None else now))
        except (ValueError, TypeError, OverflowError):
            return None


def parse_result(raw: dict, source_id: str, city: str) -> FetchResult:
    code = raw.get('status')
    diagnostics = {'catalog': bool(raw.get('catalog')), 'raw_count': len(raw.get('items', []))}
    result = FetchResult(FetchStatus.PARSE_CHANGED, [], source_id, city, time.time(), code,
                         retry_after_seconds(raw.get('retry_after')), diagnostics)
    if code == 429:
        result.status = FetchStatus.RATE_LIMITED
    elif code == 403:
        result.status = FetchStatus.BLOCKED
    elif code == 439 or raw.get('challenge'):
        result.status = FetchStatus.CHALLENGE
    elif raw.get('blocked'):
        result.status = FetchStatus.BLOCKED
    elif code and code >= 400:
        result.status = FetchStatus.NETWORK_ERROR
    elif not raw.get('catalog'):
        if raw.get('empty'):
            result.status = FetchStatus.NO_RESULTS
    elif raw.get('items'):
        try:
            result.items = [_to_item(r) for r in raw['items']]
        except (ValueError, TypeError, KeyError, OverflowError):
            result.items = []
            result.diagnostics['validation_error'] = True
        else:
            result.status = FetchStatus.OK
    elif raw.get('empty'):
        result.status = FetchStatus.NO_RESULTS
    return result


class Scraper:
    source_id = 'local_browser'

    def __init__(self, profile_dir: Path, proxy_url: str | None, headless: bool = False,
                 save_debug_html: bool = False, debug_retention_hours: float = 24,
                 browser_engine: str = 'playwright', fetch_mode: str = 'light_fetch',
                 browser_channel: str = 'chrome'):
        self.profile_dir, self.proxy_url, self.headless = profile_dir, proxy_url, headless
        if browser_engine not in ('playwright', 'patchright'):
            raise ValueError('Unknown browser engine')
        if fetch_mode not in ('light_fetch', 'page_navigation'):
            raise ValueError('Unknown fetch mode')
        if browser_channel not in ('chrome', 'chromium'):
            raise ValueError('Unknown browser channel')
        self.browser_engine, self.fetch_mode, self.browser_channel = browser_engine, fetch_mode, browser_channel
        # Keep the original Playwright paths, so rollback reuses its own session.
        base = profile_dir if browser_engine == 'playwright' else profile_dir.with_name(profile_dir.name + '-patchright')
        self.user_data_dir = base.with_name(base.name + '-chrome') if browser_channel == 'chrome' else base
        self._browser_lock = None
        self._network = None
        self._request_count = self._transferred_bytes = 0
        self.run_id = uuid.uuid4().hex
        self._pw = None
        self.ctx = None
        self.page = None
        self._final_url: dict[str, str] = {}
        self.light_ok = False
        self._closed = False
        self._sample_written = False
        self.sample_path = profile_dir.parent / 'feed_sample.html' if save_debug_html else None
        legacy = profile_dir.parent / 'feed_sample.html'
        if legacy.exists() and (not save_debug_html or time.time() - legacy.stat().st_mtime > debug_retention_hours * 3600):
            legacy.unlink()
        self.debug_retention_hours = debug_retention_hours

    def cleanup_debug(self) -> None:
        if self.sample_path and self.sample_path.exists() and (
            time.time() - self.sample_path.stat().st_mtime > self.debug_retention_hours * 3600
        ):
            self.sample_path.unlink()

    @property
    def closed(self) -> bool:
        return self._closed or bool(self.page and self.page.is_closed())

    async def start(self) -> None:
        if self._pw or self.ctx:
            raise RuntimeError('Browser already started')
        try:
            api = importlib.import_module(f'{self.browser_engine}.async_api')
        except ImportError as exc:
            command = 'uv sync --frozen --extra experimental' if self.browser_engine == 'patchright' else 'uv sync --frozen'
            log.error('Selected engine %s is unavailable. Install it: %s', self.browser_engine, command)
            raise RuntimeError(f'Selected engine {self.browser_engine} is not installed; run {command}') from exc
        self.profile_dir.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        lock = InstanceLock(self.profile_dir.parent / 'browser.lock')
        try:
            lock.__enter__()
        except BlockingIOError:
            raise RuntimeError('Another browser engine is running; stop it first') from None
        self._browser_lock = lock
        try:
            self.user_data_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
            self.user_data_dir.chmod(0o700)
            self._closed = False
            self.light_ok = False
            self._final_url.clear()
            self.run_id = uuid.uuid4().hex
            self._pw = await api.async_playwright().start()
            self.ctx = await self._pw.chromium.launch_persistent_context(
                str(self.user_data_dir), channel=self.browser_channel, headless=self.headless,
                proxy={'server': self.proxy_url} if self.proxy_url else None,
                locale='ru-RU', timezone_id='Europe/Moscow', no_viewport=True,
                args=['--window-size=1400,900'])
            log.info('Browser engine=%s channel=%s mode=%s',
                     self.browser_engine, self.browser_channel, self.fetch_mode)
            self.page = self.ctx.pages[0] if self.ctx.pages else await self.ctx.new_page()
            self.ctx.on('close', lambda *_: setattr(self, '_closed', True))
            self.page.on('close', lambda *_: setattr(self, '_closed', True))
            await self._start_network_metrics()
            await self.set_window('minimized')
        except BaseException:
            log.error('Cannot start %s/%s; check browser installation and profile access. No fallback was attempted.',
                      self.browser_engine, self.browser_channel)
            await self.stop()
            raise

    async def _start_network_metrics(self) -> None:
        session = None
        self._request_count = self._transferred_bytes = 0
        try:
            session = await self.ctx.new_cdp_session(self.page)
            session.on('Network.requestWillBeSent', self._count_request)
            session.on('Network.loadingFinished', self._count_bytes)
            await session.send('Network.enable')
            self._network = session
        except Exception:
            if session:
                try:
                    await session.detach()
                except Exception:
                    pass
            log.warning('Passive network counters unavailable; metrics will be null')

    def _count_request(self, _event) -> None:
        self._request_count += 1

    def _count_bytes(self, event) -> None:
        self._transferred_bytes += max(0, int(event.get('encodedDataLength', 0)))

    async def stop(self) -> None:
        try:
            try:
                if self.ctx:
                    await self.ctx.close()
            finally:
                if self._pw:
                    await self._pw.stop()
        finally:
            self.ctx = self.page = self._pw = self._network = None
            self._closed = True
            if self._browser_lock:
                self._browser_lock.__exit__(None, None, None)
                self._browser_lock = None

    def _measurement(self):
        return time.monotonic(), self._request_count, self._transferred_bytes

    def _measured(self, result: FetchResult, measurement, actual_mode: str) -> FetchResult:
        started, requests, transferred = measurement
        result.diagnostics.update(
            browser_engine=self.browser_engine, fetch_mode=self.fetch_mode,
            browser_channel=self.browser_channel, actual_mode=actual_mode, run_id=self.run_id,
            duration_sec=time.monotonic() - started,
            request_count=self._request_count - requests if self._network else None,
            transferred_bytes=self._transferred_bytes - transferred if self._network else None,
            access_limited=result.status in (FetchStatus.BLOCKED, FetchStatus.CHALLENGE, FetchStatus.RATE_LIMITED))
        return result

    async def set_window(self, state: str) -> None:
        if self.headless or self.closed or not self.page:
            return
        cdp = None
        try:
            cdp = await self.ctx.new_cdp_session(self.page)
            win = await cdp.send('Browser.getWindowForTarget')
            await cdp.send('Browser.setWindowBounds', {'windowId': win['windowId'], 'bounds': {'windowState': state}})
        except Exception:
            log.debug('Не удалось изменить окно')
        finally:
            if cdp:
                try:
                    await cdp.detach()
                except Exception:
                    pass

    async def fetch(self, url: str) -> FetchResult:
        if self.fetch_mode == 'page_navigation' or not self.light_ok:
            return await self.load(url)
        measurement = self._measurement()
        try:
            raw = await asyncio.wait_for(self.page.evaluate(LIGHT_JS, [self._final_url.get(url, url),
                                                                         bool(self.sample_path) and not self._sample_written]), 35)
            if raw.get('html') and self.sample_path:
                self.sample_path.write_text(raw['html'], encoding='utf-8')
                self.sample_path.chmod(0o600)
                self._sample_written = True
            result = parse_result(raw, self.source_id, _city(url))
            if result.status in (FetchStatus.OK, FetchStatus.NO_RESULTS):
                self._final_url[url] = raw.get('url', url)
            # A restriction is a result, never a reason to make another request here.
            return self._measured(result, measurement, 'light_fetch')
        except Exception as exc:
            return self._measured(self._error(url, exc), measurement, 'light_fetch')

    async def load(self, url: str) -> FetchResult:
        measurement = self._measurement()
        code, headers = None, {}
        try:
            response = await self.page.goto(url, wait_until='domcontentloaded', timeout=30000)
            code = response.status if response else None
            headers = await response.all_headers() if response else {}
            if code in (403, 429, 439):
                # A known HTTP restriction is sufficient, even if the document cannot be inspected.
                result = parse_result(dict(status=code, retry_after=headers.get('retry-after')),
                                      self.source_id, _city(url))
                return self._measured(result, measurement, 'page_navigation')
            result = await self.read_current(url, code, headers.get('retry-after'))
            if result.status is FetchStatus.PARSE_CHANGED and code not in (403, 429, 439):
                # Wait for client rendering, without navigation or fetch retries.
                try:
                    await self.page.wait_for_selector('[data-marker="catalog-serp"]', timeout=5000)
                except Exception:
                    pass
                result = await self.read_current(url, code, headers.get('retry-after'))
            return self._measured(result, measurement, 'page_navigation')
        except Exception as exc:
            if code in (403, 429, 439):
                result = parse_result(dict(status=code, retry_after=headers.get('retry-after')),
                                      self.source_id, _city(url))
                return self._measured(result, measurement, 'page_navigation')
            return self._measured(self._error(url, exc), measurement, 'page_navigation')

    async def read_current(self, url: str | None = None, code: int | None = None,
                           retry_after: str | None = None) -> FetchResult:
        url = url or (self.page.url if self.page else '')
        try:
            raw = await self.page.evaluate(f'({INSPECT_FN})(document)')
            raw.update(status=code, retry_after=retry_after)
            result = parse_result(raw, self.source_id, _city(url))
            if result.status in (FetchStatus.OK, FetchStatus.NO_RESULTS):
                self.light_ok = True
                self._final_url[url] = self.page.url
            return result
        except Exception as exc:
            return self._error(url, exc)

    def _error(self, url: str, exc: Exception) -> FetchResult:
        log.warning('Source request failed: %s', type(exc).__name__)
        return FetchResult(FetchStatus.NETWORK_ERROR, [], self.source_id, _city(url), time.time(),
                           diagnostics={'error': type(exc).__name__})

    async def download(self, url: str) -> bytes | None:
        if self.closed or not self.ctx:
            return None
        try:
            r = await self.ctx.request.get(url, headers={'referer': 'https://www.avito.ru/'}, timeout=10000)
            return await r.body() if r.ok else None
        except Exception:
            return None


def _city(url: str) -> str:
    return urlparse(url).path.strip('/').split('/')[0]


def _to_item(r: dict) -> Item:
    href = r['href'].split('?')[0]
    full_url = f'https://www.avito.ru{href}' if href.startswith('/') else href
    parsed = urlparse(full_url)
    if parsed.scheme != 'https' or parsed.hostname not in ('www.avito.ru', 'avito.ru'):
        raise ValueError('Unexpected item URL')
    parts = parsed.path.strip('/').split('/')
    item_id = int(r['id'])
    title = r['title'].strip()
    if item_id <= 0 or not title or len(parts) < 3:
        raise ValueError('Invalid item schema')
    price = r.get('price')
    published = float(r['ts']) / 1000 if r.get('ts') else None
    if published is not None and (not math.isfinite(published) or published < 0):
        raise ValueError('Invalid timestamp')
    return Item(item_id, title, int(price) if price is not None and str(price).isdigit() else None,
                full_url, r.get('date', ''), r.get('image'), r.get('location', ''),
                r.get('description', ''), parts[1], parts[0], published=published)
