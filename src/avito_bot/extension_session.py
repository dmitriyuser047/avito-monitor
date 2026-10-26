"""Authenticated loopback bridge to an extension in the user's running Chrome.

Only a dedicated tab is exposed. Cookies/passwords are never transported or copied.
"""
import asyncio
import hmac
import json
import re
import secrets
import uuid
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit

from aiohttp import WSMsgType, web


def allowed_page(url):
    if not isinstance(url, str):
        return False
    try:
        parsed = urlsplit(url)
        return (url == 'about:blank' or parsed.scheme == 'https'
                and parsed.hostname in ('avito.ru', 'www.avito.ru')
                and not parsed.username and not parsed.password and parsed.port in (None, 443))
    except ValueError:
        return False


def pairing_token(directory):
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)
    path = directory / 'pairing-token'
    if not path.exists():
        with path.open('x') as file:
            path.chmod(0o600)
            file.write(secrets.token_urlsafe(32))
    return path.read_text().strip()


class ExtensionPage:
    def __init__(self, session):
        self.session, self.url = session, 'about:blank'
        self.listeners = {}

    def on(self, event, callback):
        self.listeners.setdefault(event, []).append(callback)

    def emit(self, event, *args):
        for callback in self.listeners.get(event, []):
            callback(*args)

    def is_closed(self):
        return not self.session.connected

    async def evaluate(self, expression, args=None):
        from .scraper import INSPECT_FN, LIGHT_JS
        if expression == LIGHT_JS:
            url, want_html = args
            if not allowed_page(url) or url == 'about:blank':
                raise ValueError('Extension fetch is restricted to Avito')
            return await self.session.request('fetch', url=url, want_html=bool(want_html))
        if INSPECT_FN in expression:
            return await self.session.request('inspect')
        raise ValueError('Unsupported extension operation')

    async def goto(self, url, **kwargs):
        if not allowed_page(url) or url == 'about:blank':
            raise ValueError('Extension navigation is restricted to Avito')
        result = await self.session.request('navigate', url=url)
        self.url = result['url']
        async def headers():
            return {'retry-after': result['retry_after']} if result.get('retry_after') is not None else {}
        return SimpleNamespace(status=result.get('status'), all_headers=headers)

    async def wait_for_function(self, *args, **kwargs):
        # A bounded local wait; Scraper reads the same document again afterwards.
        await asyncio.sleep(.5)


class ExtensionContext:
    def __init__(self, session):
        self.session = session
        self.request = SimpleNamespace(get=self.unavailable)

    async def unavailable(self, *args, **kwargs):
        raise RuntimeError('Optional browser transport is unavailable in extension mode')

    new_cdp_session = unavailable


class ExtensionBrowserSession:
    mode = 'chrome_extension'

    def __init__(self, directory: Path, port=9233):
        self.directory, self.port = directory, port
        self.user_data_dir = directory
        self.engine = 'chrome_extension'
        self.proxy_url = self.pw = None
        self.connected, self.last_error = False, None
        self.epoch = uuid.uuid4().hex
        self.lock = asyncio.Lock()
        self.ctx, self.page = ExtensionContext(self), ExtensionPage(self)
        self._runner = self._ws = None
        self._ready = asyncio.Event()
        self._pending = {}
        self._token = None

    async def start(self):
        if self._runner:
            raise RuntimeError('Extension bridge already started')
        self.page, self.ctx = ExtensionPage(self), ExtensionContext(self)
        self._token = pairing_token(self.directory)
        app = web.Application(client_max_size=2_000_000)
        app.router.add_get('/bridge', self._connect)
        self._runner = web.AppRunner(app, access_log=None, shutdown_timeout=2)
        try:
            await self._runner.setup()
            site = web.TCPSite(self._runner, '127.0.0.1', self.port)
            await site.start()
            if not self.port:
                self.port = site._server.sockets[0].getsockname()[1]
            await asyncio.wait_for(self._ready.wait(), 20)
            self.last_error = None
        except BaseException as exc:
            self.last_error = 'extension_not_connected' if isinstance(exc, TimeoutError) else type(exc).__name__
            await self.disconnect()
            raise

    async def attach(self):
        await self.start()

    async def _connect(self, request):
        if not re.fullmatch(r'chrome-extension://[a-p]{32}', request.headers.get('Origin', '')):
            raise web.HTTPForbidden()
        ws = web.WebSocketResponse(heartbeat=15, max_msg_size=2_000_000)
        await ws.prepare(request)
        try:
            hello = await ws.receive_json(timeout=5)
            if (not isinstance(hello, dict) or not isinstance(hello.get('token'), str)
                    or not hmac.compare_digest(hello['token'], self._token)
                    or self.connected or hello.get('type') != 'hello'
                    or not allowed_page(hello.get('url', ''))):
                await ws.close(code=1008)
                return ws
            self._ws, self.connected = ws, True
            self.page.url = hello['url']
            self.epoch = uuid.uuid4().hex
            self._ready.set()
            async for message in ws:
                if message.type != WSMsgType.TEXT:
                    continue
                try:
                    payload = json.loads(message.data)
                    if payload.get('type') == 'navigation':
                        if not allowed_page(payload.get('url', '')):
                            await ws.close(code=1008)
                            break
                        self.page.url = payload['url']
                        self.page.emit('framenavigated', SimpleNamespace(parent_frame=None))
                    elif payload.get('type') == 'tab_closed':
                        break
                    elif payload.get('type') == 'result':
                        future = self._pending.pop(payload.get('id'), None)
                        if future and not future.done():
                            if payload.get('error'):
                                future.set_exception(RuntimeError('Extension operation failed'))
                            else:
                                future.set_result(payload.get('result'))
                except (ValueError, TypeError, AttributeError):
                    await ws.close(code=1008)
                    break
        except (TimeoutError, ValueError, TypeError):
            await ws.close(code=1008)
        finally:
            if self._ws is ws:
                self.connected, self._ws = False, None
                self._ready.clear()
                self.page.emit('close')
                for future in self._pending.values():
                    if not future.done():
                        future.set_exception(ConnectionError('Extension disconnected'))
                self._pending.clear()
        return ws

    async def request(self, operation, **values):
        if not self.connected or self._ws is None:
            raise ConnectionError('Chrome extension is disconnected')
        key = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self._pending[key] = future
        try:
            await self._ws.send_json(dict(id=key, operation=operation, **values))
            return await asyncio.wait_for(future, 35)
        finally:
            self._pending.pop(key, None)

    async def get_avito_page(self):
        if not self.connected:
            raise ConnectionError('Chrome extension is disconnected')
        return self.page

    async def health(self):
        return dict(mode=self.mode, engine=self.engine, connected=self.connected, profile='user_chrome',
                    last_error=self.last_error, epoch=self.epoch, route='unknown')

    async def disconnect(self):
        # Leave the user's Chrome and dedicated tab open.
        self.connected = False
        if self._ws is not None:
            await self._ws.close()
        if self._runner:
            await self._runner.cleanup()
        self._runner = self._ws = None
        self._ready.clear()
