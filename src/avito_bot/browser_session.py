"""Browser ownership only. No catalog navigation, retries, cookies or source restriction policy."""
import asyncio
import importlib
import json
import os
import shutil
import sys
import uuid
from pathlib import Path
from typing import Protocol
from urllib.parse import urlsplit

import httpx

from .config import parse_proxy_url
from .runtime import InstanceLock

TAB_NAME = 'avito-monitor-dedicated-v3'


def validate_endpoint(endpoint: str) -> str:
    u = urlsplit(endpoint)
    if (u.scheme != 'http' or u.hostname != '127.0.0.1' or not u.port
            or u.username or u.password or u.query or u.fragment or u.path not in ('', '/')):
        raise ValueError('CDP_ENDPOINT must be http://127.0.0.1:<port> without credentials or a path')
    return endpoint.rstrip('/')


def validate_profile(profile: Path) -> Path:
    profile = profile.expanduser().resolve()
    defaults = [Path.home() / 'Library/Application Support/Google/Chrome',
                Path.home() / '.config/google-chrome', Path.home() / 'AppData/Local/Google/Chrome/User Data']
    if any(profile == p.resolve() or p.resolve() in profile.parents for p in defaults):
        raise ValueError('CDP requires a separate non-default Chrome user data directory')
    return profile


async def verify_local_listener(port: int) -> None:
    """Validate the actual local listener, including wildcard bindings, before CDP discovery."""
    executable = shutil.which('lsof')
    if executable:
        proc = await asyncio.create_subprocess_exec(executable, '-nP', f'-iTCP:{port}',
            '-sTCP:LISTEN', '-Fn', stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), 5)
        except BaseException:
            proc.kill()
            await proc.wait()
            raise
        addresses = [line[1:].rsplit(':', 1)[0] for line in stdout.decode().splitlines() if line.startswith('n')]
        if not addresses or any(address not in ('127.0.0.1', '[::1]') for address in addresses):
            raise ValueError('CDP listener must bind only to loopback, never wildcard/LAN')
    elif sys.platform.startswith('linux'):
        addresses = []
        for file in ('/proc/net/tcp', '/proc/net/tcp6'):
            for line in Path(file).read_text().splitlines()[1:]:
                parts = line.split()
                address, local_port = parts[1].split(':')
                if int(local_port, 16) == port and parts[3] == '0A':
                    addresses.append(address)
        if not addresses or any(a not in ('0100007F', '00000000000000000000000001000000') for a in addresses):
            raise ValueError('CDP listener must bind only to loopback')
    else:
        raise RuntimeError('Cannot verify local CDP listener; install lsof')


class BrowserSession(Protocol):
    async def start(self) -> None: ...
    async def attach(self) -> None: ...
    async def get_avito_page(self): ...
    async def health(self) -> dict: ...
    async def disconnect(self) -> None: ...


class ManagedBrowserSession:
    mode = 'managed'

    def __init__(self, profile_dir: Path, engine='playwright', channel='chrome', headless=False, proxy_url=None):
        self.profile_dir, self.engine, self.channel = profile_dir, engine, channel
        self.headless, self.proxy_url = headless, proxy_url
        base = profile_dir if engine == 'playwright' else profile_dir.with_name(profile_dir.name + '-patchright')
        self.user_data_dir = base.with_name(base.name + '-chrome') if channel == 'chrome' else base
        self.pw = self.ctx = self.page = self._lock = None
        self.connected = False
        self.last_error = None
        self.epoch = uuid.uuid4().hex
        self.lock = asyncio.Lock()

    async def start(self) -> None:
        if self.pw or self.ctx:
            raise RuntimeError('Browser already started')
        try:
            api = importlib.import_module(f'{self.engine}.async_api')
        except ImportError as exc:
            self.last_error = 'engine_not_installed'
            raise RuntimeError(f'Selected engine {self.engine} is not installed') from exc
        self.profile_dir.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        lock = InstanceLock(self.profile_dir.parent / 'browser.lock')
        try:
            lock.__enter__()
        except BlockingIOError:
            self.last_error = 'profile_in_use'
            raise RuntimeError('Another browser engine is running; stop it first') from None
        self._lock = lock
        try:
            self.user_data_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
            self.user_data_dir.chmod(0o700)
            singleton = self.user_data_dir / 'SingletonLock'
            if singleton.is_symlink():
                try:
                    pid = int(os.readlink(singleton).rsplit('-', 1)[1])
                    os.kill(pid, 0)
                except ProcessLookupError:
                    pass  # Chrome itself handles its stale singleton, without deleting the profile.
                except (ValueError, PermissionError):
                    raise RuntimeError('Cannot verify ownership of the existing Chrome profile') from None
                else:
                    raise RuntimeError('Chrome profile is already in use')
            # Managed profiles must not reopen saved URLs before the scheduler allows a check.
            prefs = self.user_data_dir / 'Default' / 'Preferences'
            if prefs.exists():
                data = json.loads(prefs.read_text())
                data.setdefault('session', {}).update(restore_on_startup=5, startup_urls=[])
                temporary = prefs.with_suffix('.monitor-tmp')
                temporary.write_text(json.dumps(data))
                temporary.chmod(0o600)
                temporary.replace(prefs)
            self.pw = await api.async_playwright().start()
            self.ctx = await self.pw.chromium.launch_persistent_context(
                str(self.user_data_dir), channel=self.channel, headless=self.headless, timeout=30000,
                proxy=parse_proxy_url(self.proxy_url) if self.proxy_url else None,
                locale='ru-RU', timezone_id='Europe/Moscow', no_viewport=True,
                args=['--window-size=1400,900', '--no-first-run', '--disable-session-crashed-bubble'])
            self.page = self.ctx.pages[0] if self.ctx.pages else await self.ctx.new_page()
            self.connected, self.last_error, self.epoch = True, None, uuid.uuid4().hex
            self.ctx.on('close', lambda *_: setattr(self, 'connected', False))
        except BaseException as exc:
            self.last_error = 'profile_or_launch_' + type(exc).__name__
            await self.disconnect()
            raise

    async def attach(self) -> None:
        await self.start()

    async def get_avito_page(self):
        if not self.connected:
            raise RuntimeError('Browser disconnected')
        return self.page

    async def health(self) -> dict:
        return dict(mode=self.mode, engine=self.engine, connected=self.connected,
                    profile='persistent', last_error=self.last_error, epoch=self.epoch,
                    route=('proxy' if self.proxy_url else 'direct') if self.mode == 'managed' else 'unknown')

    async def disconnect(self) -> None:
        try:
            try:
                if self.ctx:
                    await asyncio.wait_for(self.ctx.close(), 10)
            finally:
                if self.pw:
                    await self.pw.stop()
        finally:
            self.connected = False
            self.ctx = self.page = self.pw = None
            if self._lock:
                self._lock.__exit__(None, None, None)
                self._lock = None


class AttachedChromeSession(ManagedBrowserSession):
    mode = 'cdp_attach'

    def __init__(self, profile_dir: Path, endpoint: str, engine='playwright'):
        super().__init__(profile_dir, engine)
        self.endpoint = endpoint
        self.user_data_dir = profile_dir
        self.browser = None

    async def start(self) -> None:
        await self.attach()

    async def attach(self) -> None:
        if self.pw:
            raise RuntimeError('Browser already started')
        try:
            if self.engine != 'playwright':
                raise ValueError('cdp_attach requires explicit BROWSER_ENGINE=playwright')
            endpoint = validate_endpoint(self.endpoint)
            profile = validate_profile(self.profile_dir)
            await verify_local_listener(urlsplit(endpoint).port)
            # Local discovery only; do not follow redirects or environment proxies.
            async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=5) as client:
                response = await client.get(endpoint + '/json/version')
                response.raise_for_status()
                websocket = response.json()['webSocketDebuggerUrl']
            u = urlsplit(websocket)
            if (u.scheme != 'ws' or u.hostname != '127.0.0.1' or u.port != urlsplit(endpoint).port
                    or u.username or u.password or u.query or u.fragment
                    or not u.path.startswith('/devtools/browser/')):
                raise ValueError('Chrome advertised a non-local debugging endpoint')
            self.pw = await importlib.import_module('playwright.async_api').async_playwright().start()
            self.browser = await self.pw.chromium.connect_over_cdp(websocket, timeout=10000)
            cdp = await self.browser.new_browser_cdp_session()
            try:
                args = (await cdp.send('Browser.getBrowserCommandLine'))['arguments']
            finally:
                await cdp.detach()
            options = dict(a[2:].split('=', 1) for a in args if a.startswith('--') and '=' in a)
            if (options.get('remote-debugging-address') != '127.0.0.1'
                    or options.get('remote-debugging-port') != str(urlsplit(endpoint).port)
                    or not options.get('user-data-dir')
                    or validate_profile(Path(options['user-data-dir'])) != profile):
                raise ValueError('Chrome debugging address/port/profile does not match configuration')
            if not self.browser.contexts:
                raise RuntimeError('No Chrome context available')
            self.ctx = self.browser.contexts[0]
            # Only claim a deliberately named tab. Never repurpose the first user's Avito tab.
            saved_target = None
            target_file = profile / 'avito-monitor-target'
            if target_file.exists():
                saved_target = target_file.read_text().strip()
            for page in self.ctx.pages:
                if page.is_closed():
                    continue
                probe = await self.ctx.new_cdp_session(page)
                try:
                    target = (await probe.send('Target.getTargetInfo'))['targetInfo']['targetId']
                finally:
                    await probe.detach()
                if target == saved_target or await page.evaluate('window.name') == TAB_NAME:
                    self.page = page
                    break
            if self.page is None:
                self.page = await self.ctx.new_page()  # about:blank, no request to Avito
                await self.page.evaluate('(name) => { window.name = name; }', TAB_NAME)
            probe = await self.ctx.new_cdp_session(self.page)
            try:
                target = (await probe.send('Target.getTargetInfo'))['targetInfo']['targetId']
            finally:
                await probe.detach()
            profile.mkdir(parents=True, exist_ok=True, mode=0o700)
            target_file.write_text(target)
            target_file.chmod(0o600)
            self.connected, self.last_error, self.epoch = True, None, uuid.uuid4().hex
            self.browser.on('disconnected', lambda *_: setattr(self, 'connected', False))
        except BaseException as exc:
            self.last_error = 'cdp_' + type(exc).__name__
            await self.disconnect()
            raise

    async def disconnect(self) -> None:
        # Stopping the client transport detaches. Never Browser.close, context.close or page.close.
        try:
            if self.pw:
                await self.pw.stop()
        finally:
            self.connected = False
            self.browser = self.ctx = self.page = self.pw = None


class BrowserSessionManager:
    def __init__(self, profile_dir: Path, *, mode='managed', engine='playwright', channel='chrome',
                 headless=False, proxy_url=None, cdp_endpoint='http://127.0.0.1:9222', cdp_profile_dir=None, extension_bridge_port=9233):
        if mode == 'chrome_extension':
            from .extension_session import ExtensionBrowserSession
            self.session = ExtensionBrowserSession(profile_dir.parent / 'extension-bridge', extension_bridge_port)
            return
        if mode not in ('managed', 'cdp_attach'):
            raise ValueError('Unknown browser session mode')
        self.session = (AttachedChromeSession(cdp_profile_dir or profile_dir.parent / 'chrome-cdp-profile',
                                             cdp_endpoint, engine) if mode == 'cdp_attach'
                        else ManagedBrowserSession(profile_dir, engine, channel, headless, proxy_url))

    async def start(self):
        await self.session.start()

    async def attach(self):
        await self.session.attach()

    async def get_avito_page(self):
        return await self.session.get_avito_page()

    async def health(self):
        return await self.session.health()

    async def disconnect(self):
        await self.session.disconnect()
