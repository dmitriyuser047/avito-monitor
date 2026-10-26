"""External proxy (mobile port, residential): settings, separate identity and the daily traffic limit."""
import asyncio
import base64
import os
import time
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from avito_bot.config import load_config, parse_proxy_url
from avito_bot.db import DB
from avito_bot.monitor import Monitor
from avito_bot.scraper import Scraper
from avito_bot.sources import FetchResult, FetchStatus

FIXTURES = Path(__file__).with_name('fixtures')


@pytest.fixture
def clean_env(monkeypatch):
    # A developer's .env (extension mode, DIRECT_INTERFACE) must not leak into these scenarios.
    monkeypatch.setattr('avito_bot.config._load_dotenv', lambda _: None)
    for key in ('PROXY_URL', 'DIRECT_INTERFACE', 'BROWSER_SESSION_MODE', 'DAILY_TRAFFIC_LIMIT_MB'):
        monkeypatch.delenv(key, raising=False)
    return monkeypatch


def test_proxy_url_becomes_playwright_settings():
    assert parse_proxy_url('http://user%40x:p%3Ass@mobile.example:8000') == dict(
        server='http://mobile.example:8000', username='user@x', password='p:ss')
    assert parse_proxy_url('socks5://10.0.0.1:1080') == dict(server='socks5://10.0.0.1:1080')
    with pytest.raises(ValueError, match='SOCKS5'):
        parse_proxy_url('socks5://user:pass@10.0.0.1:1080')
    for bad in ('mobile.example:8000', 'ftp://h:1', 'http://h', 'http://h:1/path', 'http://h:99999'):
        with pytest.raises(ValueError):
            parse_proxy_url(bad)


def test_proxy_gets_its_own_identity_and_hides_the_password(clean_env):
    clean_env.setenv('PROXY_URL', 'http://login:secret@mobile.example:8000')
    clean_env.setenv('DAILY_TRAFFIC_LIMIT_MB', '500')
    cfg = load_config()
    assert cfg.source_id == 'proxy_browser' and cfg.browser_profile_dir.name == 'browser-profile-proxy'
    assert cfg.daily_traffic_limit_mb == 500
    assert 'secret' not in repr(cfg)
    clean_env.delenv('PROXY_URL')
    home = load_config()
    assert home.source_id == 'local_browser' and home.browser_profile_dir.name == 'browser-profile'


@pytest.mark.parametrize('key,value', [('DIRECT_INTERFACE', 'en0'), ('BROWSER_SESSION_MODE', 'chrome_extension')])
def test_proxy_conflicts_are_rejected(clean_env, key, value):
    clean_env.setenv('PROXY_URL', 'http://login:secret@mobile.example:8000')
    clean_env.setenv(key, value)
    with pytest.raises(ValueError):
        load_config()


def test_negative_traffic_limit_is_rejected(clean_env):
    clean_env.setenv('DAILY_TRAFFIC_LIMIT_MB', '-1')
    with pytest.raises(ValueError):
        load_config()


class SizedSource:
    source_id = 'proxy_browser'
    closed = False

    def __init__(self, transferred=None, document=None):
        self.calls, self.transferred, self.document = [], transferred, document

    async def fetch(self, url):
        self.calls.append(url)
        return FetchResult(FetchStatus.NO_RESULTS, [], self.source_id, url.split('/')[3], time.time(), 200,
                           diagnostics=dict(transferred_bytes=self.transferred, document_bytes=self.document))


@pytest.fixture
def env(tmp_path, clean_env):
    cfg = load_config()
    cfg.data_dir, cfg.city, cfg.night_hours, cfg.requests_per_minute = tmp_path, 'kazan', (0, 0), 60
    cfg.daily_traffic_limit_mb = 1
    db = DB(tmp_path/'x.sqlite3')
    db.add_subscription('3060', None, None, [], 'gpu', ['kazan'])
    yield cfg, db
    db.close()


def test_daily_limit_pauses_collection_and_photos_until_midnight(env):
    async def scenario():
        cfg, db = env
        source, alert = SizedSource(transferred=600_000), AsyncMock()
        monitor = Monitor(cfg, db, source, alert)
        assert await monitor.poll_once()
        assert monitor.traffic_today() == 600_000 and not monitor.traffic_exhausted()
        db.set('next_poll:proxy_browser', '0')
        assert await monitor.poll_once()
        assert monitor.traffic_exhausted()
        db.set('next_poll:proxy_browser', '0')
        assert not await monitor.poll_once() and not await monitor.poll_once()
        assert len(source.calls) == 2
        assert monitor.status.startswith('PAUSED') and not monitor.photos_allowed()
        # One alert per day, not one per scheduler tick.
        texts = [c.args[0] for c in alert.await_args_list if c.args[0].startswith('Дневной лимит')]
        assert len(texts) == 1 and '1 МБ' in texts[0]
        assert 'трафик сегодня: 1.2 МБ из 1 МБ' in monitor.health_text()
        # A new day starts from zero.
        assert not monitor.traffic_exhausted(time.time() + 86400)
    asyncio.run(scenario())


def test_document_size_counts_when_cdp_is_unavailable(env):
    async def scenario():
        cfg, db = env
        cfg.daily_traffic_limit_mb = 0
        monitor = Monitor(cfg, db, SizedSource(document=280_000), AsyncMock())
        assert await monitor.poll_once()
        assert monitor.traffic_today() == 280_000 and not monitor.traffic_exhausted()
        monitor.add_traffic(None)
        monitor.add_traffic(-5)
        assert monitor.traffic_today() == 280_000
    asyncio.run(scenario())


@pytest.mark.parametrize('engine', ['playwright', 'patchright'])
def test_real_browser_authenticates_to_the_proxy(tmp_path, engine):
    """A loopback proxy that demands a login, as mobile ports do; no external destinations."""
    pytest.importorskip(f'{engine}.async_api')
    expected = 'Basic ' + base64.b64encode(b'mobile:s3cret').decode()
    seen = []

    async def serve(reader, writer):
        try:
            head = (await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), 5)).decode('latin-1')
            target = head.split(' ', 2)[1]
            auth = next((line.split(':', 1)[1].strip() for line in head.split('\r\n')
                         if line.lower().startswith('proxy-authorization:')), None)
            if auth != expected:
                seen.append(('407', target))
                writer.write(b'HTTP/1.1 407 Proxy Authentication Required\r\n'
                             b'Proxy-Authenticate: Basic realm="port"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
            else:
                seen.append(('ok', target))
                name = 'catalog' if '/kazan/' in target else 'empty'
                body = (FIXTURES/f'{name}.html').read_bytes()
                writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nConnection: close\r\n'
                             b'Content-Length: ' + str(len(body)).encode() + b'\r\n\r\n' + body)
            await writer.drain()
        finally:
            writer.close()

    async def scenario():
        server = await asyncio.start_server(serve, '127.0.0.1', 0)
        port = server.sockets[0].getsockname()[1]
        scraper = Scraper(tmp_path/'p', f'http://mobile:s3cret@127.0.0.1:{port}', headless=True,
                          browser_engine=engine, source_id='proxy_browser',
                          browser_channel=os.environ.get('AVITO_TEST_BROWSER_CHANNEL', 'chrome'))
        url = 'http://fixture.test/kazan/bytovaya_elektronika?s=104'
        try:
            await scraper.start()
            first = await scraper.fetch(url)
            light = await scraper.fetch(url)
            for result in (first, light):
                assert result.status is FetchStatus.OK and result.source_id == 'proxy_browser'
            assert ('ok', url) in seen
            # Every catalog request reached the "network" only through the authenticated proxy.
            assert sum(1 for state, target in seen if state == 'ok' and target == url) == 2
        finally:
            await scraper.stop()
            server.close()
            await server.wait_closed()
    asyncio.run(scenario())


def test_budget_lowered_on_home_ip_does_not_cap_the_proxy(env):
    cfg, db = env
    db.set('rpm', '1')  # saved on the home IP
    cfg.requests_per_minute = cfg.night_requests_per_minute = 6
    assert Monitor(cfg, db, SizedSource(), AsyncMock()).budget() == 6
    db.set('rpm:proxy_browser', '4')
    assert Monitor(cfg, db, SizedSource(), AsyncMock()).budget() == 4
