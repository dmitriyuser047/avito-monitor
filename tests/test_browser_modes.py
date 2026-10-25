"""Engine/mode regressions. All browser traffic uses fixtures or loopback mocks."""
import asyncio
import importlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from avito_bot.config import load_config
from avito_bot.scraper import Scraper
from avito_bot.sources import FetchStatus

FIXTURES = Path(__file__).with_name('fixtures')
URL = 'https://fixture.test/kazan/bytovaya_elektronika?s=104'


def fake_api(monkeypatch, launch_error=None):
    page = SimpleNamespace(on=Mock(), is_closed=Mock(return_value=False))
    cdp = SimpleNamespace(on=Mock(), send=AsyncMock(return_value={'windowId': 1}), detach=AsyncMock())
    ctx = SimpleNamespace(pages=[page], on=Mock(), close=AsyncMock(), new_cdp_session=AsyncMock(return_value=cdp))
    launch = AsyncMock(return_value=ctx, side_effect=launch_error)
    driver = SimpleNamespace(chromium=SimpleNamespace(launch_persistent_context=launch), stop=AsyncMock())
    start = AsyncMock(return_value=driver)
    api = SimpleNamespace(async_playwright=lambda: SimpleNamespace(start=start))
    imported = Mock(return_value=api)
    monkeypatch.setattr('avito_bot.scraper.importlib.import_module', imported)
    return SimpleNamespace(page=page, cdp=cdp, ctx=ctx, launch=launch, driver=driver, start=start, imported=imported)


@pytest.mark.parametrize('engine', ['playwright', 'patchright'])
@pytest.mark.parametrize('channel', ['chrome', 'chromium'])
def test_selected_engine_profile_proxy_cdp_and_close(tmp_path, monkeypatch, engine, channel):
    async def scenario():
        api = fake_api(monkeypatch)
        scraper = Scraper(tmp_path/'profile', 'http://127.0.0.1:8899', browser_engine=engine,
                          browser_channel=channel)
        await scraper.start()
        api.imported.assert_called_once_with(f'{engine}.async_api')
        api.launch.assert_awaited_once()
        args, opts = api.launch.await_args
        name = 'profile' + ('-patchright' if engine == 'patchright' else '') + ('-chrome' if channel == 'chrome' else '')
        assert args == (str(tmp_path/name),)
        assert opts['channel'] == channel and not opts['headless']
        assert opts['proxy'] == {'server': 'http://127.0.0.1:8899'}
        assert opts['no_viewport'] is True
        assert any(call.args[0] == 'Network.enable' for call in api.cdp.send.await_args_list)
        assert any(call.args[0] == 'Browser.setWindowBounds' for call in api.cdp.send.await_args_list)
        # Real close handlers accept both event signatures.
        api.page.on.call_args.args[1]()
        assert scraper.closed
        await scraper.stop()
        api.ctx.close.assert_awaited_once()
        api.driver.stop.assert_awaited_once()
    asyncio.run(scenario())


@pytest.mark.parametrize('engine', ['playwright', 'patchright'])
def test_missing_engine_fails_without_loading_another(tmp_path, monkeypatch, engine):
    async def scenario():
        imported = Mock(side_effect=ModuleNotFoundError(engine))
        monkeypatch.setattr('avito_bot.scraper.importlib.import_module', imported)
        with pytest.raises(RuntimeError, match='not installed'):
            await Scraper(tmp_path/'p', None, browser_engine=engine).start()
        imported.assert_called_once_with(f'{engine}.async_api')
        assert not (tmp_path/'p').exists()
    asyncio.run(scenario())


@pytest.mark.parametrize('engine', ['playwright', 'patchright'])
@pytest.mark.parametrize('error', [PermissionError('profile denied'), RuntimeError('profile locked'),
                                   RuntimeError('executable missing')])
def test_launch_failure_never_switches_browser_or_profile(tmp_path, monkeypatch, engine, error):
    async def scenario():
        api = fake_api(monkeypatch, error)
        scraper = Scraper(tmp_path/'p', None, browser_engine=engine)
        with pytest.raises(type(error)):
            await scraper.start()
        api.launch.assert_awaited_once()
        api.driver.stop.assert_awaited_once()
        assert scraper.closed
        # Failure releases the common lock, including when Chrome is absent.
        api.launch.side_effect = None
        other = Scraper(tmp_path/'p', None, browser_engine=engine)
        await other.start()
        await other.stop()
    asyncio.run(scenario())


def test_two_engines_cannot_start_together(tmp_path, monkeypatch):
    async def scenario():
        api = fake_api(monkeypatch)
        first = Scraper(tmp_path/'p', None)
        second = Scraper(tmp_path/'p', None, browser_engine='patchright')
        await first.start()
        with pytest.raises(RuntimeError, match='Another browser engine'):
            await second.start()
        assert api.launch.await_count == 1
        await second.stop()  # Must not release the first instance's lock.
        with pytest.raises(RuntimeError, match='Another browser engine'):
            await second.start()
        await first.stop()
        await second.start()
        await second.stop()
    asyncio.run(scenario())


def test_profile_directory_denied_releases_lock(tmp_path, monkeypatch):
    async def scenario():
        api = fake_api(monkeypatch)
        scraper = Scraper(tmp_path/'p', None)
        original = Path.mkdir
        def mkdir(path, *args, **kwargs):
            if path == scraper.user_data_dir:
                raise PermissionError('denied')
            return original(path, *args, **kwargs)
        with monkeypatch.context() as patch:
            patch.setattr(Path, 'mkdir', mkdir)
            with pytest.raises(PermissionError):
                await scraper.start()
        api.start.assert_not_awaited()
        await scraper.start()
        await scraper.stop()
    asyncio.run(scenario())


@pytest.mark.parametrize('mode', ['light_fetch', 'page_navigation'])
@pytest.mark.parametrize(('code', 'challenge', 'expected'), [(403, False, FetchStatus.BLOCKED),
                         (429, False, FetchStatus.RATE_LIMITED), (200, True, FetchStatus.CHALLENGE)])
def test_restrictions_do_not_trigger_extra_navigation(tmp_path, mode, code, challenge, expected):
    async def scenario():
        scraper = Scraper(tmp_path/'p', None, fetch_mode=mode)
        scraper.light_ok = True
        raw = dict(status=code, items=[], challenge=challenge, retry_after='300')
        response = SimpleNamespace(status=code, all_headers=AsyncMock(return_value={'retry-after': '300'}))
        scraper.page = SimpleNamespace(evaluate=AsyncMock(return_value=raw), goto=AsyncMock(return_value=response),
                                       wait_for_selector=AsyncMock(), url=URL)
        result = await scraper.fetch(URL)
        assert result.status is expected and result.retry_after_sec == 300
        assert result.diagnostics['access_limited'] is True
        assert result.diagnostics['duration_sec'] >= 0
        assert result.diagnostics['request_count'] is None
        assert scraper.page.goto.await_count == int(mode == 'page_navigation')
        scraper.page.wait_for_selector.assert_not_awaited()
        assert scraper.page.evaluate.await_count == int(mode == 'light_fetch' or code == 200)
    asyncio.run(scenario())


def test_cdp_failure_does_not_break_collection(tmp_path, monkeypatch):
    async def scenario():
        api = fake_api(monkeypatch)
        api.ctx.new_cdp_session.side_effect = RuntimeError('unsupported')
        scraper = Scraper(tmp_path/'p', None, headless=True)
        await scraper.start()
        api.page.evaluate = AsyncMock(return_value=dict(status=200, catalog=True, empty=True, items=[]))
        scraper.light_ok = True
        result = await scraper.fetch(URL)
        assert result.status is FetchStatus.NO_RESULTS
        assert result.diagnostics['request_count'] is None
        await scraper.stop()
    asyncio.run(scenario())


@pytest.mark.parametrize(('key','value'), [('BROWSER_ENGINE','unknown'),('FETCH_MODE','unknown'),('BROWSER_CHANNEL','unknown')])
def test_invalid_settings_rejected(monkeypatch, key, value):
    monkeypatch.setenv(key, value)
    with pytest.raises(ValueError):
        load_config()


def test_default_settings_and_independent_profiles(monkeypatch, tmp_path):
    # Prevent machine-specific .env from affecting the defaults under test.
    monkeypatch.setattr('avito_bot.config._load_dotenv', lambda _: None)
    for key in ('BROWSER_ENGINE', 'FETCH_MODE', 'BROWSER_CHANNEL'):
        monkeypatch.delenv(key, raising=False)
    cfg = load_config()
    assert (cfg.browser_engine, cfg.fetch_mode, cfg.browser_channel) == ('playwright', 'light_fetch', 'chrome')
    first = Scraper(tmp_path/'p', None)
    second = Scraper(tmp_path/'p', None, browser_engine='patchright')
    assert first.user_data_dir != second.user_data_dir
    assert first.source_id == second.source_id == 'local_browser'


@pytest.mark.parametrize('engine', ['playwright', 'patchright'])
def test_actual_engine_both_modes_offline_and_restart(tmp_path, engine):
    pytest.importorskip(f'{engine}.async_api')
    async def scenario():
        scraper = Scraper(tmp_path/'p', None, headless=True, browser_engine=engine,
                          browser_channel=os.environ.get('AVITO_TEST_BROWSER_CHANNEL', 'chrome'))
        try:
            await scraper.start()
            requests = []
            async def route(req):
                requests.append(req.request.url)
                name = 'catalog' if req.request.url.startswith('https://fixture.test/') else 'empty'
                await req.fulfill(status=200, body=(FIXTURES/f'{name}.html').read_text(), content_type='text/html')
            await scraper.ctx.route('**/*', route)
            await scraper.set_window('normal')
            # First light_fetch check navigates once, subsequent checks run LIGHT_JS.
            warm = await scraper.fetch(URL)
            light = await scraper.fetch(URL)
            scraper.fetch_mode = 'page_navigation'
            nav = await scraper.fetch(URL)
            for result in (warm, light, nav):
                assert result.status is FetchStatus.OK
                assert result.items[0].id == 123456 and result.items[0].published == 1792830000
                assert result.diagnostics['browser_engine'] == engine
                assert result.diagnostics['request_count'] >= 1
                assert result.diagnostics['transferred_bytes'] >= 0
            assert warm.diagnostics['actual_mode'] == nav.diagnostics['actual_mode'] == 'page_navigation'
            assert light.diagnostics['actual_mode'] == 'light_fetch'
            assert requests.count(URL) == 3
            # APIRequestContext isn't covered by page routing: mock its transport.
            scraper.ctx.request.get = AsyncMock(return_value=SimpleNamespace(ok=True, body=AsyncMock(return_value=b'photo')))
            assert await scraper.download('https://fixture.test/photo') == b'photo'
            await scraper.page.close()
            assert scraper.closed
            await scraper.stop()
            await scraper.start()
            assert not scraper.closed and not scraper.light_ok
        finally:
            await scraper.stop()
    asyncio.run(scenario())


@pytest.mark.parametrize('engine', ['playwright', 'patchright'])
def test_actual_engine_direct_proxy_and_request_api_on_loopback(tmp_path, monkeypatch, engine):
    """Real TLS + CONNECT, including ctx.request; no DNS or remote fixture server."""
    api = pytest.importorskip(f'{engine}.async_api')
    import ssl
    from avito_bot.direct_proxy import DirectProxy

    async def scenario():
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(FIXTURES/'loopback-cert.pem', FIXTURES/'loopback-key.pem')
        received = []
        async def serve(reader, writer):
            try:
                headers = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), 5)
                target = headers.split(b' ', 2)[1].decode()
                received.append(target)
                body = b'photo' if target == '/photo' else (FIXTURES/'catalog.html').read_bytes()
                writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nConnection: close\r\nContent-Length: '
                             + str(len(body)).encode()+b'\r\n\r\n'+body)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
        server = await asyncio.start_server(serve, '127.0.0.1', 0, ssl=tls)
        port = server.sockets[0].getsockname()[1]
        proxy = DirectProxy('127.0.0.1', port=0)
        async def resolve(host):
            if host != 'fixture.test':
                raise OSError('Test forbids external destinations')
            return '127.0.0.1'
        proxy.resolve = resolve
        await proxy.start()
        proxy.port = proxy._server.sockets[0].getsockname()[1]
        # Trust the public test-only self-signed certificate only in this context.
        factory = api.async_playwright
        async def start():
            driver = await factory().start()
            launch = driver.chromium.launch_persistent_context
            async def local_launch(*args, **kwargs):
                return await launch(*args, **kwargs, ignore_https_errors=True)
            driver.chromium.launch_persistent_context = local_launch
            return driver
        monkeypatch.setattr(api, 'async_playwright', lambda: SimpleNamespace(start=start))
        scraper = Scraper(tmp_path/'p', proxy.url, headless=True, browser_engine=engine,
                          browser_channel=os.environ.get('AVITO_TEST_BROWSER_CHANNEL', 'chrome'))
        url = f'https://fixture.test:{port}/kazan/catalog'
        try:
            await scraper.start()
            first = await scraper.fetch(url)
            light = await scraper.fetch(url)
            scraper.fetch_mode = 'page_navigation'
            navigation = await scraper.fetch(url)
            for result in (first, light, navigation):
                assert result.status is FetchStatus.OK
                assert result.diagnostics['request_count'] >= 1
                assert result.diagnostics['transferred_bytes'] > 0
            assert await scraper.download(f'https://fixture.test:{port}/photo') == b'photo'
            assert received.count('/kazan/catalog') == 3 and received.count('/photo') == 1
        finally:
            await scraper.stop()
            await proxy.stop()
            server.close()
            await server.wait_closed()
    asyncio.run(scenario())


@pytest.mark.parametrize('code', [403,429,439])
def test_known_http_restriction_survives_broken_response_headers(tmp_path, code):
    async def scenario():
        source = Scraper(tmp_path/'p', None, fetch_mode='page_navigation')
        response = SimpleNamespace(status=code, all_headers=AsyncMock(side_effect=RuntimeError('closed')))
        source.page = SimpleNamespace(goto=AsyncMock(return_value=response), evaluate=AsyncMock())
        result = await source.fetch(URL)
        assert result.http_status == code and result.diagnostics['access_limited']
        source.page.evaluate.assert_not_awaited()
        source.page.goto.assert_awaited_once()
    asyncio.run(scenario())
