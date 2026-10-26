"""Current-Chrome bridge tests. Only loopback and intercepted fixture pages are used."""
import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock

import aiohttp
import pytest

from avito_bot.extension_session import ExtensionBrowserSession, allowed_page, pairing_token
from avito_bot.extension_setup import prepare_extension
from avito_bot.scraper import Scraper
from avito_bot.sources import FetchStatus

ORIGIN = 'chrome-extension://' + 'a'*32
URL = 'https://www.avito.ru/kazan/bytovaya_elektronika?s=104'


@pytest.mark.parametrize('url,allowed', [(URL,True),('about:blank',True),('https://avito.ru/',True),
    ('https://avito.ru.evil.test/',False),('http://avito.ru/',False),
    ('https://user:secret@avito.ru/',False),('https://avito.ru:8888/',False),('file:///etc/passwd',False)])
def test_bridge_limits_page_origins(url, allowed):
    assert allowed_page(url) is allowed


def test_extension_packaging_scopes_permissions_and_keeps_pairing_private(tmp_path):
    extension = prepare_extension(tmp_path)
    manifest = json.loads((extension/'manifest.json').read_text())
    assert set(manifest['host_permissions']) == {'https://www.avito.ru/*','https://avito.ru/*'}
    assert 'debugger' not in manifest['permissions'] and 'cookies' not in manifest['permissions']
    token = pairing_token(tmp_path/'extension-bridge')
    assert token in (extension/'settings.js').read_text()
    assert pairing_token(tmp_path/'extension-bridge') == token
    assert (extension/'settings.js').stat().st_mode & 0o777 == 0o600
    assert (tmp_path/'extension-bridge/pairing-token').stat().st_mode & 0o777 == 0o600
    assert 'performance.timeOrigin' in (extension/'inspector.js').read_text()


async def listening(session):
    for _ in range(100):
        if session._runner and session.port:
            try:
                r,w=await asyncio.open_connection('127.0.0.1',session.port)
                w.close();await w.wait_closed()
                return
            except OSError:
                pass
        await asyncio.sleep(.01)
    raise AssertionError('Bridge did not listen')


def test_bridge_rejects_web_origin_and_bad_token_and_routes_only_one_connection(tmp_path):
    async def scenario():
        session=ExtensionBrowserSession(tmp_path,0)
        start=asyncio.create_task(session.start())
        try:
            await listening(session)
            async with aiohttp.ClientSession() as client:
                endpoint=f'http://127.0.0.1:{session.port}/bridge'
                with pytest.raises(aiohttp.WSServerHandshakeError) as error:
                    await client.ws_connect(endpoint,headers={'Origin':'https://evil.test'})
                assert error.value.status == 403
                bad=await client.ws_connect(endpoint,headers={'Origin':ORIGIN})
                await bad.send_json(dict(type='hello',token='wrong',url=URL))
                assert (await bad.receive()).type in (aiohttp.WSMsgType.CLOSE,aiohttp.WSMsgType.CLOSED)
                assert not session.connected
                ws=await client.ws_connect(endpoint,headers={'Origin':ORIGIN})
                await ws.send_json(dict(type='hello',token=pairing_token(tmp_path),url=URL))
                await start
                assert session.connected and session.page.url == URL
                other=await client.ws_connect(endpoint,headers={'Origin':ORIGIN})
                await other.send_json(dict(type='hello',token=pairing_token(tmp_path),url=URL))
                assert (await other.receive()).type == aiohttp.WSMsgType.CLOSE
                assert session.connected
                read=asyncio.create_task(session.request('inspect'))
                command=await ws.receive_json()
                await ws.send_json(dict(type='result',id=command['id'],result={'catalog':True,'items':[]}))
                assert (await read)['catalog']
                pending=asyncio.create_task(session.request('inspect'))
                await ws.receive_json()
                await ws.close()
                with pytest.raises(ConnectionError):
                    await pending
                assert not session.connected
        finally:
            await session.disconnect()
            if not start.done():start.cancel()
            await asyncio.gather(start,return_exceptions=True)
    asyncio.run(scenario())


def test_extension_cannot_send_arbitrary_evaluation_or_external_navigation(tmp_path):
    async def scenario():
        session=ExtensionBrowserSession(tmp_path)
        session.request=AsyncMock()
        with pytest.raises(ValueError):
            await session.page.evaluate('document.cookie')
        with pytest.raises(ValueError):
            await session.page.goto('https://evil.test/')
        session.request.assert_not_awaited()
    asyncio.run(scenario())


def test_real_extension_collects_in_existing_browser_and_disconnect_leaves_it_open(tmp_path):
    async def scenario():
        from playwright.async_api import async_playwright
        scraper=Scraper(tmp_path/'p',None,session_mode='chrome_extension',extension_bridge_port=0)
        session=scraper.session_manager.session
        session.connect_timeout=.05
        with pytest.raises(TimeoutError):
            await scraper.start()
        session.connect_timeout=20
        extension=prepare_extension(tmp_path,session.port)
        async with async_playwright() as driver:
            ctx=await driver.chromium.launch_persistent_context(str(tmp_path/'test-chrome'),headless=True,
                channel='chromium',args=[f'--disable-extensions-except={extension}',f'--load-extension={extension}'])
            requests=[]
            fixtures=Path(__file__).parent/'fixtures'
            selected={'name':'catalog','status':200}
            async def route(route):
                if route.request.url.startswith('https://www.avito.ru/'):
                    requests.append(route.request.url)
                    await route.fulfill(status=selected['status'],body=(fixtures/(selected['name']+'.html')).read_text(),
                        headers={'content-type':'text/html; charset=utf-8','retry-after':'120'})
                else:
                    await route.fulfill(status=200,body='',content_type='text/plain')
            await ctx.route('https://**/*',route)
            start=asyncio.create_task(scraper.start())
            try:
                session=scraper.session_manager.session
                await listening(session)
                worker=ctx.service_workers[0] if ctx.service_workers else await ctx.wait_for_event('serviceworker')
                extension_id=worker.url.split('/')[2]
                popup=await ctx.new_page()
                await popup.goto(f'chrome-extension://{extension_id}/popup.html')
                await popup.locator('#connect').click()
                await start
                assert requests == [] and scraper.page.url == 'about:blank'
                user_page=await ctx.new_page()
                await user_page.goto(URL+'&user-tab=1')
                result=await scraper.fetch(URL)
                assert result.status == FetchStatus.OK, result
                assert result.http_status == 200
                count=len(requests)
                current=await scraper.read_current()
                assert current.status == FetchStatus.OK and len(requests) == count
                light=await scraper.fetch(URL)
                assert light.status == FetchStatus.OK
                assert light.diagnostics['actual_mode'] == 'light_fetch'
                # Extension mode has no CDP; the document size still comes from Resource Timing.
                assert light.diagnostics['transferred_bytes'] is None
                assert light.diagnostics['document_decoded_bytes'] == len((fixtures/'catalog.html').read_bytes())
                scraper.fetch_mode='page_navigation'
                selected.update(name='challenge',status=429)
                limited=await scraper.fetch(URL)
                assert limited.status == FetchStatus.RATE_LIMITED
                assert limited.retry_after_sec == 120
                assert limited.diagnostics['verification_page'] is True
                assert user_page.url.endswith('&user-tab=1')
                await scraper.stop()
                assert not user_page.is_closed()
                assert any(page.url == URL for page in ctx.pages)
                # Reconnect uses the existing dedicated tab and sends no catalog request.
                before=len(requests)
                start=asyncio.create_task(scraper.start())
                await listening(session)
                await popup.locator('#connect').click()
                await start
                assert len(requests) == before
                assert len([page for page in ctx.pages if page.url == URL]) == 1
                assert scraper.page.url == URL
            finally:
                await scraper.stop()
                if not start.done():start.cancel()
                await asyncio.gather(start,return_exceptions=True)
                await ctx.close()
    asyncio.run(scenario())


def test_late_extension_connection_survives_startup_timeout_and_is_adopted(tmp_path):
    async def scenario():
        from avito_bot.config import load_config
        from avito_bot.db import DB
        from avito_bot.monitor import Monitor
        scraper=Scraper(tmp_path/'p',None,session_mode='chrome_extension',extension_bridge_port=0)
        session=scraper.session_manager.session
        session.connect_timeout=.05
        with pytest.raises(TimeoutError):
            await scraper.start()
        assert session._runner is not None
        cfg=load_config();cfg.data_dir=tmp_path
        db=DB(tmp_path/'db')
        monitor=Monitor(cfg,db,scraper,AsyncMock())
        monitor.source_failed('startup timeout')
        monitor._restart_at=asyncio.get_running_loop().time()+600
        async with aiohttp.ClientSession() as client:
            async with client.ws_connect(f'http://127.0.0.1:{session.port}/bridge',headers={'Origin':ORIGIN}) as ws:
                await ws.send_json(dict(type='hello',token=pairing_token(tmp_path/'extension-bridge'),url=URL))
                await session._ready.wait()
                assert await monitor._ensure_browser()
                assert session.connected and monitor.source_down is None
                assert not scraper.closed and session._runner is not None
                assert len(session.page.listeners['framenavigated']) == 1
                # Re-adopting the connected extension is idempotent and creates no tab/request.
                await scraper.start()
                assert len(session.page.listeners['framenavigated']) == 1
        await scraper.stop()
        assert session._runner is None
        db.close()
    asyncio.run(scenario())
