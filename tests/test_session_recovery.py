"""Offline architecture regressions: SQLite faults, CDP ownership, route and request budgets."""
import asyncio
import json
import os
import socket
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from avito_bot.access_coordinator import AccessCoordinator
from avito_bot.browser_session import AttachedChromeSession, validate_endpoint, validate_profile
from avito_bot.db import DB
from avito_bot.diagnostics import startup_report
from avito_bot.direct_proxy import DirectProxy, _DNSBackend
from avito_bot.monitor import Monitor
from avito_bot.scraper import Scraper, parse_result
from avito_bot.sources import FetchStatus, Search, SourceState
from avito_bot.startup_diagnostics import StartupDiagnostics
from test_restrictions import CATALOG, catalog, config, database, respond, source, state


@pytest.mark.parametrize('failure', ['state', 'event', 'outbox', 'metrics'])
def test_confirmation_failure_rolls_back_everything_and_can_retry(tmp_path, monkeypatch, failure):
    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, catalog(100), mode='page_navigation')
        mon = Monitor(config(tmp_path), db, scraper, AsyncMock(), recipients=lambda: [1])
        await mon.poll_once()
        respond(scraper, dict(challenge=True, items=[]), code=429, headers={'retry-after': '3600'})
        db.set('next_poll:local_browser', '0')
        await mon.poll_once()
        before, facts = state(db), mon.access.get('kazan')
        respond(scraper, catalog(100, 101))
        method = {'state': 'save_source_state', 'event': 'record_event', 'outbox': 'stage_items', 'metrics': 'record_fetch'}[failure]
        original = getattr(db, method)
        def fail(*args, **kwargs):
            if failure != 'event' or args[0] == 'verification_confirmed':
                raise OSError('simulated SQLite write failure')
            return original(*args, **kwargs)
        with monkeypatch.context() as patch:
            patch.setattr(db, method, fail)
            with pytest.raises(OSError):
                await mon.poll_once()
        assert mon.verification._confirmed == set()
        assert state(db) == before and mon.access.get('kazan') == facts
        assert db.unseen([101]) == {101} and db.queue_counts() == {}
        assert db.conn.execute("SELECT count(*) FROM events WHERE kind='verification_confirmed'").fetchone()[0] == 0
        assert await mon.verification.tick()
        assert mon.access.get('kazan').verification_confirmed
        assert state(db)['server_until'] == before['server_until']
        assert db.queue_counts() == {'pending': 1}
        assert scraper.page.goto.await_count == 2
        db.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('manual', [False, True])
def test_one_catalog_cannot_clear_another_city(tmp_path, manual):
    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, dict(challenge=True, items=[]), mode='page_navigation')
        mon = Monitor(config(tmp_path), db, scraper, AsyncMock())
        await mon.poll_once()
        db.save_source_state('local_browser', 'moskva', SourceState.VERIFICATION_REQUIRED, reason='challenge')
        other = next(s for s in db.source_states('local_browser') if s['city'] == 'moskva')
        respond(scraper, catalog(100))
        if manual:
            await mon.open_verification()
        else:
            await mon.verification.tick()
        states = {s['city']: s for s in db.source_states('local_browser')}
        assert states['kazan']['state'] == SourceState.AVAILABLE
        assert states['moskva'] == other
        db.close()
    asyncio.run(scenario())


def test_wrong_query_and_connection_epoch_cannot_confirm_old_dom(tmp_path):
    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, dict(challenge=True, items=[]), mode='page_navigation')
        mon = Monitor(config(tmp_path), db, scraper, AsyncMock())
        await mon.poll_once()
        respond(scraper, catalog(100))
        scraper.page.url = CATALOG + '&q=different'
        assert not await mon.verification.tick()
        scraper.page.url = CATALOG
        scraper.run_id = 'a-new-connection'
        scraper.navigation_generation = 0
        assert not await mon.verification.tick()
        assert state(db)['state'] == SourceState.VERIFICATION_REQUIRED
        scraper._on_navigated(SimpleNamespace(parent_frame=None))
        assert await mon.verification.tick()
        db.close()
    asyncio.run(scenario())


def test_429_challenge_is_orthogonal_to_server_term_and_plain_429(tmp_path):
    db = DB(tmp_path/'db')
    access = AccessCoordinator(db, 'local_browser')
    result = parse_result(dict(status=429, challenge=True, items=[], retry_after='120'), 'local_browser', 'kazan')
    until = time.time() + 120
    access.record(result, SourceState.RATE_LIMITED, until, 'HTTP 429 IP check', 429, until)
    facts = access.get('kazan')
    assert facts.verification_required and facts.http_status == 429 and facts.rate_limit_is_server_defined
    with db.atomic():
        access.confirm('kazan', time.time())
    assert not access.get('kazan').verification_required and access.get('kazan').verification_confirmed
    assert state(db)['server_until'] == until
    plain = parse_result(dict(status=429, items=[]), 'local_browser', 'moskva')
    access.record(plain, SourceState.RATE_LIMITED, time.time()+900, 'HTTP 429', 429)
    assert not access.get('moskva').verification_required
    db.close()


@pytest.mark.parametrize('url', ['http://0.0.0.0:9222', 'http://localhost:9222', 'https://127.0.0.1:9222',
                                'http://192.168.0.5:9222', 'http://u:p@127.0.0.1:9222',
                                'http://127.0.0.1:9222/?token=secret'])
def test_nonlocal_or_sensitive_cdp_endpoints_rejected(url):
    with pytest.raises(ValueError):
        validate_endpoint(url)


def test_default_chrome_profile_is_rejected():
    with pytest.raises(ValueError):
        validate_profile(Path.home()/'Library/Application Support/Google/Chrome/Default')


def test_cdp_disconnect_never_closes_browser_or_context(tmp_path):
    async def scenario():
        session = AttachedChromeSession(tmp_path/'cdp', 'http://127.0.0.1:9222')
        session.browser = SimpleNamespace(close=AsyncMock())
        session.ctx = SimpleNamespace(close=AsyncMock())
        browser, context = session.browser, session.ctx
        session.pw = SimpleNamespace(stop=AsyncMock())
        transport = session.pw
        await session.disconnect()
        browser.close.assert_not_awaited()
        context.close.assert_not_awaited()
        transport.stop.assert_awaited_once()
    asyncio.run(scenario())


def test_cdp_collector_reuses_fresh_dom_once_and_does_not_fetch_after_navigation(tmp_path):
    async def scenario():
        scraper = source(tmp_path, dict(**catalog(100), document_started_at=time.time()))
        scraper.session_mode = 'cdp_attach'
        first = await scraper.check(Search('kazan', 'bytovaya_elektronika', CATALOG))
        assert first.status == FetchStatus.OK and first.diagnostics['actual_mode'] == 'loaded_dom'
        scraper.page.goto.assert_not_awaited()
        second = await scraper.check(Search('kazan', 'bytovaya_elektronika', CATALOG))
        assert second.status == FetchStatus.OK and second.diagnostics['actual_mode'] == 'light_fetch'
        assert scraper.page.evaluate.await_count == 2
    asyncio.run(scenario())


def test_only_one_task_owns_the_page(tmp_path):
    async def scenario():
        scraper = source(tmp_path, catalog(100), mode='page_navigation')
        entered, release = asyncio.Event(), asyncio.Event()
        original = scraper.page.goto
        async def navigation(*args, **kwargs):
            entered.set()
            await release.wait()
            return await original(*args, **kwargs)
        scraper.page.goto = navigation
        first = asyncio.create_task(scraper.load(CATALOG))
        await entered.wait()
        second = asyncio.create_task(scraper.read_current())
        await asyncio.sleep(0)
        scraper.page.evaluate.assert_not_awaited()
        release.set()
        await asyncio.gather(first, second)
    asyncio.run(scenario())


def test_passive_diagnostics_never_store_sensitive_url_or_headers(tmp_path):
    trace = StartupDiagnostics(tmp_path/'trace.json', True)
    trace.request({'requestId': '1', 'type': 'Document', 'request': {
        'url': 'https://avito.ru/private?token=secret#cookie', 'headers': {'Cookie': 'private-cookie'}}})
    trace.begin_navigation('https://avito.ru/path?password=secret')
    trace.response({'response': {'url': 'https://avito.ru/?auth=secret', 'status': 429,
                                'headers': {'Set-Cookie': 'private-cookie'}}})
    trace.finished({'requestId': '1'})
    trace.end_navigation()
    text = (tmp_path/'trace.json').read_text()
    assert all(secret not in text for secret in ('token', 'secret', 'private-cookie', '/private', 'Set-Cookie'))
    assert json.loads(text)['requests_before_navigation'] is None
    assert (tmp_path/'trace.json').stat().st_mode & 0o777 == 0o600


def test_startup_report_is_read_only_and_cdp_route_is_unknown(tmp_path):
    cfg = config(tmp_path)
    cfg.browser_session_mode, cfg.direct_interface = 'cdp_attach', 'en0'
    report = startup_report(cfg)
    assert 'Network route: unknown' in report and 'Last HTTP status: unknown' in report
    assert 'Proxy: unknown' in report
    assert 'does not control attached Chrome' in report
    assert not (tmp_path/'bot.sqlite3').exists()


def test_proxy_stop_cancels_tunnels_and_can_restart(tmp_path):
    async def scenario():
        proxy = DirectProxy('127.0.0.1', port=0)
        await proxy.start()
        reader, writer = await asyncio.open_connection('127.0.0.1', proxy.port)
        await asyncio.sleep(.01)
        assert proxy._connections
        await proxy.stop()
        assert await reader.read() == b''
        assert not proxy._connections and not proxy._resolving
        writer.close()
        await writer.wait_closed()
        await proxy.start()
        assert proxy.health()['connected']
        await proxy.stop()
    asyncio.run(scenario())


def test_doh_bootstrap_uses_the_same_bound_socket_without_os_dns(tmp_path):
    async def scenario():
        proxy = DirectProxy('127.0.0.1', port=0)
        proxy._open_bound = AsyncMock(return_value=(object(), object()))
        await _DNSBackend(proxy).connect_tcp('dns.google', 443)
        proxy._open_bound.assert_awaited_once_with('8.8.8.8', 443)
        await proxy.stop()
    asyncio.run(scenario())


def test_additive_migration_preserves_every_durable_table_and_server_term(tmp_path):
    path = tmp_path/'db'
    db = DB(path)
    db.add_subscription('3060', None, None, [])
    db.mark_seen([100])
    db.stage_items([(101, {'item': {}, 'subs': []}, [1])], time.time())
    db.record_price(100, 18000, time.time())
    db.record_event('old_event')
    until = time.time()+3600
    db.save_source_state('local_browser', 'kazan', SourceState.RATE_LIMITED, until, 'HTTP 429', 429, until)
    db.record_fetch(source_id='local_browser', city='kazan', fetched_at=time.time(), status='ok',
                    http_status=200, raw_count=1, matched_count=0, overlap_count=None, interval_sec=None,
                    cooldown_until=None, item_ids_json='[100]')
    tables = ('subscriptions', 'seen', 'items', 'notification_outbox', 'price_history', 'events',
              'source_state', 'fetch_metrics', 'source_observations')
    before = {t: db.conn.execute(f'SELECT * FROM {t}').fetchall() for t in tables}
    db.conn.execute('DROP TABLE access_status')  # emulate the pre-3.0 schema
    db.close()
    db = DB(path)
    AccessCoordinator(db, 'local_browser')
    assert {t: db.conn.execute(f'SELECT * FROM {t}').fetchall() for t in tables} == before
    assert AccessCoordinator(db, 'local_browser').get('kazan').rate_limit_until == until
    db.close()


def test_real_cdp_attachment_reconnect_and_user_tab_ownership_offline(tmp_path):
    """Launch a separate real Chrome with remote debugging; all page traffic is loopback."""
    async def scenario():
        requests = []
        async def serve(reader, writer):
            try:
                head = await reader.readuntil(b'\r\n\r\n')
                target = head.split(b' ', 2)[1].decode()
                requests.append(target)
                body = (Path(__file__).parent/'fixtures/catalog.html').read_bytes().replace(
                    b'<head>', b'<head><link rel="icon" href="data:,">')
                writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nConnection: close\r\nContent-Length: '
                             + str(len(body)).encode()+b'\r\n\r\n'+body)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
        server = await asyncio.start_server(serve, '127.0.0.1', 0)
        url = f'http://127.0.0.1:{server.sockets[0].getsockname()[1]}/kazan/bytovaya_elektronika?s=104'
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            debug_port = sock.getsockname()[1]
        profile = tmp_path/'cdp'
        from playwright.async_api import async_playwright
        driver = await async_playwright().start()
        chrome = driver.chromium.executable_path if os.environ.get('AVITO_TEST_BROWSER_CHANNEL') == 'chromium' else '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
        process = subprocess.Popen([chrome, f'--user-data-dir={profile}', f'--remote-debugging-port={debug_port}',
            '--remote-debugging-address=127.0.0.1', '--enable-automation', '--headless=new', '--no-first-run',
            '--no-default-browser-check', '--disable-background-networking',
            '--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1', 'about:blank'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        scraper = Scraper(tmp_path/'p', None, session_mode='cdp_attach',
                          cdp_endpoint=f'http://127.0.0.1:{debug_port}', cdp_profile_dir=profile)
        try:
            for attempt in range(100):
                try:
                    await scraper.start()
                    break
                except Exception:
                    if process.poll() is not None:
                        raise RuntimeError('Test Chrome exited')
                    await asyncio.sleep(.1)
            else:
                pytest.fail('CDP test Chrome did not start')
            assert scraper.page.url == 'about:blank' and requests == []
            user_page = await scraper.ctx.new_page()
            await user_page.goto(url + '&user-tab=1')
            await scraper.page.goto(url)
            target = (profile/'avito-monitor-target').read_text()
            count = len(requests)
            await scraper.stop()
            assert process.poll() is None
            await scraper.start()
            assert (profile/'avito-monitor-target').read_text() == target
            result = await scraper.check(Search('kazan', 'bytovaya_elektronika', url))
            assert result.status == FetchStatus.OK and result.diagnostics['actual_mode'] == 'loaded_dom'
            assert len(requests) == count
            assert any(p.url.endswith('&user-tab=1') for p in scraper.ctx.pages)
            assert len(scraper.ctx.pages) == 3  # original blank, dedicated tab, user's tab
            process.terminate()
            await asyncio.to_thread(process.wait, 10)
            for _ in range(30):
                if scraper.closed:
                    break
                await asyncio.sleep(.05)
            assert scraper.closed
        finally:
            await scraper.stop()
            if process.poll() is None:
                process.terminate()
                await asyncio.to_thread(process.wait, 10)
            await driver.stop()
            server.close()
            await server.wait_closed()
    asyncio.run(scenario())


@pytest.mark.parametrize('engine', ['playwright', 'patchright'])
def test_managed_start_has_no_catalog_requests_and_profile_survives_restart(tmp_path, engine):
    async def scenario():
        scraper = Scraper(tmp_path/'p', None, headless=True, browser_engine=engine,
                          browser_channel=os.environ.get('AVITO_TEST_BROWSER_CHANNEL', 'chrome'))
        try:
            await scraper.start()
            assert all(page.url == 'about:blank' for page in scraper.ctx.pages)
            assert scraper.diagnostics.summary['first_navigation'] is None
            profile = scraper.user_data_dir
            await scraper.ctx.add_cookies([dict(name='local-session', value='persisted',
                domain='fixture.test', path='/', expires=time.time()+86400)])
            await scraper.stop()
            # Restore URLs would otherwise introduce startup navigation. Use an unreachable
            # loopback URL so even a regression could not contact Avito.
            prefs = profile/'Default/Preferences'
            data = json.loads(prefs.read_text())
            data['session'] = dict(restore_on_startup=4, startup_urls=['http://127.0.0.1:1/kazan/catalog'])
            prefs.write_text(json.dumps(data))
            await scraper.start()
            assert scraper.user_data_dir == profile
            assert all(page.url == 'about:blank' for page in scraper.ctx.pages)
            assert any(c['name'] == 'local-session' and c['value'] == 'persisted' for c in await scraper.ctx.cookies())
            assert json.loads(prefs.read_text())['session']['restore_on_startup'] == 5
        finally:
            await scraper.stop()
    asyncio.run(scenario())


def test_cdp_old_dom_does_not_confirm_a_restriction_after_reconnect(tmp_path):
    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, dict(challenge=True, items=[]), mode='page_navigation')
        mon = Monitor(config(tmp_path), db, scraper, AsyncMock())
        await mon.poll_once()
        scraper.session_mode, scraper.run_id, scraper.navigation_generation = 'cdp_attach', 'reconnected', 0
        respond(scraper, dict(**catalog(100), document_started_at=time.time()-3600))
        assert not await mon.verification.tick()
        assert state(db)['state'] == SourceState.VERIFICATION_REQUIRED
        respond(scraper, dict(**catalog(100), document_started_at=time.time()+.001))
        assert await mon.verification.tick()
        scraper.page.goto.assert_awaited_once()
        db.close()
    asyncio.run(scenario())


def test_direct_proxy_dns_transport_uses_local_tls_and_preserves_http_errors(tmp_path):
    async def scenario():
        import ssl
        import httpcore
        from avito_bot.direct_proxy import _DoHClient, _BoundStream
        fixtures = Path(__file__).parent/'fixtures'
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(fixtures/'loopback-cert.pem', fixtures/'loopback-key.pem')
        async def serve(reader, writer):
            try:
                await reader.readuntil(b'\r\n\r\n')
                body = b'{"Answer":[{"type":1,"data":"127.0.0.1"}]}'
                writer.write(b'HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: '
                             + str(len(body)).encode()+b'\r\n\r\n'+body)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
        server = await asyncio.start_server(serve, '127.0.0.1', 0, ssl=tls)
        class LocalBackend(httpcore.AsyncNetworkBackend):
            async def connect_tcp(self, *args, **kwargs):
                reader, writer = await asyncio.open_connection('127.0.0.1', server.sockets[0].getsockname()[1])
                return _BoundStream(reader, writer)
        proxy = DirectProxy('127.0.0.1', port=0)
        await proxy._http.aclose()
        # The certificate is test-only. Production uses default CA verification for dns.google.
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        proxy._http.pool = httpcore.AsyncConnectionPool(network_backend=LocalBackend(), ssl_context=context)
        try:
            assert await proxy.resolve('fixture.test') == '127.0.0.1'
        finally:
            await proxy.stop()
            server.close()
            await server.wait_closed()
    asyncio.run(scenario())


def test_new_429_check_after_confirmed_server_term_can_be_confirmed_again(tmp_path):
    async def scenario():
        from test_restrictions import expire
        db = database(tmp_path)
        scraper = source(tmp_path, dict(challenge=True, items=[]), mode='page_navigation', code=429,
                         headers={'retry-after': '3600'})
        mon = Monitor(config(tmp_path), db, scraper, AsyncMock())
        await mon.poll_once()
        respond(scraper, catalog(100))
        assert await mon.verification.tick()
        assert state(db)['state'] == SourceState.RATE_LIMITED
        expire(db)
        respond(scraper, dict(challenge=True, items=[]), code=429, headers={'retry-after': '3600'})
        assert await mon.poll_once()
        respond(scraper, catalog(100))
        assert await mon.verification.tick()
        assert db.conn.execute("SELECT count(*) FROM events WHERE kind='verification_confirmed'").fetchone()[0] == 2
        db.close()
    asyncio.run(scenario())


def test_confirming_one_city_keeps_other_city_rate_limit_and_backoff_history(tmp_path):
    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, dict(challenge=True, items=[]), mode='page_navigation', code=429)
        mon = Monitor(config(tmp_path), db, scraper, AsyncMock())
        await mon.poll_once()
        other = parse_result(dict(status=429, items=[]), 'local_browser', 'moskva')
        await mon.ingest(other)
        before = next(s for s in db.source_states('local_browser') if s['city'] == 'moskva')
        streak = db.get('rate_limit_streak:local_browser')
        respond(scraper, catalog(100))
        assert await mon.verification.tick()
        assert next(s for s in db.source_states('local_browser') if s['city'] == 'moskva') == before
        assert db.get('rate_limit_streak:local_browser') == streak
        db.close()
    asyncio.run(scenario())
