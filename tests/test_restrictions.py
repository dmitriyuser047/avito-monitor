"""End-to-end recovery scenarios A-H: no user pause, no /resume, restrictions always binding."""
import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from avito_bot.config import load_config
from avito_bot.db import DB
from avito_bot.delivery import DeliveryWorker
from avito_bot.monitor import Monitor
from avito_bot.photo_cache import PhotoCache
from avito_bot.scraper import Scraper
from avito_bot.sources import SourceState

CATALOG = 'https://www.avito.ru/kazan/bytovaya_elektronika?s=104'


def raw_item(item_id):
    return dict(id=item_id, title='RTX 3060 Ti', price='18000',
                href=f'/kazan/tovary_dlya_kompyutera/gpu_{item_id}', date='1 минуту назад',
                ts=int(time.time()*1000))


def catalog(*ids):
    return dict(catalog=True, items=[raw_item(i) for i in ids])


def config(tmp_path, engine='patchright', mode='light_fetch'):
    cfg = load_config()
    cfg.data_dir, cfg.city, cfg.night_hours = tmp_path, 'kazan', (0, 0)
    cfg.browser_engine, cfg.fetch_mode, cfg.direct_interface = engine, mode, None
    cfg.sections, cfg.requests_per_minute, cfg.cooldown_seconds = ['tovary_dlya_kompyutera'], 1, 900
    cfg.verification_check_seconds = 0  # local DOM reads on every tick in tests
    return cfg


def page(raw, code=200, headers=None, url=CATALOG):
    response = SimpleNamespace(status=code, all_headers=AsyncMock(return_value=headers or {}))
    return SimpleNamespace(evaluate=AsyncMock(return_value=raw), goto=AsyncMock(return_value=response),
                           wait_for_function=AsyncMock(), is_closed=lambda: False, on=Mock(), url=url)


def source(tmp_path, raw, engine='patchright', mode='light_fetch', **kw):
    scraper = Scraper(tmp_path/'profile', None, browser_engine=engine, fetch_mode=mode)
    scraper.page = page(raw, **kw)
    scraper.ctx = SimpleNamespace(request=SimpleNamespace(get=AsyncMock()))
    scraper.set_window = AsyncMock()
    return scraper


def respond(scraper, raw, code=200, headers=None):
    scraper.page.evaluate.return_value = raw
    scraper.page.goto.return_value = SimpleNamespace(status=code, all_headers=AsyncMock(return_value=headers or {}))


def database(tmp_path):
    db = DB(tmp_path/'bot.sqlite3')
    if not db.subscriptions():
        db.add_subscription('3060', None, None, [], 'gpu', ['kazan'])
    return db


def state(db):
    return db.source_states('local_browser')[0]


def expire(db):
    with db.conn:
        db.conn.execute('UPDATE source_state SET next_allowed_at=0')
    db.set('next_poll:local_browser', '0')


def test_a_passed_captcha_resumes_collection_and_delivery_without_commands(tmp_path):
    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, catalog(100), mode='page_navigation')
        alert = AsyncMock()
        mon = Monitor(config(tmp_path, mode='page_navigation'), db, scraper, alert, recipients=lambda: [1])
        assert await mon.poll_once()  # baseline
        respond(scraper, dict(challenge=True, items=[]))
        db.set('next_poll:local_browser', '0')
        assert await mon.poll_once()
        assert state(db)['state'] == SourceState.VERIFICATION_REQUIRED
        assert 'команды отправлять не нужно' in alert.await_args.args[0]
        # Requests stop; the page is only read locally while the captcha is on screen.
        db.set('next_poll:local_browser', '0')
        for _ in range(3):
            assert not await mon.poll_once()
        assert scraper.page.goto.await_count == 2
        # The user passes the check in the same Chrome; the open page now shows a catalog.
        respond(scraper, catalog(100, 101))
        assert not await mon.poll_once()
        assert scraper.page.goto.await_count == 2  # confirmed from the DOM, no HTTP load
        assert state(db)['state'] == SourceState.AVAILABLE
        assert alert.await_args.args[0].startswith('Доступ к Авито восстановлен')
        assert db.queue_counts() == {'pending': 1}  # the listing visible in the confirmed page
        assert db.conn.execute("SELECT count(*) FROM events WHERE kind='verification_confirmed'").fetchone()[0] == 1
        # Scheduled checks continue by themselves and new listings reach Telegram.
        respond(scraper, catalog(100, 101, 102))
        db.set('next_poll:local_browser', '0')
        assert await mon.poll_once()
        assert scraper.page.goto.await_count == 3
        send = AsyncMock()
        worker = DeliveryWorker(db, send, lambda _: True)
        assert await worker.deliver_once() and await worker.deliver_once()
        assert {c.args[0].id for c in send.await_args_list} == {101, 102}
        db.close()
    asyncio.run(scenario())


def test_a_stale_light_fetch_page_never_confirms_a_captcha(tmp_path):
    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, catalog(100))
        mon = Monitor(config(tmp_path), db, scraper, AsyncMock())
        assert await mon.poll_once()  # warm-up navigation; the page now shows a normal catalog
        scraper.page.evaluate.side_effect = [dict(challenge=True, items=[], status=200)]  # fetch() answer
        db.set('next_poll:local_browser', '0')
        assert await mon.poll_once()
        assert state(db)['state'] == SourceState.VERIFICATION_REQUIRED
        scraper.page.evaluate.side_effect = None
        scraper.page.evaluate.return_value = catalog(100)  # the stale document
        reads = scraper.page.evaluate.await_count
        for _ in range(3):
            assert not await mon.poll_once()
        assert scraper.page.evaluate.await_count == reads  # not trusted, not even read
        assert state(db)['state'] == SourceState.VERIFICATION_REQUIRED
        assert 'обновите' in mon.verification.describe()
        # The user reloads the page in Chrome (a navigation) and passes the check.
        scraper._on_navigated(SimpleNamespace(parent_frame=None))
        assert not await mon.poll_once()
        assert state(db)['state'] == SourceState.AVAILABLE
        assert scraper.page.goto.await_count == 1
        db.close()
    asyncio.run(scenario())


def test_b_429_without_retry_after_returns_to_schedule_by_itself(tmp_path):
    async def scenario():
        db = database(tmp_path)
        cfg = config(tmp_path, mode='page_navigation')
        scraper = source(tmp_path, dict(items=[]), mode='page_navigation', code=429)
        alert = AsyncMock()
        mon = Monitor(cfg, db, scraper, alert)
        started = time.time()
        assert await mon.poll_once()
        saved = state(db)
        assert saved['state'] == SourceState.RATE_LIMITED and saved['server_until'] is None
        assert started + cfg.cooldown_seconds - 1 <= saved['next_allowed_at'] <= time.time() + cfg.cooldown_seconds
        assert 'Авито срок не указал' in alert.await_args.args[0] and 'продолжится сам' in alert.await_args.args[0]
        db.set('next_poll:local_browser', '0')
        for _ in range(3):
            assert not await mon.poll_once()
        assert scraper.page.goto.await_count == 1
        respond(scraper, catalog(100))
        expire(db)
        assert await mon.poll_once()  # no /resume
        assert state(db)['state'] == SourceState.AVAILABLE and db.get('rate_limit_streak:local_browser') == '0'
        assert alert.await_args.args[0].startswith('Доступ к Авито восстановлен')
        assert not hasattr(mon, 'resume') and not hasattr(mon, 'set_paused')
        db.close()
    asyncio.run(scenario())


def test_c_retry_after_survives_restart_and_is_fully_respected(tmp_path):
    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, dict(items=[]), mode='page_navigation', code=429, headers={'retry-after': '3600'})
        await Monitor(config(tmp_path, mode='page_navigation'), db, scraper, AsyncMock()).poll_once()
        saved = state(db)
        assert saved['server_until'] == saved['next_allowed_at'] > time.time() + 3500
        db.close()
        db = database(tmp_path)  # application restart
        restarted_source = source(tmp_path, catalog(100), engine='playwright', mode='page_navigation')
        alert = AsyncMock()
        mon = Monitor(config(tmp_path, 'playwright', 'page_navigation'), db, restarted_source, alert)
        db.set('next_poll:local_browser', '0')
        for _ in range(3):
            assert not await mon.poll_once()
        restarted_source.page.goto.assert_not_awaited()
        assert state(db)['next_allowed_at'] == saved['next_allowed_at']
        assert 'срок Авито (Retry-After)' in alert.await_args.args[0]
        expire(db)
        assert await mon.poll_once()
        assert state(db)['state'] == SourceState.AVAILABLE
        db.close()
    asyncio.run(scenario())


def test_d_passed_captcha_waits_for_active_server_term(tmp_path):
    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, dict(challenge=True, items=[]), mode='page_navigation',
                         headers={'retry-after': '3600'})
        alert = AsyncMock()
        mon = Monitor(config(tmp_path, mode='page_navigation'), db, scraper, alert)
        assert await mon.poll_once()
        assert state(db)['state'] == SourceState.VERIFICATION_REQUIRED
        respond(scraper, catalog(100))  # the user passed the check
        db.set('next_poll:local_browser', '0')
        assert not await mon.poll_once()
        saved = state(db)
        assert saved['state'] == SourceState.RATE_LIMITED and saved['next_allowed_at'] > time.time() + 3500
        assert alert.await_args.args[0].startswith('Проверка пройдена') and 'продолжится сам' in alert.await_args.args[0]
        for _ in range(3):
            assert not await mon.poll_once()
        assert scraper.page.goto.await_count == 1
        expire(db)
        assert await mon.poll_once()
        assert state(db)['state'] == SourceState.AVAILABLE and scraper.page.goto.await_count == 2
        db.close()
    asyncio.run(scenario())


def test_e_browser_crash_keeps_telegram_and_restrictions(tmp_path):
    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, catalog(100), mode='page_navigation')
        alert = AsyncMock()
        starts = []
        async def start_source():
            starts.append(1)
            scraper._closed = False
        mon = Monitor(config(tmp_path, mode='page_navigation'), db, scraper, alert,
                      recipients=lambda: [1], start_source=start_source)
        scraper.stop = AsyncMock()
        assert await mon.poll_once()
        respond(scraper, catalog(100, 101))
        db.set('next_poll:local_browser', '0')
        assert await mon.poll_once()
        respond(scraper, dict(items=[]), code=429, headers={'retry-after': '3600'})
        db.set('next_poll:local_browser', '0')
        assert await mon.poll_once()
        scraper._closed = True  # Chrome crashed
        assert not await mon.poll_once()
        assert 'Перезапускаю' in alert.await_args.args[0]
        assert db.conn.execute("SELECT result FROM events WHERE kind='source_failed'").fetchone() == ('browser_closed',)
        send = AsyncMock()
        assert await DeliveryWorker(db, send, lambda _: True).deliver_once()  # Telegram keeps working
        mon._restart_at = 0
        assert not await mon.poll_once()
        assert starts == [1] and not mon.source_down and mon.service_state == 'RUNNING'
        assert state(db)['state'] == SourceState.RATE_LIMITED  # the saved 429 term is untouched
        assert scraper.page.goto.await_count == 3
        db.close()
    asyncio.run(scenario())


def test_f_staged_listing_survives_telegram_outage_and_restart_without_duplicates(tmp_path):
    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, catalog(100), mode='page_navigation')
        mon = Monitor(config(tmp_path, mode='page_navigation'), db, scraper, AsyncMock(), recipients=lambda: [1])
        assert await mon.poll_once()
        respond(scraper, catalog(100, 101))
        db.set('next_poll:local_browser', '0')
        assert await mon.poll_once()
        failing = AsyncMock(side_effect=ConnectionError('telegram down'))
        assert await DeliveryWorker(db, failing, lambda _: True).deliver_once()
        assert db.queue_counts() == {'retry': 1}
        db.close()
        db = database(tmp_path)  # restart
        with db.conn:
            db.conn.execute('UPDATE notification_outbox SET next_attempt_at=0')
        send = AsyncMock()
        assert await DeliveryWorker(db, send, lambda _: True).deliver_once()
        assert db.queue_counts() == {'sent': 1}
        mon = Monitor(config(tmp_path, mode='page_navigation'), db, scraper, AsyncMock(), recipients=lambda: [1])
        db.set('next_poll:local_browser', '0')
        assert await mon.poll_once()  # the same listing is received again
        assert db.queue_counts() == {'sent': 1}
        assert send.await_count == 1
        db.close()
    asyncio.run(scenario())


def test_g_three_subscriptions_share_one_fetch_and_one_photo(tmp_path):
    async def scenario():
        db = database(tmp_path)
        db.add_subscription('rtx', None, None, [], 'gpu', ['kazan'])
        db.add_subscription('3060 ti', None, 30000, [], 'gpu', ['kazan'])
        scraper = source(tmp_path, catalog(100), mode='page_navigation')
        mon = Monitor(config(tmp_path, mode='page_navigation'), db, scraper, AsyncMock(), recipients=lambda: [1, 2, 3])
        assert await mon.poll_once()
        respond(scraper, catalog(100, 101))
        db.set('next_poll:local_browser', '0')
        assert await mon.poll_once()
        assert scraper.page.goto.await_count == 2  # one load per check, not per subscription
        assert db.queue_counts() == {'pending': 3}
        assert db.conn.execute("SELECT count(*) FROM items").fetchone()[0] == 1
        download = AsyncMock(return_value=b'jpeg')
        photos = PhotoCache(download)
        async def send(item, subs, chat_id):
            assert len(subs) == 3
            await photos.get('https://img.example/1.jpg')
        worker = DeliveryWorker(db, send, lambda _: True)
        for _ in range(3):
            assert await worker.deliver_once()
        assert download.await_count == 1 and db.queue_counts() == {'sent': 3}
        db.close()
    asyncio.run(scenario())


def test_h_first_start_of_new_profile_makes_no_request_before_polling(tmp_path, monkeypatch):
    async def scenario():
        fresh = page(catalog(100), url='about:blank')
        cdp = SimpleNamespace(on=Mock(), send=AsyncMock(return_value={'windowId': 1}), detach=AsyncMock())
        request = SimpleNamespace(get=AsyncMock())
        ctx = SimpleNamespace(pages=[fresh], on=Mock(), close=AsyncMock(), request=request,
                              new_cdp_session=AsyncMock(return_value=cdp))
        driver = SimpleNamespace(chromium=SimpleNamespace(launch_persistent_context=AsyncMock(return_value=ctx)),
                                 stop=AsyncMock())
        api = SimpleNamespace(async_playwright=lambda: SimpleNamespace(start=AsyncMock(return_value=driver)))
        monkeypatch.setattr('avito_bot.scraper.importlib.import_module', Mock(return_value=api))
        scraper = Scraper(tmp_path/'profile', None, browser_engine='patchright')
        await scraper.start()
        assert (tmp_path/'profile-patchright-chrome').is_dir()
        fresh.goto.assert_not_awaited()
        fresh.evaluate.assert_not_awaited()
        request.get.assert_not_awaited()
        db = database(tmp_path)
        alert = AsyncMock()
        mon = Monitor(config(tmp_path), db, scraper, alert)
        assert not mon.photos_allowed()
        assert await mon.poll_once()
        fresh.goto.assert_awaited_once()
        assert fresh.goto.await_args.args[0] == CATALOG
        alert.assert_not_awaited()
        assert db.conn.execute('SELECT status,is_warmup,actual_mode FROM fetch_metrics').fetchall() == \
            [('ok', 1, 'page_navigation')]
        assert mon.photos_allowed()
        await scraper.stop()
        db.close()
    asyncio.run(scenario())


def test_h_browser_startup_failure_is_not_an_avito_restriction(tmp_path):
    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, catalog(100))
        attempts = []
        async def start_source():
            attempts.append(1)
            raise RuntimeError('Chrome is not installed')
        alert = AsyncMock()
        mon = Monitor(config(tmp_path), db, scraper, alert, start_source=start_source)
        scraper.stop = AsyncMock()
        mon.source_failed('startup: RuntimeError')
        mon._restart_at = 0
        assert not await mon.poll_once()
        assert attempts == [1] and mon.source_down and mon._restart_at > time.monotonic()
        assert db.source_states('local_browser') == []  # nothing is recorded as a block
        health = mon.health_text()
        assert 'RESTARTING' in health and 'ACCESS_DENIED' not in health and 'VERIFICATION' not in health
        scraper.page.goto.assert_not_awaited()
        db.close()
    asyncio.run(scenario())


def test_403_is_not_retried_blindly_and_unblock_recovers_on_request(tmp_path):
    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, dict(items=[]), mode='page_navigation', code=403)
        alert = AsyncMock()
        mon = Monitor(config(tmp_path, mode='page_navigation'), db, scraper, alert)
        assert await mon.poll_once()
        assert state(db)['state'] == SourceState.ACCESS_DENIED
        expire(db)
        respond(scraper, dict(blocked=True, items=[]))  # the open page still says access is limited
        for _ in range(3):
            assert not await mon.poll_once()
        assert scraper.page.goto.await_count == 1
        respond(scraper, catalog(100))
        assert 'обычная выдача подтверждена' in await mon.open_verification()  # one load, on request
        assert state(db)['state'] == SourceState.AVAILABLE and scraper.page.goto.await_count == 2
        db.close()
    asyncio.run(scenario())


def test_restart_reports_saved_restriction_without_new_request(tmp_path):
    async def scenario():
        db = database(tmp_path)
        old = source(tmp_path, dict(challenge=True, items=[]), engine='playwright', mode='page_navigation')
        await Monitor(config(tmp_path, 'playwright', 'page_navigation'), db, old, AsyncMock()).poll_once()
        db.close()
        db = database(tmp_path)
        scraper = source(tmp_path, catalog(100), url='about:blank')
        alert = AsyncMock()
        mon = Monitor(config(tmp_path), db, scraper, alert)
        for _ in range(3):
            assert not await mon.poll_once()
        scraper.page.goto.assert_not_awaited()
        scraper.page.evaluate.assert_not_awaited()
        alert.assert_awaited_once()
        text = alert.await_args.args[0]
        assert text.startswith('Сохранённое ограничение с прошлого запуска')
        assert 'текущего браузера patchright' in text and 'продолжится сам' in text
        for expected in ('сохранено с прошлого запуска', 'капча', 'HTTP-статус: 200',
                         'Последний запрос к Авито: ', 'Ограничение установлено: ', 'команды не нужны'):
            assert expected in text
        assert 'не открыта страница Авито' in mon.health_text()
        db.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('command', ['pause', 'resume'])
def test_pause_commands_and_buttons_are_gone(command):
    from avito_bot import ui
    from avito_bot.bot import COMMANDS
    menu = ui.main_menu(status='AVAILABLE: работает', last_ok=None, active=1, total=1, found_today=0, city='Казань')
    labels = [b.text for row in menu[1].inline_keyboard for b in row]
    assert not any('пауз' in label.lower() or 'продолжить' in label.lower() for label in labels)
    assert command not in {c.command for c in COMMANDS}


def test_restored_captcha_is_confirmed_after_unblock_shows_the_check(tmp_path):
    async def scenario():
        db = database(tmp_path)
        old = source(tmp_path, dict(challenge=True, items=[]), engine='playwright', mode='page_navigation')
        await Monitor(config(tmp_path, 'playwright', 'page_navigation'), db, old, AsyncMock()).poll_once()
        db.close()
        db = database(tmp_path)
        scraper = source(tmp_path, dict(challenge=True, items=[]), url='about:blank')
        mon = Monitor(config(tmp_path), db, scraper, AsyncMock())
        assert not await mon.poll_once()
        scraper.page.url = CATALOG
        assert 'команды отправлять не нужно' in await mon.open_verification()  # the check is on screen now
        assert state(db)['state'] == SourceState.VERIFICATION_REQUIRED
        respond(scraper, catalog(100))  # the user passed it
        assert not await mon.poll_once()
        assert state(db)['state'] == SourceState.AVAILABLE and scraper.page.goto.await_count == 1
        db.close()
    asyncio.run(scenario())


IP_PAGE = dict(blocked=True, items=[])  # «Доступ ограничен: проблема с IP» with a «Продолжить» check


def test_429_ip_check_page_is_shown_and_lifted_by_passing_it(tmp_path):
    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, catalog(100), mode='page_navigation')
        alert = AsyncMock()
        mon = Monitor(config(tmp_path, mode='page_navigation'), db, scraper, alert)
        assert await mon.poll_once()
        respond(scraper, IP_PAGE, code=429)
        db.set('next_poll:local_browser', '0')
        assert await mon.poll_once()
        saved = state(db)
        assert saved['state'] == SourceState.RATE_LIMITED and saved['reason'] == 'HTTP 429 IP check'
        assert saved['next_allowed_at'] > time.time() + 800 and saved['server_until'] is None
        scraper.set_window.assert_awaited_with('normal')
        assert '«Продолжить»' in alert.await_args.args[0] and 'продолжит сбор сразу' in alert.await_args.args[0]
        assert 'можно пройти проверку' in mon.health_text()
        for _ in range(3):  # the check is still on screen: local reads only
            assert not await mon.poll_once()
        assert state(db)['state'] == SourceState.RATE_LIMITED
        respond(scraper, catalog(100))  # the user pressed «Продолжить» and passed the check
        assert not await mon.poll_once()
        assert state(db)['state'] == SourceState.AVAILABLE  # long before the local pause ends
        assert alert.await_args.args[0].startswith('Доступ к Авито восстановлен')
        assert scraper.page.goto.await_count == 2 and db.get('rate_limit_streak:local_browser') == '0'
        db.close()
    asyncio.run(scenario())


def test_429_ip_check_keeps_retry_after_after_passing(tmp_path):
    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, IP_PAGE, mode='page_navigation', code=429, headers={'retry-after': '3600'})
        alert = AsyncMock()
        mon = Monitor(config(tmp_path, mode='page_navigation'), db, scraper, alert)
        assert await mon.poll_once()
        assert 'сбор начнётся после срока Авито' in alert.await_args.args[0]
        respond(scraper, catalog(100))
        assert not await mon.poll_once()
        saved = state(db)
        assert saved['state'] == SourceState.RATE_LIMITED and saved['next_allowed_at'] > time.time() + 3500
        assert alert.await_args.args[0].startswith('Проверка пройдена')
        assert scraper.page.goto.await_count == 1
        db.close()
    asyncio.run(scenario())


def test_plain_429_page_is_not_offered_as_a_check(tmp_path):
    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, dict(items=[]), mode='page_navigation', code=429)
        mon = Monitor(config(tmp_path, mode='page_navigation'), db, scraper, AsyncMock())
        assert await mon.poll_once()
        assert state(db)['reason'] == 'HTTP 429'
        scraper.page.evaluate.return_value = catalog(100)
        reads = scraper.page.evaluate.await_count
        for _ in range(3):
            assert not await mon.poll_once()
        assert scraper.page.evaluate.await_count == reads and state(db)['state'] == SourceState.RATE_LIMITED
        db.close()
    asyncio.run(scenario())


def test_light_fetch_429_is_followed_by_a_navigation(tmp_path):
    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, catalog(100))
        alert = AsyncMock()
        mon = Monitor(config(tmp_path), db, scraper, alert)
        assert await mon.poll_once()  # warm-up navigation
        scraper.page.evaluate.side_effect = [dict(status=429, items=[])]  # off-page fetch answer
        db.set('next_poll:local_browser', '0')
        assert await mon.poll_once()
        assert scraper.page.goto.await_count == 1 and not scraper.light_ok
        scraper.page.evaluate.side_effect = None
        respond(scraper, IP_PAGE, code=429)
        expire(db)
        assert await mon.poll_once()
        assert scraper.page.goto.await_count == 2  # navigated: the check page is now in the window
        assert state(db)['reason'] == 'HTTP 429 IP check'
        assert alert.await_count == 2  # every new 429 is reported
        db.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('draw', [0.7, 1.3])
def test_check_interval_has_random_spread(tmp_path, monkeypatch, draw):
    async def scenario():
        import avito_bot.monitor as monitor_module
        monkeypatch.setattr(monitor_module.random, 'uniform', lambda a, b: draw if (a, b) == (0.7, 1.3) else 1)
        db = database(tmp_path)
        cfg = config(tmp_path, mode='page_navigation')
        mon = Monitor(cfg, db, source(tmp_path, catalog(100), mode='page_navigation'), AsyncMock())
        started = time.time()
        assert await mon.poll_once()
        planned = float(db.get('next_poll:local_browser')) - started
        assert abs(planned - mon.delay() * draw) < 2
        db.close()
    asyncio.run(scenario())
