"""Origin of a stop: user's pause, restriction restored from SQLite, or a new answer from Avito."""
import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from avito_bot.config import feed_url, load_config
from avito_bot.db import DB
from avito_bot.monitor import Monitor
from avito_bot.scraper import Scraper
from avito_bot.sources import SourceState

CATALOG = 'https://www.avito.ru/kazan/bytovaya_elektronika?s=104'


def raw_item(item_id):
    return dict(id=item_id, title='RTX 3060 Ti', price='18000',
                href=f'/kazan/tovary_dlya_kompyutera/gpu_{item_id}', date='1 минуту назад',
                ts=int(time.time()*1000))


def config(tmp_path, engine='patchright', mode='light_fetch'):
    cfg = load_config()
    cfg.data_dir, cfg.city, cfg.night_hours = tmp_path, 'kazan', (0, 0)
    cfg.browser_engine, cfg.fetch_mode, cfg.direct_interface = engine, mode, None
    cfg.sections, cfg.requests_per_minute, cfg.cooldown_seconds = ['tovary_dlya_kompyutera'], 1, 900
    return cfg


def page(raw, code=200, headers=None, url='about:blank'):
    response = SimpleNamespace(status=code, all_headers=AsyncMock(return_value=headers or {}))
    return SimpleNamespace(evaluate=AsyncMock(return_value=raw), goto=AsyncMock(return_value=response),
                           wait_for_selector=AsyncMock(), is_closed=lambda: False, on=Mock(), url=url)


def source(tmp_path, raw, engine='patchright', mode='light_fetch', **kw):
    scraper = Scraper(tmp_path/'profile', None, browser_engine=engine, fetch_mode=mode)
    scraper.page = page(raw, **kw)
    scraper.ctx = SimpleNamespace(request=SimpleNamespace(get=AsyncMock()))
    scraper.set_window = AsyncMock()
    return scraper


def database(tmp_path):
    db = DB(tmp_path/'bot.sqlite3')
    if not db.subscriptions():
        db.add_subscription('3060', None, None, [], 'gpu', ['kazan'])
    return db


def restore_challenge(tmp_path, server_term=None):
    """A captcha answered to the Playwright profile in an earlier run."""
    async def earlier_run():
        db = database(tmp_path)
        headers = {'retry-after': str(server_term)} if server_term else None
        old = source(tmp_path, dict(challenge=True, items=[]), engine='playwright', headers=headers)
        await Monitor(config(tmp_path, 'playwright'), db, old, AsyncMock()).poll_once()
        assert db.source_states('local_browser')[0]['state'] == SourceState.WAITING_FOR_USER
        db.close()
    asyncio.run(earlier_run())


def test_first_start_of_new_profile_makes_no_request_before_polling(tmp_path, monkeypatch):
    async def scenario():
        fresh = page(dict(catalog=True, items=[raw_item(100)]))
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
        # Launching a new profile neither navigates, nor evaluates, nor fetches photos.
        fresh.goto.assert_not_awaited()
        fresh.evaluate.assert_not_awaited()
        request.get.assert_not_awaited()
        db = database(tmp_path)
        alert = AsyncMock()
        mon = Monitor(config(tmp_path), db, scraper, alert)
        assert not mon.photos_allowed()
        assert 'старая' not in mon.health_text() and 'ожидается' not in mon.health_text()
        assert await mon.poll_once()
        fresh.goto.assert_awaited_once()
        assert fresh.goto.await_args.args[0] == CATALOG
        assert fresh.evaluate.await_count == 1
        alert.assert_not_awaited()
        assert db.conn.execute('SELECT status,is_warmup,actual_mode FROM fetch_metrics').fetchall() == \
            [('ok', 1, 'page_navigation')]
        assert mon.photos_allowed()
        await scraper.stop()
        db.close()
    asyncio.run(scenario())


def test_restart_with_saved_restriction_is_not_reported_as_patchright_result(tmp_path):
    restore_challenge(tmp_path)

    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, dict(catalog=True, items=[]))
        alert = AsyncMock()
        mon = Monitor(config(tmp_path), db, scraper, alert)
        for _ in range(3):
            assert not await mon.poll_once()
        scraper.page.goto.assert_not_awaited()
        scraper.page.evaluate.assert_not_awaited()
        alert.assert_awaited_once()
        text = alert.await_args.args[0]
        assert text.startswith('Сохранённое ограничение с прошлого запуска')
        assert 'текущего браузера patchright' in text and 'Авито ограничил доступ' not in text
        for expected in ('старая сохранённая блокировка · ожидается проверка', 'капча', 'HTTP-статус: 200',
                         'Последний запрос к Авито: ', 'Ограничение установлено: ', 'нужна ручная проверка'):
            assert expected in text
        health = mon.health_text()
        assert 'старая сохранённая блокировка' in health and 'ожидается проверка' in health
        assert 'Ручная пауза' not in health and 'старая' in mon.status
        assert db.source_states('local_browser')[0]['state'] == SourceState.WAITING_FOR_USER
        assert not mon.photos_allowed()
        db.close()
    asyncio.run(scenario())


def test_restart_keeps_single_429_cooldown_without_escalation(tmp_path):
    async def scenario():
        db = database(tmp_path)
        old = source(tmp_path, dict(items=[]), engine='playwright', code=429, headers={'retry-after': '3600'})
        await Monitor(config(tmp_path, 'playwright'), db, old, AsyncMock()).poll_once()
        saved = db.source_states('local_browser')[0]
        restarted = Monitor(config(tmp_path), db, source(tmp_path, dict(items=[])), AsyncMock())
        assert not await restarted.poll_once()
        state = db.source_states('local_browser')[0]
        assert state['state'] == SourceState.COOLDOWN and state['since'] == saved['since']
        assert state['next_allowed_at'] == saved['next_allowed_at']
        assert 'старая сохранённая блокировка' in restarted.health_text()
        assert 'срок Авито (Retry-After)' in restarted.health_text()
        db.close()
    asyncio.run(scenario())


def test_resume_confirms_loaded_page_without_new_request(tmp_path):
    restore_challenge(tmp_path)

    async def scenario():
        db = database(tmp_path)
        raw = dict(challenge=True, items=[])
        scraper = source(tmp_path, raw, url=CATALOG)
        mon = Monitor(config(tmp_path), db, scraper, AsyncMock())
        assert not await mon.poll_once()
        # Captcha is still on screen: /resume does not lift it.
        assert 'не подтверждена' in await mon.resume()
        assert db.source_states('local_browser')[0]['state'] == SourceState.WAITING_FOR_USER
        assert not await mon.poll_once()
        # The user passed the check in the window; the loaded DOM now shows a catalog.
        raw.clear()
        raw.update(catalog=True, items=[raw_item(100)])
        assert 'без нового запроса' in await mon.resume()
        scraper.page.goto.assert_not_awaited()
        assert scraper.page.evaluate.await_count == 2
        assert db.source_states('local_browser')[0]['state'] == SourceState.ACTIVE
        assert not mon.paused and mon.collection_confirmed
        manual = db.conn.execute("SELECT status,operation,actual_mode FROM fetch_metrics ORDER BY id DESC LIMIT 1")
        assert manual.fetchone() == ('ok', 'manual', 'loaded_dom')
        db.set('next_poll:local_browser', '0')
        assert await mon.poll_once()  # light fetch reuses the confirmed page
        scraper.page.goto.assert_not_awaited()
        db.close()
    asyncio.run(scenario())


def test_resume_keeps_server_term_after_confirmed_check(tmp_path):
    restore_challenge(tmp_path, server_term=3600)

    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, dict(catalog=True, empty=True, items=[]), url=CATALOG)
        mon = Monitor(config(tmp_path), db, scraper, AsyncMock())
        assert 'Пауза источника ещё не истекла' in await mon.open_verification()
        assert 'после срока Авито' in await mon.resume()
        state = db.source_states('local_browser')[0]
        assert state['state'] == SourceState.COOLDOWN and state['next_allowed_at'] > time.time() + 3500
        assert not await mon.poll_once()
        scraper.page.goto.assert_not_awaited()
        db.close()
    asyncio.run(scenario())


@pytest.mark.parametrize(('headers', 'server'), [({'retry-after': '120'}, True), ({}, False)])
def test_real_429_separates_server_term_from_local_backoff(tmp_path, headers, server):
    async def scenario():
        db = database(tmp_path)
        cfg = config(tmp_path, mode='page_navigation')
        scraper = source(tmp_path, dict(items=[]), mode='page_navigation', code=429, headers=headers)
        alert = AsyncMock()
        mon = Monitor(cfg, db, scraper, alert)
        started = time.time()
        assert await mon.poll_once()
        state = db.source_states('local_browser')[0]
        assert state['state'] == SourceState.COOLDOWN and state['http_status'] == 429
        expected = 120 if server else cfg.cooldown_seconds
        assert started + expected - 1 <= state['next_allowed_at'] <= time.time() + expected
        assert (state['server_until'] == state['next_allowed_at']) if server else state['server_until'] is None
        text = alert.await_args.args[0]
        assert 'новый HTTP 429' in text and 'patchright' in text
        assert ('срок Авито' in text) if server else ('Авито срок не указал' in text)
        health = mon.health_text()
        assert 'новый HTTP 429' in health and 'HTTP-статус: 429' in health
        assert ('срок Авито (Retry-After)' if server else 'локальная пауза бота') in health
        # /resume removes nothing that Avito or the backoff set, and makes no request.
        assert 'не сбрасывается' in await mon.resume()
        assert db.source_states('local_browser')[0]['next_allowed_at'] == state['next_allowed_at']
        db.set('next_poll:local_browser', '0')
        for _ in range(3):
            assert not await mon.poll_once()
        assert scraper.page.goto.await_count == 1
        # After the term a second 429 stops collection and doubles the local backoff.
        with db.conn:
            db.conn.execute('UPDATE source_state SET next_allowed_at=0')
        assert await mon.poll_once()
        second = db.source_states('local_browser')[0]
        assert second['state'] == SourceState.WAITING_FOR_USER and second['reason'] == 'repeated HTTP 429'
        if not server:
            assert second['next_allowed_at'] >= time.time() + 2*cfg.cooldown_seconds - 5
        db.close()
    asyncio.run(scenario())


def test_manual_pause_is_separate_from_restrictions(tmp_path):
    async def scenario():
        db = database(tmp_path)
        scraper = source(tmp_path, dict(catalog=True, empty=True, items=[]))
        mon = Monitor(config(tmp_path), db, scraper, AsyncMock())
        mon.set_paused(True)
        assert not await mon.poll_once()
        assert 'Ручная пауза: включена' in mon.health_text() and mon.status.startswith('PAUSED')
        assert db.source_states('local_browser') == []
        assert await mon.resume() == 'Опрос возобновлён.'
        assert await mon.poll_once()
        db.close()
    asyncio.run(scenario())


def test_status_reflects_commands_immediately(tmp_path):
    async def scenario():
        db = database(tmp_path)
        db.save_source_state('local_browser', 'kazan', SourceState.WAITING_FOR_USER, time.time() - 60,
                             'repeated HTTP 429', 429)
        scraper = source(tmp_path, dict(catalog=True, empty=True, items=[]))
        mon = Monitor(config(tmp_path), db, scraper, AsyncMock())
        db.set('next_poll:local_browser', str(time.time() + 3600))  # no poll may refresh the status
        assert not await mon.poll_once()
        assert mon.status.startswith('WAITING_FOR_USER: старая сохранённая блокировка')
        assert await mon.resume() == 'Опрос возобновлён.'
        # The menu sent right after /resume must not show the lifted restriction.
        assert mon.status == 'ACTIVE: ожидает следующей проверки'
        scraper.page.goto.assert_not_awaited()
        mon.set_paused(True)
        assert mon.status == 'PAUSED: пауза'
        db.save_source_state('local_browser', 'kazan', SourceState.COOLDOWN, time.time() + 600, 'HTTP 429', 429)
        assert 'не сбрасывается' in await mon.resume()
        assert mon.status == 'COOLDOWN: новый HTTP 429'
        db.close()
    asyncio.run(scenario())
