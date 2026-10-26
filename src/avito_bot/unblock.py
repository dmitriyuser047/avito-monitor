"""Explicit manual verification using the saved profile; never runs beside the bot."""
import asyncio
import logging
import time

from .config import load_config
from .db import DB
from .direct_proxy import DirectProxy
from .monitor import Monitor
from .runtime import InstanceLock
from .scraper import Scraper
from .sources import FetchStatus, SourceState, USER_STATES


async def run() -> None:
    cfg = load_config()
    db = DB(cfg.data_dir / 'bot.sqlite3')
    proxy, scraper = None, None
    try:
        states = db.source_states(cfg.source_id)
        for s in states:
            if s['state'] != SourceState.AVAILABLE:
                print(f"Сохранённое состояние {s['city']}: {s['state']} ({s['reason']}), "
                      f"HTTP {s['http_status'] or '—'}, установлено {time.strftime('%d.%m %H:%M', time.localtime(s['since']))}")
        if any((s.get('server_until') or 0) > time.time() for s in states):
            print('Пауза источника ещё не истекла. Новых обращений не выполнялось.')
            return
        if cfg.browser_session_mode == 'managed' and cfg.direct_interface:
            proxy = DirectProxy(cfg.direct_interface)
            await proxy.start()
        # The check must be passed from the same IP and profile the bot collects with.
        scraper = Scraper(cfg.browser_profile_dir, proxy.url if proxy else cfg.proxy_url, headless=False,
                          browser_engine=cfg.browser_engine, fetch_mode=cfg.fetch_mode,
                          browser_channel=cfg.browser_channel, session_mode=cfg.browser_session_mode,
                          cdp_endpoint=cfg.cdp_endpoint, cdp_profile_dir=cfg.cdp_profile_dir,
                          startup_diagnostics=cfg.startup_diagnostics, extension_bridge_port=cfg.extension_bridge_port,
                          source_id=cfg.source_id)
        await scraper.start()
        await scraper.set_window('normal')
        waiting = next((s['city'] for s in states if s['state'] in USER_STATES), cfg.city)
        from .config import feed_url
        async def announce(text):
            print(text)
        monitor = Monitor(cfg, db, scraper, announce)
        result = await scraper.load(feed_url(waiting, cfg.feed))
        await monitor.ingest(result)
        if result.status in (FetchStatus.OK, FetchStatus.NO_RESULTS):
            monitor.access.confirm(waiting, time.time())
            print('Доступ подтверждён для этого города. Запустите бота — сбор продолжится сам.')
            return
        for attempt in range(180):
            if scraper.closed:
                return
            if await monitor.verification.tick():
                print('Проверка сохранена. Запустите бота; серверные сроки сохраняются.')
                return
            if attempt == 0:
                print('Пройдите проверку в окне браузера. Повторных загрузок нет.')
            await asyncio.sleep(5)
        print('Время ожидания истекло. Доступ не подтверждён.')
    finally:
        if scraper:
            await scraper.stop()
        if proxy:
            await proxy.stop()
        db.close()


def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    try:
        with InstanceLock(load_config().data_dir / 'bot.lock'):
            asyncio.run(run())
    except BlockingIOError:
        print('Сначала остановите запущенный бот. Либо используйте в нём /unblock.')
    except KeyboardInterrupt:
        pass
