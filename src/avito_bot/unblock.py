"""Explicit manual verification using the saved profile; never runs beside the bot."""
import asyncio
import logging
import time

from .config import load_config
from .db import DB
from .direct_proxy import DirectProxy
from .monitor import rate_limit_terms
from .runtime import InstanceLock
from .scraper import Scraper
from .sources import FetchStatus, SourceState, USER_STATES


async def run() -> None:
    cfg = load_config()
    db = DB(cfg.data_dir / 'bot.sqlite3')
    proxy, scraper = None, None
    try:
        states = db.source_states('local_browser')
        for s in states:
            if s['state'] != SourceState.AVAILABLE:
                print(f"Сохранённое состояние {s['city']}: {s['state']} ({s['reason']}), "
                      f"HTTP {s['http_status'] or '—'}, установлено {time.strftime('%d.%m %H:%M', time.localtime(s['since']))}")
        if any((s['next_allowed_at'] or 0) > time.time() for s in states):
            print('Пауза источника ещё не истекла. Новых обращений не выполнялось.')
            return
        if cfg.direct_interface:
            proxy = DirectProxy(cfg.direct_interface)
            await proxy.start()
        scraper = Scraper(cfg.data_dir / 'browser-profile', proxy.url if proxy else None, headless=False,
                          browser_engine=cfg.browser_engine, fetch_mode=cfg.fetch_mode,
                          browser_channel=cfg.browser_channel)
        await scraper.start()
        await scraper.set_window('normal')
        waiting = next((s['city'] for s in states if s['state'] in USER_STATES), cfg.city)
        from .config import feed_url
        result = await scraper.load(feed_url(waiting, cfg.feed))
        for _ in range(180):
            if result.status in (FetchStatus.OK, FetchStatus.NO_RESULTS):
                for s in states:
                    db.save_source_state('local_browser', s['city'], SourceState.AVAILABLE)
                print('Доступ подтверждён. Запустите бота — сбор продолжится сам.')
                return
            if result.status is FetchStatus.RATE_LIMITED:
                server_until, until = rate_limit_terms(time.time(), result.retry_after_sec, cfg.cooldown_seconds, 1,
                                                       cfg.max_backoff_seconds)
                db.save_source_state('local_browser', waiting, SourceState.RATE_LIMITED, until, 'HTTP 429',
                                     result.http_status, server_until)
                print('Авито указал паузу. Проверка остановлена.')
                return
            if scraper.closed:
                return
            print('Пройдите проверку в окне браузера. Повторных загрузок нет.') if _ == 0 else None
            await asyncio.sleep(5)
            result = await scraper.read_current()
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
