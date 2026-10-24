"""Разблокировка вручную: открыть окно браузера бота на Авито и дождаться, пока человек решит капчу.

Запуск (бот в это время должен быть остановлен): uv run avito-unblock
"""

import asyncio
import logging

from .config import load_config
from .direct_proxy import DirectProxy
from .scraper import Scraper, Status

WAIT_MINUTES = 15


async def run() -> None:
    cfg = load_config()
    proxy = DirectProxy(cfg.direct_interface) if cfg.direct_interface else None
    if proxy:
        await proxy.start()
    scraper = Scraper(cfg.data_dir / "browser-profile", proxy.url if proxy else None, headless=False)
    await scraper.start()
    await scraper.set_window("normal")
    try:
        status, _ = await scraper.fetch(cfg.feed_url)
        if status is Status.OK:
            print("Авито открывается без проверки, разблокировка не нужна.")
            return
        print("В окне браузера нажмите «Продолжить» и решите капчу.")
        for _ in range(WAIT_MINUTES * 12):
            await asyncio.sleep(5)
            status, _ = await scraper.read_current()
            if status is Status.OK:
                print("Готово: доступ к Авито есть. Можно запускать бота.")
                return
        print(f"За {WAIT_MINUTES} минут проверка не пройдена. Если капча не появляется, "
              "IP заблокирован надолго — подождите несколько часов или перезагрузите роутер.")
    finally:
        await scraper.stop()
        if proxy:
            await proxy.stop()


def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
