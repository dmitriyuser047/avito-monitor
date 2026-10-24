"""Личный Telegram-бот: новые объявления Авито (комплектующие и ПК)."""

import asyncio
import logging
from logging.handlers import RotatingFileHandler

from .bot import TelegramBot
from .config import load_config
from .db import DB, Subscription
from .direct_proxy import DirectProxy
from .matcher import Item
from .monitor import Monitor
from .scraper import Scraper

log = logging.getLogger("avito_bot")


async def run() -> None:
    cfg = load_config()
    db = DB(cfg.data_dir / "bot.sqlite3")
    db.prune_seen()

    proxy = None
    if cfg.direct_interface:
        proxy = DirectProxy(cfg.direct_interface)
        await proxy.start()

    scraper = Scraper(cfg.data_dir / "browser-profile", proxy.url if proxy else None, cfg.headless)
    await scraper.start()

    if cfg.bot_token:
        tg = TelegramBot(cfg.bot_token, db, cfg.owner_id, cfg.city)
        alert, send_item = tg.alert, tg.send_item
    else:
        log.warning("BOT_TOKEN не задан — пробный режим, объявления пишутся в консоль")
        tg = None

        async def alert(text: str) -> None:
            log.info("[alert] %s", text)

        async def send_item(item: Item, subs: list[Subscription]) -> None:
            log.info("[объявление] %s | %s ₽ | %s | %s", item.title, item.price, item.date, item.url)

    monitor = Monitor(cfg, db, scraper, alert, send_item)
    tasks = [monitor.run()]
    if tg:
        tg.monitor = monitor
        tg.fetch_image = scraper.download
        await tg.setup()
        tasks.append(tg.dp.start_polling(tg.bot, handle_signals=False))
    try:
        await asyncio.gather(*tasks)
    finally:
        await scraper.stop()
        if proxy:
            await proxy.stop()


def main() -> None:
    data_dir = load_config().data_dir
    data_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            RotatingFileHandler(data_dir / "bot.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8"),
        ],
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("aiogram.event").setLevel(logging.WARNING)
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
