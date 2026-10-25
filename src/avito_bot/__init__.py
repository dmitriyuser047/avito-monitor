"""Local Avito monitor: independent source, Telegram polling and durable delivery."""
import asyncio
import logging
import os
import signal
from logging.handlers import RotatingFileHandler
from aiogram.utils.token import TokenValidationError

from .bot import TelegramBot
from .config import load_config
from .db import DB
from .delivery import DeliveryWorker
from .direct_proxy import DirectProxy
from .monitor import Monitor
from .photo_cache import PhotoCache
from .runtime import InstanceLock, raise_open_files_limit
from .scraper import Scraper
from .watchdog import ComponentHung, HUNG_EXIT_CODE, Watchdog

log = logging.getLogger('avito_bot')


async def run() -> None:
    cfg = load_config()
    db = DB(cfg.data_dir / 'bot.sqlite3')
    tg, proxy, scraper = None, None, None
    tasks = []
    try:
        db.prune_seen()
        db.prune_events()
        db.record_event('service_started', result=str(os.getpid()))
        if cfg.bot_token:
            try:
                tg = TelegramBot(cfg.bot_token, db, cfg.owner_id, cfg.city)
            except TokenValidationError:
                log.error('BOT_TOKEN is invalid; use avito-setup')
                raise SystemExit(78)
            if not tg.owner_id:
                log.error('OWNER_ID must be configured before starting; use avito-setup')
                raise SystemExit(78)
            db.set('owner_id', str(tg.owner_id))
        scraper = Scraper(cfg.data_dir / 'browser-profile', None, cfg.headless,
                          cfg.save_debug_html, cfg.debug_html_retention_hours,
                          cfg.browser_engine, cfg.fetch_mode, cfg.browser_channel,
                          session_mode=cfg.browser_session_mode, cdp_endpoint=cfg.cdp_endpoint,
                          cdp_profile_dir=cfg.cdp_profile_dir, startup_diagnostics=cfg.startup_diagnostics)
        async def console_alert(text):
            log.info('[alert] %s', text)
        async def start_source():
            # Used at startup and by every automatic browser restart; starting makes no request to Avito.
            nonlocal proxy
            if proxy and not proxy.health()['connected']:
                await proxy.stop()
                proxy = None
            if cfg.browser_session_mode == 'managed' and cfg.direct_interface and proxy is None:
                candidate = DirectProxy(cfg.direct_interface)
                try:
                    await candidate.start()
                except BaseException:
                    await candidate.stop()
                    raise
                proxy = candidate
                scraper.proxy_url = proxy.url
                scraper.proxy_health = proxy.health
            await scraper.start()
        monitor = Monitor(cfg, db, scraper, tg.alert if tg else console_alert,
                          recipients=tg.recipients if tg else None, start_source=start_source)
        if tg:
            tg.monitor = monitor
            # Reuse each listing image for multiple recipients without extra
            # requests. Respect the source safety gate on every recipient.
            photo_cache = PhotoCache(scraper.download)
            # Delivery does not need a live browser; photo is optional.
            async def optional_photo(url):
                # No photo request before the first confirmed catalog of this run (e.g. a fresh profile).
                if not monitor.photos_allowed():
                    return None
                return await photo_cache.get(url)
            tg.fetch_image = optional_photo
            worker = DeliveryWorker(db, tg.send_to_chat, tg.allowed)
            monitor.delivery = worker
            tasks.append(asyncio.create_task(worker.run(), name='delivery'))
            await tg.setup()
            tasks.append(asyncio.create_task(tg.dp.start_polling(tg.bot, handle_signals=False), name='telegram'))
        try:
            await start_source()
        except Exception as exc:
            # A browser/proxy failure is not a restriction of Avito; the monitor retries it with backoff.
            log.error('Source startup failed: %s. Telegram remains available; restart is automatic.',
                      type(exc).__name__)
            db.record_event('source_failed', source_id=scraper.source_id, result='startup_failed',
                            error=type(exc).__name__)
            monitor.source_failed(f'startup: {type(exc).__name__}')
        tasks.append(asyncio.create_task(monitor.run(), name='source'))
        beats = {'source': lambda: monitor.heartbeat}
        if monitor.delivery is not None and hasattr(monitor.delivery, 'heartbeat'):
            beats['delivery'] = lambda: monitor.delivery.heartbeat
        tasks.append(asyncio.create_task(Watchdog(cfg.watchdog_seconds, beats).run(), name='watchdog'))
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        registered = []
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
                registered.append(sig)
            except (NotImplementedError, RuntimeError):
                pass
        stopper = asyncio.create_task(stop.wait(), name='shutdown')
        tasks.append(stopper)
        log.info('Application ready: telegram=%s delivery=%s source=%s pid=%s',
                 bool(tg), bool(tg), monitor.status, os.getpid())
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is not stopper:
                    task.result()
        finally:
            for sig in registered:
                loop.remove_signal_handler(sig)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        cleanup = []
        if scraper:
            cleanup.append(scraper.stop())
        if proxy:
            cleanup.append(proxy.stop())
        if tg:
            cleanup.append(tg.bot.session.close())
        results = await asyncio.gather(*cleanup, return_exceptions=True)
        if any(isinstance(result, BaseException) for result in results):
            log.warning('A resource reported an error during shutdown')
        try:
            db.record_event('service_stopped', result=str(os.getpid()))
        except Exception:
            log.warning('Cannot record shutdown event')
        db.close()


def main() -> None:
    try:
        cfg = load_config()
    except (ValueError, IndexError) as exc:
        print(f'Configuration error: {exc}')
        raise SystemExit(78)
    cfg.data_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    cfg.data_dir.chmod(0o700)
    handlers = [RotatingFileHandler(cfg.data_dir / 'bot.log', maxBytes=5_000_000, backupCount=3, encoding='utf-8')]
    if os.environ.get('AVITO_SERVICE') != '1':
        handlers.append(logging.StreamHandler())
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s', handlers=handlers)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('aiogram.event').setLevel(logging.WARNING)
    log.info('Open files limit: %s', raise_open_files_limit())
    try:
        with InstanceLock(cfg.data_dir / 'bot.lock'):
            asyncio.run(run())
    except BlockingIOError:
        log.error('Another instance is already running; refusing duplicate Telegram polling')
        raise SystemExit(78)
    except ComponentHung as exc:
        log.error('%s; exiting so the supervisor starts a fresh process', exc)
        raise SystemExit(HUNG_EXIT_CODE)
    except KeyboardInterrupt:
        pass
