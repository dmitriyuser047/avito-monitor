"""Настройки из .env (или переменных окружения)."""

import os
import math
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[2]

# Город по-русски → как в адресе Авито. Для остальных пишите как в адресе: avito.ru/<город>/
CITIES = {
    "москва": "moskva", "санкт-петербург": "sankt-peterburg", "спб": "sankt-peterburg", "питер": "sankt-peterburg",
    "новосибирск": "novosibirsk", "екатеринбург": "ekaterinburg", "казань": "kazan",
    "нижний новгород": "nizhniy_novgorod", "челябинск": "chelyabinsk", "самара": "samara", "омск": "omsk",
    "ростов-на-дону": "rostov-na-donu", "уфа": "ufa", "красноярск": "krasnoyarsk", "воронеж": "voronezh",
    "пермь": "perm", "волгоград": "volgograd", "краснодар": "krasnodar", "саратов": "saratov",
    "тюмень": "tyumen", "тольятти": "tolyatti", "ижевск": "izhevsk", "набережные челны": "naberezhnye_chelny",
}
POPULAR_CITIES = ["moskva", "sankt-peterburg", "kazan", "ekaterinburg", "novosibirsk", "nizhniy_novgorod",
                  "krasnodar", "samara", "ufa", "rostov-na-donu"]
CITY_NAMES = {slug: name.title().replace("-На-", "-на-") for name, slug in CITIES.items() if len(name) > 4 and name != "питер"}


def feed_url(city: str, feed: str) -> str:
    return f"https://www.avito.ru/{city}/{feed}?s=104"  # s=104 — сортировка «по дате»


def city_slug(text: str) -> str:
    """«Казань» → kazan; неизвестный город — как написан (ожидаем вид из адреса Авито)."""
    key = text.strip().lower().replace("ё", "е")
    return CITIES.get(key, key.replace(" ", "_"))


def city_name(slug: str) -> str:
    return CITY_NAMES.get(slug, slug)


def parse_proxy_url(url: str) -> dict:
    """PROXY_URL → Playwright proxy settings. The password never leaves this dict (no repr, no logs)."""
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError:
        raise ValueError('PROXY_URL must look like http://login:password@host:port') from None
    if parts.scheme not in ('http', 'https', 'socks5') or not parts.hostname or not port:
        raise ValueError('PROXY_URL must look like http://login:password@host:port (http, https or socks5)')
    if parts.path not in ('', '/') or parts.query or parts.fragment:
        raise ValueError('PROXY_URL must not contain a path or query')
    if parts.scheme == 'socks5' and parts.username:
        # Chromium cannot authenticate to SOCKS5; mobile ports serve HTTP on the same port.
        raise ValueError('Chromium does not support SOCKS5 with a login; use http:// for PROXY_URL')
    host = f'[{parts.hostname}]' if ':' in parts.hostname else parts.hostname
    settings = {'server': f'{parts.scheme}://{host}:{port}'}
    if parts.username:
        settings.update(username=unquote(parts.username), password=unquote(parts.password or ''))
    return settings


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


@dataclass
class Config:
    bot_token: str
    owner_id: int | None
    city: str  # город по умолчанию для запросов
    feed: str  # раздел-лента, одна на город
    sections: list[str]  # какие разделы из ленты нужны (по адресу объявления)
    requests_per_minute: float  # бюджет загрузок ленты на этот IP, делится между городами
    night_requests_per_minute: float
    night_hours: tuple[int, int]
    direct_interface: str | None
    headless: bool
    data_dir: Path = field(default=ROOT / "data")
    save_debug_html: bool = False
    debug_html_retention_hours: float = 24
    cooldown_seconds: float = 900  # first local backoff after 429 without Retry-After
    max_backoff_seconds: float = 6 * 3600  # ceiling of the doubling local backoff
    verification_check_seconds: float = 5  # local DOM reads while a check is pending; no requests
    watchdog_seconds: float = 600  # a loop without a heartbeat this long restarts the process
    price_drop_percent: float = 10  # notify about a matching listing that got this much cheaper; 0 disables
    daily_report_hour: int = 9  # local hour of the daily report; -1 disables
    poll_jitter: float = 0.3  # random spread of the check interval, ±share
    browser_engine: str = 'playwright'
    fetch_mode: str = 'light_fetch'
    browser_channel: str = 'chrome'

    browser_session_mode: str = 'managed'
    cdp_endpoint: str = 'http://127.0.0.1:9222'
    cdp_profile_dir: Path = field(default=ROOT / 'data/chrome-cdp-profile')
    startup_diagnostics: bool = False
    extension_bridge_port: int = 9233
    proxy_url: str | None = field(default=None, repr=False)  # external proxy (mobile port, residential)
    daily_traffic_limit_mb: float = 0  # pause collection for the rest of the day after this much; 0 disables

    @property
    def feed_url(self) -> str:
        return feed_url(self.city, self.feed)

    @property
    def source_id(self) -> str:
        # A separate identity keeps restrictions and metrics of the proxy IP apart from the home IP.
        return 'proxy_browser' if self.proxy_url else 'local_browser'

    @property
    def browser_profile_dir(self) -> Path:
        # Cookies earned on the home IP are not carried to the proxy IP and back.
        return self.data_dir / ('browser-profile-proxy' if self.proxy_url else 'browser-profile')


def load_config() -> Config:
    _load_dotenv(ROOT / ".env")
    night = os.environ.get("NIGHT_HOURS", "2-7").split("-")
    owner = os.environ.get("OWNER_ID", "").strip()
    cfg = Config(
        bot_token=os.environ.get("BOT_TOKEN", ""),
        owner_id=int(owner) if owner else None,
        city=os.environ.get("CITY", "kazan"),
        feed=os.environ.get("FEED", "bytovaya_elektronika").strip(),
        sections=[s.strip() for s in os.environ.get(
            "SECTIONS", "tovary_dlya_kompyutera,nastolnye_kompyutery").split(",") if s.strip()],
        requests_per_minute=float(os.environ.get("REQUESTS_PER_MINUTE", "1")),
        night_requests_per_minute=float(os.environ.get("NIGHT_REQUESTS_PER_MINUTE", "1")),
        night_hours=(int(night[0]), int(night[1])),
        direct_interface=os.environ.get("DIRECT_INTERFACE", "").strip() or None,
        headless=os.environ.get("HEADLESS", "0") == "1",
        save_debug_html=os.environ.get("SAVE_DEBUG_HTML", "0") == "1",
        debug_html_retention_hours=float(os.environ.get("DEBUG_HTML_RETENTION_HOURS", "24")),
        cooldown_seconds=float(os.environ.get("COOLDOWN_SECONDS", "900")),
        max_backoff_seconds=float(os.environ.get("MAX_BACKOFF_SECONDS", str(6 * 3600))),
        verification_check_seconds=float(os.environ.get("VERIFICATION_CHECK_SECONDS", "5")),
        watchdog_seconds=float(os.environ.get("WATCHDOG_SECONDS", "600")),
        price_drop_percent=float(os.environ.get("PRICE_DROP_PERCENT", "10")),
        daily_report_hour=int(os.environ.get("DAILY_REPORT_HOUR", "9")),
        poll_jitter=float(os.environ.get("POLL_JITTER", "0.3")),
        extension_bridge_port=int(os.environ.get('EXTENSION_BRIDGE_PORT', '9233')),
        browser_session_mode=os.environ.get('BROWSER_SESSION_MODE', 'managed').strip().lower(),
        cdp_endpoint=os.environ.get('CDP_ENDPOINT', 'http://127.0.0.1:9222').strip(),
        cdp_profile_dir=Path(os.environ.get('CDP_PROFILE_DIR', str(ROOT / 'data/chrome-cdp-profile'))).expanduser().resolve(),
        startup_diagnostics=os.environ.get('STARTUP_DIAGNOSTICS', '0') == '1',
        browser_engine=os.environ.get('BROWSER_ENGINE', 'playwright').strip().lower(),
        fetch_mode=os.environ.get('FETCH_MODE', 'light_fetch').strip().lower(),
        browser_channel=os.environ.get('BROWSER_CHANNEL', 'chrome').strip().lower(),
        proxy_url=os.environ.get('PROXY_URL', '').strip() or None,
        daily_traffic_limit_mb=float(os.environ.get('DAILY_TRAFFIC_LIMIT_MB', '0')),
    )
    for name, allowed in (('browser_session_mode', ('managed', 'cdp_attach', 'chrome_extension')), ('browser_engine', ('playwright', 'patchright')),
                          ('fetch_mode', ('light_fetch', 'page_navigation')),
                          ('browser_channel', ('chrome', 'chromium'))):
        if getattr(cfg, name) not in allowed:
            raise ValueError(f'{name} must be one of: {", ".join(allowed)}')
    if cfg.proxy_url:
        parse_proxy_url(cfg.proxy_url)
        if cfg.browser_session_mode != 'managed':
            raise ValueError('PROXY_URL works only with BROWSER_SESSION_MODE=managed')
        if cfg.direct_interface:
            raise ValueError('Use either PROXY_URL or DIRECT_INTERFACE, not both')
    if not math.isfinite(cfg.daily_traffic_limit_mb) or cfg.daily_traffic_limit_mb < 0:
        raise ValueError('DAILY_TRAFFIC_LIMIT_MB must be 0 (off) or positive')
    if not 1024 <= cfg.extension_bridge_port <= 65535:
        raise ValueError('EXTENSION_BRIDGE_PORT must be 1024–65535')
    for name in ('requests_per_minute', 'night_requests_per_minute', 'cooldown_seconds', 'max_backoff_seconds',
                 'verification_check_seconds', 'watchdog_seconds', 'debug_html_retention_hours'):
        value = getattr(cfg, name)
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f'{name} must be a positive finite number')
    if not math.isfinite(cfg.price_drop_percent) or not 0 <= cfg.price_drop_percent < 100:
        raise ValueError('PRICE_DROP_PERCENT must be 0–99')
    if not math.isfinite(cfg.poll_jitter) or not 0 <= cfg.poll_jitter <= 0.5:
        raise ValueError('POLL_JITTER must be 0–0.5')
    if not -1 <= cfg.daily_report_hour <= 23:
        raise ValueError('DAILY_REPORT_HOUR must be -1 (off) or 0–23')
    if cfg.max_backoff_seconds < cfg.cooldown_seconds:
        raise ValueError('MAX_BACKOFF_SECONDS must not be less than COOLDOWN_SECONDS')
    if any(h < 0 or h > 23 for h in cfg.night_hours):
        raise ValueError('NIGHT_HOURS must contain hours 0–23')
    if cfg.owner_id is not None and cfg.owner_id <= 0:
        raise ValueError('OWNER_ID must be positive')
    return cfg
