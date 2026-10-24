"""Настройки из .env (или переменных окружения)."""

import os
import math
from dataclasses import dataclass, field
from pathlib import Path

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
    cooldown_seconds: float = 900

    @property
    def feed_url(self) -> str:
        return feed_url(self.city, self.feed)


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
    )
    for name in ('requests_per_minute', 'night_requests_per_minute', 'cooldown_seconds', 'debug_html_retention_hours'):
        value = getattr(cfg, name)
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f'{name} must be a positive finite number')
    if any(h < 0 or h > 23 for h in cfg.night_hours):
        raise ValueError('NIGHT_HOURS must contain hours 0–23')
    if cfg.owner_id is not None and cfg.owner_id <= 0:
        raise ValueError('OWNER_ID must be positive')
    return cfg
