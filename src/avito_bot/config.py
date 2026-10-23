"""Настройки из .env (или переменных окружения)."""

import os
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
CITY_NAMES = {slug: name.title().replace("-На-", "-на-") for name, slug in CITIES.items() if len(name) > 4 and name != "питер"}


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
    city: str
    feeds: list[str]
    poll_interval: float
    poll_jitter: float
    night_interval: float
    night_hours: tuple[int, int]
    direct_interface: str | None
    headless: bool
    data_dir: Path = field(default=ROOT / "data")

    @property
    def feed_urls(self) -> list[str]:
        # s=104 — сортировка «по дате»
        return [f"https://www.avito.ru/{self.city}/{feed}?s=104" for feed in self.feeds]


def load_config() -> Config:
    _load_dotenv(ROOT / ".env")
    night = os.environ.get("NIGHT_HOURS", "2-7").split("-")
    owner = os.environ.get("OWNER_ID", "").strip()
    return Config(
        bot_token=os.environ.get("BOT_TOKEN", ""),
        owner_id=int(owner) if owner else None,
        city=os.environ.get("CITY", "kazan"),
        feeds=[
            f.strip()
            for f in os.environ.get(
                "FEEDS",
                "tovary_dlya_kompyutera/komplektuyuschie,nastolnye_kompyutery",
            ).split(",")
            if f.strip()
        ],
        poll_interval=float(os.environ.get("POLL_INTERVAL", "10")),
        poll_jitter=float(os.environ.get("POLL_JITTER", "3")),
        night_interval=float(os.environ.get("NIGHT_INTERVAL", "60")),
        night_hours=(int(night[0]), int(night[1])),
        direct_interface=os.environ.get("DIRECT_INTERFACE", "").strip() or None,
        headless=os.environ.get("HEADLESS", "0") == "1",
    )
