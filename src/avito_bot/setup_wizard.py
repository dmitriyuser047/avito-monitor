"""Первичная настройка на новом компьютере: токен бота, город, VPN → файл .env.

Запуск: uv run avito-setup              — настроить заново
        uv run avito-setup --if-needed  — только если .env ещё нет (так вызывают стартовые скрипты)
"""

import ipaddress
import socket
import sys

import httpx

from .config import CITIES, ROOT

ENV = ROOT / ".env"
EXAMPLE = ROOT / ".env.example"


def ask(prompt: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{prompt}{suffix}: ").strip()
    return value or default


def check_token(token: str) -> str | None:
    """Имя бота, если токен рабочий."""
    try:
        r = httpx.get(f"https://api.telegram.org/bot{token}/getMe", timeout=15).json()
    except Exception as e:
        print(f"  Не удалось связаться с Telegram: {e}")
        return None
    return r["result"]["username"] if r.get("ok") else None


def city_slug(text: str) -> str:
    key = text.strip().lower().replace("ё", "е")
    return CITIES.get(key, key)


def vpn_detected() -> bool:
    """VPN в режиме TUN подменяет DNS: avito.ru резолвится в 198.18.x.x."""
    try:
        ip = ipaddress.ip_address(socket.gethostbyname("www.avito.ru"))
    except OSError:
        return False
    return ip in ipaddress.ip_network("198.18.0.0/15")


def lan_ip() -> str:
    """Адрес компьютера в домашней сети (192.168.x.x и т.п.)."""
    for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
        ip = ipaddress.ip_address(info[4][0])
        if ip.is_private and not ip.is_loopback and not str(ip).startswith("198.18"):
            return str(ip)
    return ""


def public_ip_info() -> str:
    try:
        d = httpx.get("https://ipinfo.io/json", timeout=10).json()
        return f"{d.get('ip')} — {d.get('city')}, {d.get('country')}, {d.get('org')}"
    except Exception:
        return "не удалось узнать"


def write_env(values: dict[str, str]) -> None:
    lines = []
    for line in EXAMPLE.read_text(encoding="utf-8").splitlines():
        key = line.split("=", 1)[0].strip()
        if not line.lstrip().startswith("#") and "=" in line and key in values:
            line = f"{key}={values[key]}"
        lines.append(line)
    ENV.write_text("\n".join(lines) + "\n", encoding="utf-8")
    try:
        ENV.chmod(0o600)
    except OSError:
        pass


def configured() -> bool:
    if not ENV.exists():
        return False
    return any(line.startswith("BOT_TOKEN=") and len(line.split("=", 1)[1].strip()) > 20
               for line in ENV.read_text(encoding="utf-8").splitlines())


def run() -> None:
    if "--if-needed" in sys.argv and configured():
        return

    print("\nНастройка Авито-монитора\n")
    print("1. Токен бота. Создайте бота у @BotFather в Telegram (/newbot) и вставьте токен.")
    print("   У каждого компьютера должен быть свой бот: один токен нельзя запускать на двух ПК сразу.\n")
    while True:
        token = ask("   Токен")
        name = check_token(token) if token else None
        if name:
            print(f"   Бот @{name} подключён.\n")
            break
        print("   Токен не подошёл, проверьте и вставьте ещё раз.")

    print("2. Город — как в адресе Авито (avito.ru/kazan/...) или по-русски.")
    print("   Например: Казань, Москва, Санкт-Петербург, Екатеринбург, Новосибирск.\n")
    city = city_slug(ask("   Город", "kazan"))
    print(f"   Проверить: https://www.avito.ru/{city}/tovary_dlya_kompyutera\n")

    direct = ""
    print("3. Проверка сети.")
    print(f"   Внешний IP: {public_ip_info()}")
    if vpn_detected():
        default = "en0" if sys.platform == "darwin" else lan_ip()
        print("   Похоже, включён VPN: Авито с ним сразу блокирует. Бот может ходить на Авито напрямую,")
        print("   через домашнюю сеть, а всё остальное останется в VPN.")
        if sys.platform == "darwin":
            print("   Укажите интерфейс домашней сети (обычно en0 — Wi-Fi).")
        else:
            print("   Укажите локальный IP этого компьютера в домашней сети (например 192.168.1.5).")
        direct = ask("   Интерфейс или IP", default)
    else:
        print("   VPN не обнаружен, обход не нужен.")
        print("   Важно: Авито пускает только домашний российский IP. На VPS и через VPN работать не будет.")

    write_env({"BOT_TOKEN": token, "OWNER_ID": "", "CITY": city, "DIRECT_INTERFACE": direct})
    print(f"\nГотово, настройки сохранены в {ENV.name}.")
    print(f"Теперь напишите боту @{name} команду /start — вы станете его владельцем.\n")


def main() -> None:
    try:
        run()
    except (KeyboardInterrupt, EOFError):
        print("\nНастройка прервана.")
        sys.exit(1)
