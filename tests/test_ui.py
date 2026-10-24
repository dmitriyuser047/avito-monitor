import time
from pathlib import Path

from avito_bot import ui
from avito_bot.bot import parse_price
from avito_bot.db import DB, Subscription
from avito_bot.matcher import Item


def test_parse_price():
    assert parse_price("8000-15000") == (8000, 15000)
    assert parse_price("15 000 - 8 000") == (8000, 15000)
    assert parse_price("от 5000") == (5000, None)
    assert parse_price("до 20 000 ₽") == (None, 20000)
    assert parse_price("15к") == (None, 15000)
    assert parse_price("10к-20к") == (10000, 20000)
    assert parse_price("дорого") is None


def test_screens_render():
    s = Subscription(3, "1660 super", 8000, 15000, ["нерабоч", "ti"])
    screens = [
        ui.main_menu(paused=False, status="работает", last_ok=time.time() - 5, active=1, total=2, found_today=3, city="Казань"),
        ui.main_menu(paused=True, status="работает", last_ok=None, active=0, total=0, found_today=0, city="Казань"),
        ui.sub_list([s]), ui.sub_list([]), ui.sub_card(s, 4), ui.delete_confirm(s),
        ui.status_screen(status="работает", paused=False, last_ok=None, cycle=3.2, interval="10 ± 3 с",
                         feeds=["a/b"], seen=1234),
        ui.wiz_category(False), ui.wiz_category(True), ui.wiz_query("gpu", False), ui.wiz_query("", True),
        ui.wiz_price("1660 super", False), ui.wiz_minus("1660 super", ["куплю", "ti"], False),
        ui.wiz_confirm("1660 super", None, 15000, []),
    ]
    for text, kb in screens:
        assert text and kb.inline_keyboard
        for row in kb.inline_keyboard:
            for b in row:
                assert len(b.callback_data or "") <= 64
                assert b.style in (None, "primary", "success", "danger")
    assert "нерабочие" in ui.sub_card(s, 0)[0]
    assert ui.sub_title("", "gpu") == "Видеокарты, все"
    assert ui.sub_title("3060", "pc") == "Компьютеры: 3060"
    assert "Видеокарты (по слову" in ui.sub_card(Subscription(1, "видеокарта", None, None, []), 0)[0]


def test_item_caption_limit():
    item = Item(1, "Т" * 200, 35000, "https://www.avito.ru/x", "1 минуту назад", None, "р-н Советский", "о" * 2000)
    cap = ui.item_caption(item, [Subscription(1, "1660 super", None, None, [])])
    assert len(cap) < 1024
    kb = ui.item_keyboard(item, [Subscription(1, "1660 super", None, None, [])])
    assert kb.inline_keyboard[0][0].url == item.url


def test_db_migration_and_stats(tmp_path: Path):
    db = DB(tmp_path / "x.sqlite3")
    sid = db.add_subscription("rtx 3060", None, None, [])
    db.update_subscription(sid, enabled=False, minus_words=["майн"])
    s = db.subscription(sid)
    assert not s.enabled and s.minus_words == ["майн"]
    assert db.subscriptions(only_enabled=True) == []
    db.record_notification(10, [sid])
    assert db.found_today() == 1 and db.found_for(sid) == 1
    DB(tmp_path / "x.sqlite3")  # повторное открытие не ломает миграцию


def test_monitor_cities_and_budget(tmp_path: Path):
    from avito_bot.config import load_config
    from avito_bot.monitor import Monitor
    cfg = load_config()
    cfg.city, cfg.requests_per_minute, cfg.night_hours = "kazan", 5, (0, 0)
    db = DB(tmp_path / "m.sqlite3")
    mon = Monitor(cfg, db, None, None, None)
    assert mon.cities() == ["kazan"]
    db.add_subscription("rtx", None, None, [], "", ["moskva", "kazan"])
    db.add_subscription("ryzen", None, None, [], "", [])
    off = db.add_subscription("x", None, None, [], "", ["ufa"])
    db.update_subscription(off, enabled=False)
    assert mon.cities() == ["kazan", "moskva"]
    assert mon.city_period() == 24
    assert mon._slow_down() == 4 and mon.budget() == 4
    for _ in range(5):
        mon._slow_down()
    assert mon.budget() == 1  # не ниже MIN_RPM


def test_city_picker_and_cards():
    ui.DEFAULT_CITY = "kazan"
    text, kb = ui.city_picker(["moskva", "tula"], False, 5)
    labels = [b.text for row in kb.inline_keyboard for b in row]
    assert "✓ Москва" in labels and "✓ tula" in labels and "Казань" in labels
    s = Subscription(1, "3060", None, None, [], True, "gpu", ["moskva"])
    assert "Москва" in ui.short_sub(s)
    assert "Казань (из настроек)" in ui.sub_card(Subscription(1, "3060", None, None, []), 0)[0]


def test_members_access(tmp_path: Path):
    from avito_bot.bot import TelegramBot
    db = DB(tmp_path / "a.sqlite3")
    tg = TelegramBot("123456:TEST-token", db, owner_id=1, city="kazan")
    assert tg.allowed(1) and not tg.allowed(2)
    db.add_member(2, "Вася (@vasya)")
    assert tg.allowed(2) and tg.recipients() == [1, 2]
    text, kb = ui.access_screen(db.members())
    assert "Вася" in text and any("Убрать" in b.text for row in kb.inline_keyboard for b in row)
    owner_menu = [b.text for row in ui.main_menu(paused=False, status="работает", last_ok=None, active=0, total=0,
                                                   found_today=0, city="Казань", is_owner=True)[1].inline_keyboard for b in row]
    member_menu = [b.text for row in ui.main_menu(paused=False, status="работает", last_ok=None, active=0, total=0,
                                                    found_today=0, city="Казань", is_owner=False)[1].inline_keyboard for b in row]
    assert "Доступ" in owner_menu and "Доступ" not in member_menu


def test_real_time_in_caption():
    from datetime import datetime
    from avito_bot.matcher import Item, is_fresh
    now = datetime(2026, 10, 24, 12, 30).timestamp()
    pub = datetime(2026, 10, 24, 11, 47).timestamp()
    i = Item(1, "RTX 3060", 20000, "https://x", "1 час назад", None, "р-н Советский", "", "tovary_dlya_kompyutera",
             "kazan", published=pub, found_at=pub + 120)
    assert ui.when_text(i) == "опубликовано в 11:47"
    i.found_at = now
    assert ui.when_text(i) == "опубликовано в 11:47, в поиске с 12:30"
    i.published = None
    assert ui.when_text(i) == "появилось в поиске в 12:30"
    assert "1 час назад" not in ui.item_caption(i, [Subscription(1, "3060", None, None, [])])
    assert ui.clock(datetime(2026, 10, 23, 23, 10).timestamp(), now) == "вчера 23:10"
    # свежесть — по точному времени, а не по тексту
    old = Item(2, "x", 1, "", "1 час назад", None, "", "", published=now - 5 * 3600)
    assert not is_fresh(old, now)
    assert is_fresh(Item(3, "x", 1, "", "Вчера", None, "", "", published=now - 600), now)
