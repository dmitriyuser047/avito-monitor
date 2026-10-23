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
