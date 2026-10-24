"""Тексты экранов и клавиатуры бота.

Принципы: одно сообщение — один экран (навигация редактирует его на месте),
цвет кнопок только для главного действия (primary/success) и удаления (danger),
без декоративных эмодзи.
"""

import html
import time

from aiogram.filters.callback_data import CallbackData
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from .db import Subscription
from .config import POPULAR_CITIES, city_name
from .matcher import BY_KEY, CATEGORIES, Item, split_query

DEFAULT_CITY = "kazan"  # выставляется ботом из настроек

# Частые минус-слова: (подпись на кнопке, основа слова для поиска в названии)
MINUS_PRESETS = [
    ("Б/у", "б у"),
    ("Ноутбуки", "ноутбук"),
    ("Нерабочие", "нерабоч"),
    ("На запчасти", "запчаст"),
    ("Обмен", "обмен"),
    ("Майнинг", "майн"),
]
MINUS_LABELS = {stem: label.lower() for label, stem in MINUS_PRESETS}

PRICE_PRESETS = [
    ("Любая", "any"),
    ("до 5 000", "0-5000"),
    ("до 10 000", "0-10000"),
    ("до 15 000", "0-15000"),
    ("до 20 000", "0-20000"),
    ("до 30 000", "0-30000"),
    ("до 50 000", "0-50000"),
    ("до 100 000", "0-100000"),
]


class Nav(CallbackData, prefix="nav"):
    to: str  # menu, list, status, add, pause, resume, test


class SubCb(CallbackData, prefix="sub"):
    action: str  # open, toggle, price, minus, delete, delete_ok
    id: int


class Wiz(CallbackData, prefix="wiz"):
    action: str  # price, minus, done, save, back, cancel
    value: str = ""


class MemberCb(CallbackData, prefix="member"):
    action: str  # remove, remove_ok
    id: int


class ItemCb(CallbackData, prefix="item"):
    action: str  # hide, mute
    sub_id: int = 0


def _btn(text: str, cb: CallbackData, style: str | None = None) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=cb.pack(), style=style)


# --- форматирование ---

def money(value: int) -> str:
    return f"{value:,}".replace(",", " ")


def price_range(min_price: int | None, max_price: int | None) -> str:
    if min_price and max_price:
        return f"{money(min_price)} – {money(max_price)} ₽"
    if max_price:
        return f"до {money(max_price)} ₽"
    if min_price:
        return f"от {money(min_price)} ₽"
    return "любая"


def minus_text(words: list[str]) -> str:
    return ", ".join(MINUS_LABELS.get(w, w) for w in words) if words else "нет"


def ago(ts: float | None) -> str:
    if not ts:
        return "ещё не было"
    sec = int(time.time() - ts)
    if sec < 60:
        return f"{sec} с назад"
    if sec < 3600:
        return f"{sec // 60} мин назад"
    return f"{sec // 3600} ч назад"


EXAMPLES = {
    "gpu": "<code>1660 super</code>, <code>rtx 3060 12gb</code>, <code>rx 6700 xt</code>",
    "cpu": "<code>ryzen 5600</code>, <code>i5 12400</code>, <code>xeon e5 2680</code>",
    "ram": "<code>ddr4 16gb</code>, <code>ddr5 32gb</code>, <code>3200</code>",
    "mobo": "<code>b450</code>, <code>am4</code>, <code>lga 1700</code>",
    "storage": "<code>ssd 1tb</code>, <code>nvme 512gb</code>, <code>hdd 2tb</code>",
    "psu": "<code>600w</code>, <code>deepcool</code>, <code>gold</code>",
    "cooling": "<code>сжо 360</code>, <code>deepcool ak620</code>",
    "case": "<code>deepcool</code>, <code>atx</code>",
    "pc": "<code>3060</code>, <code>rtx 4070</code>, <code>ryzen 5600</code> — ищется и в описании сборки",
}


def sub_title(query: str, category: str) -> str:
    cat = BY_KEY.get(category)
    if cat:
        return f"{cat.name}: {query}" if query else f"{cat.name}, все"
    return query


def category_text(s: Subscription) -> str:
    if s.category in BY_KEY:
        return BY_KEY[s.category].name
    cat, _ = split_query(s)
    return f"{cat.name} (по слову в запросе)" if cat else "любой"


def cities_text(cities: list[str]) -> str:
    if not cities:
        return f"{city_name(DEFAULT_CITY)} (из настроек)"
    return ", ".join(city_name(c) for c in cities)


def short_sub(s: Subscription) -> str:
    text = sub_title(s.query, s.category)
    if s.cities and s.cities != [DEFAULT_CITY]:
        text += f" · {cities_text(s.cities)}"
    if s.min_price or s.max_price:
        text += f" · {price_range(s.min_price, s.max_price)}"
    return text if s.enabled else f"{text} · выкл"


# --- экраны ---

def main_menu(*, paused: bool, status: str, last_ok: float | None, active: int, total: int,
              found_today: int, city: str, is_owner: bool = True) -> tuple[str, InlineKeyboardMarkup]:
    state = "на паузе" if paused else status
    text = (
        "<b>Авито-монитор</b>\n"
        f"{html.escape(city)} · комплектующие и ПК\n\n"
        f"Слежение: <b>{state}</b>\n"
        f"Последняя проверка: {ago(last_ok)}\n"
        f"Запросов: {active} активных из {total}\n"
        f"Найдено сегодня: {found_today}"
    )
    kb = InlineKeyboardBuilder()
    kb.row(_btn("Добавить запрос", Nav(to="add"), "primary"))
    kb.row(_btn("Мои запросы", Nav(to="list")), _btn("Состояние", Nav(to="status")))
    if paused:
        kb.row(_btn("Продолжить слежение", Nav(to="resume"), "success"))
    else:
        kb.row(_btn("Поставить на паузу", Nav(to="pause")))
    if is_owner:
        kb.row(_btn("Доступ", Nav(to="access")))
    return text, kb.as_markup()


def access_screen(members: list[tuple[int, str]]) -> tuple[str, InlineKeyboardMarkup]:
    if members:
        names = "\n".join(f"— {html.escape(name)}" for _, name in members)
        text = (
            "<b>Доступ</b>\n\n"
            f"Кроме вас ботом пользуются:\n{names}\n\n"
            "Запросы общие: участники видят и меняют тот же список и получают те же уведомления. "
            "Выдавать и забирать доступ можете только вы."
        )
    else:
        text = (
            "<b>Доступ</b>\n\n"
            "Пока ботом пользуетесь только вы. Пригласите человека — он будет получать те же уведомления "
            "и сможет менять общий список запросов."
        )
    kb = InlineKeyboardBuilder()
    kb.row(_btn("Пригласить", Nav(to="invite"), "primary"))
    for uid, name in members:
        kb.row(_btn(f"Убрать: {name}", MemberCb(action="remove", id=uid)))
    kb.row(_btn("Назад", Nav(to="menu")))
    return text, kb.as_markup()


def invite_screen(link: str) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        "<b>Приглашение</b>\n\n"
        "Отправьте эту ссылку человеку. Он откроет её, нажмёт «Запустить» — и получит доступ.\n\n"
        f"{html.escape(link)}\n\n"
        "Ссылка одноразовая и действует 24 часа. Когда человек подключится, бот вам напишет."
    )
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="Скопировать ссылку", copy_text={"text": link}, style="primary"))
    kb.row(_btn("Назад", Nav(to="access")))
    return text, kb.as_markup()


def remove_member_confirm(uid: int, name: str) -> tuple[str, InlineKeyboardMarkup]:
    text = f"Забрать доступ у <b>{html.escape(name)}</b>?\n\nУведомления ему больше не будут приходить."
    kb = InlineKeyboardBuilder()
    kb.row(
        _btn("Забрать доступ", MemberCb(action="remove_ok", id=uid), "danger"),
        _btn("Отмена", Nav(to="access")),
    )
    return text, kb.as_markup()


def sub_list(subs: list[Subscription]) -> tuple[str, InlineKeyboardMarkup]:
    kb = InlineKeyboardBuilder()
    if subs:
        text = "<b>Мои запросы</b>\n\nНажмите на запрос, чтобы изменить или удалить его."
        for s in subs:
            kb.row(_btn(short_sub(s), SubCb(action="open", id=s.id)))
    else:
        text = (
            "<b>Мои запросы</b>\n\n"
            "Пока пусто. Добавьте модель, которую ищете, — и бот пришлёт новое объявление, "
            "как только оно появится на Авито."
        )
    kb.row(_btn("Добавить запрос", Nav(to="add"), "primary"))
    kb.row(_btn("Назад", Nav(to="menu")))
    return text, kb.as_markup()


def sub_card(s: Subscription, found: int) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        f"<b>{html.escape(sub_title(s.query, s.category))}</b>\n\n"
        f"Раздел: {html.escape(category_text(s))}\n"
        f"Города: {html.escape(cities_text(s.cities))}\n"
        f"Модель: {html.escape(s.query) if s.query else 'любая'}\n"
        f"Цена: {price_range(s.min_price, s.max_price)}\n"
        f"Исключить: {html.escape(minus_text(s.minus_words))}\n"
        f"Статус: {'активен' if s.enabled else 'выключен'}\n"
        f"Найдено объявлений: {found}"
    )
    kb = InlineKeyboardBuilder()
    kb.row(
        _btn("Раздел", SubCb(action="cat", id=s.id)),
        _btn("Модель", SubCb(action="model", id=s.id)),
    )
    kb.row(_btn("Города", SubCb(action="city", id=s.id)))
    kb.row(
        _btn("Цена", SubCb(action="price", id=s.id)),
        _btn("Исключения", SubCb(action="minus", id=s.id)),
    )
    kb.row(
        _btn("Выключить", SubCb(action="toggle", id=s.id)) if s.enabled
        else _btn("Включить", SubCb(action="toggle", id=s.id), "success"),
        _btn("Удалить", SubCb(action="delete", id=s.id), "danger"),
    )
    kb.row(_btn("Назад к списку", Nav(to="list")))
    return text, kb.as_markup()


def delete_confirm(s: Subscription) -> tuple[str, InlineKeyboardMarkup]:
    text = f"Удалить запрос <b>{html.escape(sub_title(s.query, s.category))}</b>?\n\nУведомления по нему перестанут приходить."
    kb = InlineKeyboardBuilder()
    kb.row(
        _btn("Удалить", SubCb(action="delete_ok", id=s.id), "danger"),
        _btn("Отмена", SubCb(action="open", id=s.id)),
    )
    return text, kb.as_markup()


def status_screen(*, status: str, paused: bool, last_ok: float | None, cycle: float | None,
                  interval: str, feeds: list[str], seen: int) -> tuple[str, InlineKeyboardMarkup]:
    feeds_text = ", ".join(html.escape(f) for f in feeds)
    text = (
        "<b>Состояние</b>\n\n"
        f"Слежение: <b>{'на паузе' if paused else status}</b>\n"
        f"Последняя успешная проверка: {ago(last_ok)}\n"
        f"Длительность цикла: {f'{cycle:.1f} с' if cycle else '—'}\n"
        f"Интервал опроса: {interval}\n"
        f"Просмотрено объявлений: {money(seen)}\n\n"
        f"Города: {feeds_text}"
    )
    kb = InlineKeyboardBuilder()
    kb.row(_btn("Обновить", Nav(to="status")), _btn("Проверить уведомление", Nav(to="test")))
    kb.row(_btn("Назад", Nav(to="menu")))
    return text, kb.as_markup()


# --- мастер добавления ---

def _wiz_head(step: int, edit: bool) -> str:
    return "<b>Изменение запроса</b>\n\n" if edit else f"<b>Новый запрос</b> · шаг {step} из 4\n\n"


def wiz_category(edit: bool) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        _wiz_head(1, edit)
        + "Где искать?\n\n"
        "Раздел бот определяет сам — по названию и модели: «RTX 3090 galax» попадёт в видеокарты, "
        "«Игровой пк» — в компьютеры. Поэтому «Видеокарты: 3060» не пришлёт готовый ПК с 3060."
    )
    kb = InlineKeyboardBuilder()
    for cat in CATEGORIES:
        kb.button(text=cat.name, callback_data=Wiz(action="cat", value=cat.key).pack())
    kb.adjust(2)
    kb.row(_btn("Любой раздел", Wiz(action="cat", value="any")))
    kb.row(_btn("Назад", Wiz(action="back")) if edit else _btn("Отмена", Wiz(action="cancel")))
    return text, kb.as_markup()


def wiz_query(category: str, edit: bool) -> tuple[str, InlineKeyboardMarkup]:
    cat = BY_KEY.get(category)
    if cat:
        text = (
            _wiz_head(2, edit)
            + f"Раздел: <b>{cat.name}</b>\n\n"
            "Какая модель? Напишите её так, как пишут в объявлениях, или нажмите «Любая модель» — "
            "тогда придут все новые объявления раздела.\n\n"
            f"Например: {EXAMPLES[cat.key]}"
        )
    else:
        text = (
            _wiz_head(2, edit)
            + "Раздел: <b>любой</b>\n\n"
            "Что ищем? Модель найдётся везде — и отдельная деталь, и компьютер с ней.\n\n"
            "Например: <code>1660 super</code>, <code>rtx 3060</code>, <code>ryzen 5600</code>"
        )
    text += "\n\nНе важно, как написано в объявлении: «RTX3060Ti», «ртх 3060 ти» и «rtx 3060 ti» — одно и то же."
    kb = InlineKeyboardBuilder()
    if cat:
        kb.row(_btn("Любая модель", Wiz(action="anymodel"), "primary"))
    kb.row(_btn("Назад", Wiz(action="back")), _btn("Отмена", Wiz(action="cancel")))
    return text, kb.as_markup()


def wiz_price(query: str, edit: bool) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        _wiz_head(3, edit)
        + f"Запрос: <b>{html.escape(query)}</b>\n\n"
        "Какая цена подходит? Выберите вариант или напишите свой диапазон, "
        "например <code>8000-15000</code> или <code>от 5000</code>."
    )
    kb = InlineKeyboardBuilder()
    for label, value in PRICE_PRESETS:
        kb.button(text=label, callback_data=Wiz(action="price", value=value).pack())
    kb.adjust(2)
    kb.row(_btn("Назад", Wiz(action="back")), _btn("Отмена", Wiz(action="cancel")))
    return text, kb.as_markup()


def wiz_minus(query: str, minus: list[str], edit: bool) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        _wiz_head(4, edit)
        + f"Запрос: <b>{html.escape(query)}</b>\n\n"
        "Объявления о покупке («Куплю…») не приходят никогда. Что ещё не присылать? Отметьте варианты или напишите свои слова через пробел.\n\n"
        f"Сейчас исключено: {html.escape(minus_text(minus))}"
    )
    kb = InlineKeyboardBuilder()
    for label, stem in MINUS_PRESETS:
        mark = "✓ " if stem in minus else ""
        kb.button(text=f"{mark}{label}", callback_data=Wiz(action="minus", value=stem).pack())
    kb.adjust(3)
    custom = [w for w in minus if w not in MINUS_LABELS]
    if custom:
        kb.row(_btn("Сбросить свои слова", Wiz(action="minus", value="*clear")))
    kb.row(_btn("Готово" if edit else "Дальше", Wiz(action="done"), "primary"))
    kb.row(_btn("Назад", Wiz(action="back")), _btn("Отмена", Wiz(action="cancel")))
    return text, kb.as_markup()


def city_picker(selected: list[str], edit: bool, rpm: float) -> tuple[str, InlineKeyboardMarkup]:
    period = lambda n: f"~{n * 60 / max(rpm, 0.1):.0f} с"
    text = (
        ("<b>Изменение запроса</b>\n\n" if edit else "<b>Города</b>\n\n")
        + "Где искать? Отметьте один или несколько городов. Нужен другой — напишите его название.\n\n"
        f"Все города делят общий лимит запросов к Авито ({rpm:g} в минуту): один город проверяется "
        f"раз в {period(1)}, два — каждый раз в {period(2)}, три — раз в {period(3)}. "
        "Чем больше городов, тем позже приходят уведомления.\n\n"
        f"Выбрано: {html.escape(cities_text(selected))}"
    )
    kb = InlineKeyboardBuilder()
    for slug in POPULAR_CITIES + [c for c in selected if c not in POPULAR_CITIES]:
        mark = "✓ " if slug in selected else ""
        kb.button(text=f"{mark}{city_name(slug)}", callback_data=Wiz(action="city", value=slug).pack())
    kb.adjust(2)
    kb.row(_btn("Готово", Wiz(action="citydone"), "primary"))
    kb.row(_btn("Назад", Wiz(action="back")))
    return text, kb.as_markup()


def wiz_confirm(query: str, pmin: int | None, pmax: int | None, minus: list[str],
                cities: list[str] | None = None) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        "<b>Проверьте запрос</b>\n\n"
        f"Ищем: <b>{html.escape(query)}</b>\n"
        f"Города: {html.escape(cities_text(cities or []))}\n"
        f"Цена: {price_range(pmin, pmax)}\n"
        f"Исключить: {html.escape(minus_text(minus))}\n\n"
        "Уведомления будут приходить только о новых объявлениях — тех, что появятся после сохранения."
    )
    kb = InlineKeyboardBuilder()
    kb.row(_btn("Изменить города", Wiz(action="cities")))
    kb.row(_btn("Сохранить", Wiz(action="save"), "success"))
    kb.row(_btn("Назад", Wiz(action="back")), _btn("Отмена", Wiz(action="cancel")))
    return text, kb.as_markup()


# --- уведомление об объявлении ---

def item_caption(item: Item, subs: list[Subscription]) -> str:
    price = f"{money(item.price)} ₽" if item.price is not None else "цена не указана"
    place = city_name(item.city) if item.city and item.city != DEFAULT_CITY else ""
    meta = " · ".join(x for x in (html.escape(place), html.escape(item.location), html.escape(item.date)) if x)
    queries = ", ".join(html.escape(sub_title(s.query, s.category)) for s in subs)
    desc = html.escape(item.description[:220]).strip()
    return (
        f"<b>{html.escape(item.title)}</b>\n"
        f"<b>{price}</b>\n"
        f"{meta}\n"
        + (f"\n<blockquote expandable>{desc}</blockquote>\n" if desc else "\n")
        + f"Запрос: {queries}"
    )


def item_keyboard(item: Item, subs: list[Subscription]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="Открыть на Авито", url=item.url, style="primary"))
    row = [_btn("Скрыть", ItemCb(action="hide"))]
    if len(subs) == 1 and subs[0].id:
        row.append(_btn("Больше не искать", ItemCb(action="mute", sub_id=subs[0].id)))
    kb.row(*row)
    return kb.as_markup()


def alert_text(text: str) -> str:
    """Первая строка — заголовок."""
    title, _, body = text.partition("\n")
    return f"<b>{html.escape(title)}</b>" + (f"\n{html.escape(body)}" if body else "")


def alert_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(_btn("Состояние", Nav(to="status")))
    return kb.as_markup()
