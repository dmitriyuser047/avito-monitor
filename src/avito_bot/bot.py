"""Telegram-бот: меню, мастер добавления запросов, карточки запросов, уведомления."""

import html
import logging
import re
import secrets

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    BotCommand,
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardMarkup,
    LinkPreviewOptions,
    Message,
)

from . import ui
from .config import CITY_NAMES, city_slug
from .db import DB, Subscription
from .matcher import Item, parse_add
from .ui import ItemCb, MemberCb, Nav, SubCb, Wiz

log = logging.getLogger(__name__)

COMMANDS = [
    BotCommand(command="menu", description="Главное меню"),
    BotCommand(command="add", description="Новый запрос"),
    BotCommand(command="list", description="Мои запросы"),
    BotCommand(command="status", description="Состояние бота"),
    BotCommand(command="test", description="Проверить уведомление"),
    BotCommand(command="health", description="Источник и очередь доставки"),
    BotCommand(command="unblock", description="Открыть проверку Авито вручную"),
]
DESCRIPTION = (
    "Проверяю новые объявления Авито о комплектующих и компьютерах и отправляю подходящие в Telegram. "
    "Добавьте модель, которую ищете, — например 1660 super, — цену и города поиска."
)
SHORT_DESCRIPTION = "Новые объявления Авито о комплектующих и ПК — сразу в Telegram."


class Wizard(StatesGroup):
    category = State()
    query = State()
    price = State()
    minus = State()
    confirm = State()
    city = State()


EDIT_STEPS = {
    "cat": Wizard.category,
    "model": Wizard.query,
    "price": Wizard.price,
    "minus": Wizard.minus,
    "city": Wizard.city,
}


def parse_price(text: str) -> tuple[int | None, int | None] | None:
    """'8000-15000', 'от 5000', 'до 20 000', '15000' → (мин, макс)."""
    t = text.lower().replace(" ", "").replace(" ", "").replace("₽", "").replace("руб", "")
    t = t.replace("к", "000") if re.fullmatch(r"[\dк\-–отд]+", t) else t
    if m := re.fullmatch(r"(\d+)[-–](\d+)", t):
        a, b = sorted((int(m[1]), int(m[2])))
        return a, b
    if m := re.fullmatch(r"от(\d+)", t):
        return int(m[1]), None
    if m := re.fullmatch(r"(?:до)?(\d+)", t):
        return None, int(m[1])
    return None


class TelegramBot:
    def __init__(self, token: str, db: DB, owner_id: int | None, city: str = ""):
        self.bot = Bot(token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        self.db = db
        stored = db.get("owner_id")
        self.owner_id = owner_id or (int(stored) if stored else None)
        self.city = city
        ui.DEFAULT_CITY = city
        self.monitor = None  # выставляются в __init__.py
        self.fetch_image = None
        self.dp = Dispatcher()
        self.dp.include_router(self._start_router())
        self.dp.include_router(self._router())

    async def setup(self) -> None:
        """Меню команд и описание бота — то, что видно до /start и по кнопке «Меню»."""
        try:
            await self.bot.set_my_commands(COMMANDS)
            await self.bot.set_my_description(DESCRIPTION)
            await self.bot.set_my_short_description(SHORT_DESCRIPTION)
        except Exception as e:
            log.warning("Не удалось обновить меню бота: %s", e)

    # ---------- экраны ----------

    def _menu(self, uid: int) -> tuple[str, InlineKeyboardMarkup]:
        subs = self.db.subscriptions()
        mon = self.monitor
        return ui.main_menu(
            status=mon.status, last_ok=mon.last_ok,
            active=sum(s.enabled for s in subs), total=len(subs),
            found_today=self.db.found_today(), city=self._city_name(), is_owner=uid == self.owner_id,
        )

    def _access(self) -> tuple[str, InlineKeyboardMarkup]:
        return ui.access_screen(self.db.members())

    # ---------- кто имеет доступ ----------

    def allowed(self, user_id: int) -> bool:
        return user_id == self.owner_id or any(uid == user_id for uid, _ in self.db.members())

    def recipients(self) -> list[int]:
        return ([self.owner_id] if self.owner_id else []) + [uid for uid, _ in self.db.members()]

    async def invite_link(self) -> str:
        code = secrets.token_urlsafe(9)
        self.db.create_invite(code)
        me = await self.bot.get_me()
        return f"https://t.me/{me.username}?start=inv_{code}"

    def _status(self) -> tuple[str, InlineKeyboardMarkup]:
        mon = self.monitor
        cfg = mon.cfg
        cities = mon.cities()
        interval = (f"{mon.budget():g} загрузок в минуту, каждый город — раз в ~{mon.city_period():.0f} с\n"
                    f"Ночью ({cfg.night_hours[0]}:00–{cfg.night_hours[1]}:00) — {cfg.night_requests_per_minute:g} в минуту")
        return ui.status_screen(
            status=mon.status, last_ok=mon.last_ok, cycle=mon.last_cycle_sec,
            interval=interval, feeds=[ui.city_name(c) for c in cities], seen=self.db.seen_count(),
        )

    def _card(self, sub_id: int) -> tuple[str, InlineKeyboardMarkup] | None:
        s = self.db.subscription(sub_id)
        return ui.sub_card(s, self.db.found_for(s.id)) if s else None

    def _city_name(self) -> str:
        return CITY_NAMES.get(self.city, self.city)

    async def _show(self, event: Message | CallbackQuery, screen: tuple[str, InlineKeyboardMarkup]) -> Message:
        """Показать экран: в ответ на кнопку — заменить текущее сообщение, иначе — новым."""
        text, kb = screen
        if isinstance(event, CallbackQuery):
            msg = event.message
            if msg and msg.text is not None:
                try:
                    return await msg.edit_text(text, reply_markup=kb)
                except TelegramBadRequest as e:
                    if "not modified" in str(e):
                        return msg
            return await self.bot.send_message(event.from_user.id, text, reply_markup=kb)
        return await event.answer(text, reply_markup=kb)

    async def _show_wizard(self, chat_id: int, state: FSMContext, screen: tuple[str, InlineKeyboardMarkup]) -> None:
        """Мастер живёт в одном сообщении: после ввода текста редактируем его."""
        text, kb = screen
        msg_id = (await state.get_data()).get("msg_id")
        if msg_id:
            try:
                await self.bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, reply_markup=kb)
                return
            except TelegramBadRequest as e:
                if "not modified" in str(e):
                    return
        msg = await self.bot.send_message(chat_id, text, reply_markup=kb)
        await state.update_data(msg_id=msg.message_id)

    # ---------- роутеры ----------

    def _start_router(self) -> Router:
        r = Router()

        @r.message(Command("start"))
        async def start(m: Message, command: CommandObject, state: FSMContext):
            user = m.from_user
            if self.owner_id is None:
                await m.answer("Владелец ещё не задан. Укажите OWNER_ID в настройках на компьютере бота.")
                return
            arg = (command.args or "").strip()
            if arg.startswith("inv_") and not self.allowed(user.id):
                if self.db.use_invite(arg[4:]):
                    name = user.full_name + (f" (@{user.username})" if user.username else "")
                    self.db.add_member(user.id, name)
                    log.info("Новый участник: %s %s", user.id, name)
                    await m.answer("Доступ получен. Вы будете получать уведомления о новых объявлениях, "
                                   "а запросы можно смотреть и менять в меню — они общие с владельцем бота.")
                    try:
                        await self.bot.send_message(self.owner_id, f"<b>{html.escape(name)}</b> получил доступ к боту.")
                    except Exception:
                        pass
                else:
                    await m.answer("Ссылка-приглашение недействительна: её уже использовали или прошло больше суток. "
                                   "Попросите владельца бота прислать новую.")
                    return
            if not self.allowed(user.id):
                await m.answer("Это личный бот. Доступ выдаёт его владелец по ссылке-приглашению.")
                return
            await state.clear()
            await self._show(m, self._menu(m.from_user.id))

        return r

    def _router(self) -> Router:
        r = Router()
        allowed = F.from_user.func(lambda u: self.allowed(u.id))
        r.message.filter(allowed)
        r.callback_query.filter(allowed)
        is_owner = F.from_user.func(lambda u: u.id == self.owner_id)

        # --- доступ (только владелец) ---

        @r.callback_query(Nav.filter(F.to == "access"), is_owner)
        async def access(c: CallbackQuery):
            await self._show(c, self._access())
            await c.answer()

        @r.callback_query(Nav.filter(F.to == "invite"), is_owner)
        async def invite(c: CallbackQuery):
            await self._show(c, ui.invite_screen(await self.invite_link()))
            await c.answer()

        @r.callback_query(MemberCb.filter(), is_owner)
        async def member_action(c: CallbackQuery, callback_data: MemberCb):
            if callback_data.action == "remove":
                name = dict(self.db.members()).get(callback_data.id, "")
                await self._show(c, ui.remove_member_confirm(callback_data.id, name))
            elif callback_data.action == "remove_ok":
                self.db.remove_member(callback_data.id)
                await c.answer("Доступ убран")
                await self._show(c, self._access())
                return
            await c.answer()

        # --- команды ---

        @r.message(Command("menu"))
        async def cmd_menu(m: Message, state: FSMContext):
            await state.clear()
            await self._show(m, self._menu(m.from_user.id))

        @r.message(Command("list"))
        async def cmd_list(m: Message, state: FSMContext):
            await state.clear()
            await self._show(m, ui.sub_list(self.db.subscriptions()))

        @r.message(Command("status"))
        async def cmd_status(m: Message, state: FSMContext):
            await state.clear()
            await self._show(m, self._status())

        @r.message(Command("health"))
        async def cmd_health(m: Message):
            await m.answer(self.monitor.health_text())

        @r.message(Command("unblock"), is_owner)
        async def cmd_unblock(m: Message):
            try:
                await m.answer(await self.monitor.open_verification())
            except Exception:
                await m.answer("Браузер недоступен. Проверьте /health и перезапустите бот на компьютере.")

        @r.message(Command("test"))
        async def cmd_test(m: Message):
            try:
                await self._send_test(m.chat.id)
            except Exception as exc:
                log.warning("Test notification failed: %s", type(exc).__name__)

        @r.message(Command("pause", "resume"))
        async def cmd_removed_pause(m: Message):
            # 2.0 has no user pause: the bot resumes by itself when the source allows it.
            await m.answer("Паузы и /resume больше нет: бот сам продолжает сбор, когда Авито это разрешает, "
                           "и сам замечает пройденную проверку в окне Chrome. Подробности — /health.")

        @r.message(Command("add"))
        async def cmd_add(m: Message, command: CommandObject, state: FSMContext):
            await state.clear()
            if command.args:
                # быстрый ввод одной строкой: /add 1660 super | 8000-15000 | -ti
                query, pmin, pmax, minus = parse_add(command.args)
                if query:
                    await state.set_state(Wizard.confirm)
                    await state.update_data(query=query, category="", pmin=pmin, pmax=pmax, minus=minus)
                    msg = await self._show(m, ui.wiz_confirm(query, pmin, pmax, minus))
                    await state.update_data(msg_id=msg.message_id)
                    return
            await self._start_wizard(m, state)

        # --- навигация ---

        @r.callback_query(Nav.filter())
        async def nav(c: CallbackQuery, callback_data: Nav, state: FSMContext):
            to = callback_data.to
            if to == "add":
                await self._start_wizard(c, state)
            elif to == "list":
                await self._show(c, ui.sub_list(self.db.subscriptions()))
            elif to == "status":
                await self._show(c, self._status())
            elif to == "test":
                await self._send_test(c.from_user.id)
            else:
                await state.clear()
                await self._show(c, self._menu(c.from_user.id))
            await c.answer()

        # --- карточка запроса ---

        @r.callback_query(SubCb.filter())
        async def sub_action(c: CallbackQuery, callback_data: SubCb, state: FSMContext):
            s = self.db.subscription(callback_data.id)
            if not s:
                await c.answer("Запрос уже удалён")
                await self._show(c, ui.sub_list(self.db.subscriptions()))
                return
            action = callback_data.action
            if action == "toggle":
                self.db.update_subscription(s.id, enabled=not s.enabled)
                await c.answer("Запрос выключен" if s.enabled else "Запрос включён")
                await self._show(c, self._card(s.id))
                return
            if action == "delete":
                await self._show(c, ui.delete_confirm(s))
            elif action == "delete_ok":
                self.db.delete_subscription(s.id)
                await c.answer("Запрос удалён")
                await self._show(c, ui.sub_list(self.db.subscriptions()))
                return
            elif action in EDIT_STEPS:
                await state.clear()
                await state.set_state(EDIT_STEPS[action])
                await state.update_data(edit_id=s.id, query=s.query, category=s.category, pmin=s.min_price,
                                        pmax=s.max_price, minus=s.minus_words, cities=s.cities,
                                        msg_id=c.message.message_id)
                await self._show(c, self._wizard_screen(EDIT_STEPS[action], await state.get_data()))
            else:
                await self._show(c, self._card(s.id))
            await c.answer()

        # --- мастер: ввод текста ---

        @r.message(Wizard.query, F.text)
        async def wiz_query_text(m: Message, state: FSMContext):
            await _delete(m)
            query, _, _, extra_minus = parse_add(m.text)
            if not query:
                return
            data = await state.get_data()
            if data.get("edit_id"):
                await self._save_edit(m.chat.id, state, query=query)
                return
            await state.update_data(query=query, minus=list(dict.fromkeys(data.get("minus", []) + extra_minus)))
            await self._goto(m.chat.id, state, Wizard.price)

        @r.message(Wizard.price, F.text)
        async def wiz_price_text(m: Message, state: FSMContext):
            await _delete(m)
            price = parse_price(m.text)
            if price is None:
                text, kb = self._wizard_screen(Wizard.price, await state.get_data())
                text += "\n\nНе понял цену. Напишите, например, <code>8000-15000</code>, <code>до 20000</code> или <code>от 5000</code>."
                await self._show_wizard(m.chat.id, state, (text, kb))
                return
            await self._after_price(m.chat.id, state, *price)

        @r.message(Wizard.minus, F.text)
        async def wiz_minus_text(m: Message, state: FSMContext):
            await _delete(m)
            data = await state.get_data()
            minus = list(data.get("minus", []))
            for w in m.text.lower().replace(",", " ").split():
                w = w.lstrip("-")
                if w and w not in minus:
                    minus.append(w)
            await state.update_data(minus=minus)
            await self._goto(m.chat.id, state, Wizard.minus)

        @r.message(Wizard.city, F.text)
        async def wiz_city_text(m: Message, state: FSMContext):
            await _delete(m)
            data = await state.get_data()
            cities = list(data.get("cities", []))
            for part in m.text.split(","):
                slug = city_slug(part)
                if slug and slug not in cities:
                    cities.append(slug)
            await state.update_data(cities=cities)
            await self._goto(m.chat.id, state, Wizard.city)

        @r.message(Wizard.category, F.text)
        @r.message(Wizard.confirm, F.text)
        async def wiz_ignore_text(m: Message):
            await _delete(m)

        # --- мастер: кнопки ---

        @r.callback_query(Wiz.filter())
        async def wiz(c: CallbackQuery, callback_data: Wiz, state: FSMContext):
            data = await state.get_data()
            current = await state.get_state()
            if current is None:
                await c.answer("Этот шаг устарел")
                await self._show(c, self._menu(c.from_user.id))
                return
            await state.update_data(msg_id=c.message.message_id)
            edit_id = data.get("edit_id")
            action = callback_data.action
            chat = c.from_user.id

            if action == "cancel":
                await state.clear()
                await self._show(c, self._card(edit_id) if edit_id else self._menu(c.from_user.id))
            elif action == "cat":
                category = "" if callback_data.value == "any" else callback_data.value
                if edit_id and not category and not data.get("query"):
                    await c.answer("Без раздела нужна модель — сначала укажите её", show_alert=True)
                    return
                if edit_id:
                    await self._save_edit(chat, state, category=category)
                else:
                    await state.update_data(category=category)
                    await self._goto(chat, state, Wizard.query)
            elif action == "cities":
                await self._goto(chat, state, Wizard.city)
            elif action == "city":
                cities = list(data.get("cities", []))
                if callback_data.value in cities:
                    cities.remove(callback_data.value)
                else:
                    cities.append(callback_data.value)
                await state.update_data(cities=cities)
                await self._goto(chat, state, Wizard.city)
            elif action == "citydone":
                if edit_id:
                    await self._save_edit(chat, state, cities=data.get("cities", []))
                    await c.answer("Сохранено")
                    return
                await self._goto(chat, state, Wizard.confirm)
            elif action == "anymodel":
                if edit_id:
                    await self._save_edit(chat, state, query="")
                else:
                    await state.update_data(query="")
                    await self._goto(chat, state, Wizard.price)
            elif action == "price":
                if callback_data.value == "any":
                    await self._after_price(chat, state, None, None)
                else:
                    lo, hi = callback_data.value.split("-")
                    await self._after_price(chat, state, int(lo) or None, int(hi) or None)
            elif action == "minus":
                minus = list(data.get("minus", []))
                if callback_data.value == "*clear":
                    minus = [w for w in minus if w in ui.MINUS_LABELS]
                elif callback_data.value in minus:
                    minus.remove(callback_data.value)
                else:
                    minus.append(callback_data.value)
                await state.update_data(minus=minus)
                await self._goto(chat, state, Wizard.minus)
            elif action == "done":
                if edit_id:
                    await self._save_edit(chat, state, minus_words=data.get("minus", []))
                    await c.answer("Сохранено")
                    return
                await self._goto(chat, state, Wizard.confirm)
            elif action == "save":
                sub_id = self.db.add_subscription(data.get("query", ""), data.get("pmin"), data.get("pmax"),
                                                  data.get("minus", []), data.get("category", ""),
                                                  data.get("cities", []))
                await state.clear()
                await c.answer("Запрос сохранён")
                await self._show(c, self._card(sub_id))
                return
            elif action == "back":
                if edit_id:
                    await state.clear()
                    await self._show(c, self._card(edit_id))
                else:
                    prev = {
                        Wizard.query.state: Wizard.category,
                        Wizard.price.state: Wizard.query,
                        Wizard.minus.state: Wizard.price,
                        Wizard.confirm.state: Wizard.minus,
                        Wizard.city.state: Wizard.confirm,
                    }.get(current)
                    if prev:
                        await self._goto(chat, state, prev)
            await c.answer()

        # --- кнопки под объявлением ---

        @r.callback_query(ItemCb.filter())
        async def item_action(c: CallbackQuery, callback_data: ItemCb):
            if callback_data.action == "hide":
                await _delete(c.message)
                await c.answer()
                return
            s = self.db.subscription(callback_data.sub_id)
            if s:
                self.db.update_subscription(s.id, enabled=False)
                await c.answer(f"Запрос «{ui.sub_title(s.query, s.category)}» выключен. Включить снова можно в «Мои запросы».", show_alert=True)
            else:
                await c.answer("Запрос уже удалён")
            try:
                kb = InlineKeyboardMarkup(inline_keyboard=c.message.reply_markup.inline_keyboard[:1] + [
                    [b for b in c.message.reply_markup.inline_keyboard[1] if b.text != "Больше не искать"]
                ])
                await c.message.edit_reply_markup(reply_markup=kb)
            except Exception:
                pass

        # всё прочее — подсказка вместо тишины
        @r.message()
        async def fallback(m: Message):
            await self._show(m, self._menu(m.from_user.id))

        return r

    async def _after_price(self, chat_id: int, state: FSMContext, pmin: int | None, pmax: int | None) -> None:
        if (await state.get_data()).get("edit_id"):
            await self._save_edit(chat_id, state, min_price=pmin, max_price=pmax)
            return
        await state.update_data(pmin=pmin, pmax=pmax)
        await self._goto(chat_id, state, Wizard.minus)

    # ---------- мастер ----------

    async def _start_wizard(self, event: Message | CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        await state.set_state(Wizard.category)
        msg = await self._show(event, ui.wiz_category(False))
        await state.update_data(msg_id=msg.message_id, query="", category="", minus=[], cities=[])

    def _wizard_screen(self, step: State, data: dict) -> tuple[str, InlineKeyboardMarkup]:
        edit = bool(data.get("edit_id"))
        label = ui.sub_title(data.get("query", ""), data.get("category", ""))
        if step == Wizard.category:
            return ui.wiz_category(edit)
        if step == Wizard.query:
            return ui.wiz_query(data.get("category", ""), edit)
        if step == Wizard.price:
            return ui.wiz_price(label, edit)
        if step == Wizard.minus:
            return ui.wiz_minus(label, data.get("minus", []), edit)
        if step == Wizard.city:
            return ui.city_picker(data.get("cities", []), edit, self.monitor.budget())
        return ui.wiz_confirm(label, data.get("pmin"), data.get("pmax"), data.get("minus", []), data.get("cities", []))

    async def _goto(self, chat_id: int, state: FSMContext, step: State) -> None:
        await state.set_state(step)
        await self._show_wizard(chat_id, state, self._wizard_screen(step, await state.get_data()))

    async def _save_edit(self, chat_id: int, state: FSMContext, **fields) -> None:
        """Сохранить одно поле запроса и вернуться к его карточке."""
        edit_id = (await state.get_data())["edit_id"]
        self.db.update_subscription(edit_id, **fields)
        await self._show_wizard(chat_id, state, self._card(edit_id))
        await state.clear()

    # ---------- отправка ----------

    async def _send_test(self, chat_id: int) -> None:
        items = self.monitor.last_items
        if not items:
            await self.bot.send_message(chat_id, "Лента ещё не загружена. Попробуйте через несколько секунд.")
            return
        await self.send_item(items[0], [Subscription(0, "проверка уведомлений", None, None, [])], [chat_id])

    async def alert(self, text: str) -> None:
        log.info("Сообщение владельцу: %s", text)
        for chat_id in ([self.owner_id] if self.owner_id else []):
            try:
                await self.bot.send_message(chat_id, ui.alert_text(text), reply_markup=ui.alert_keyboard())
            except Exception as e:
                log.error("Не удалось отправить сообщение %s: %s", chat_id, e)

    async def send_item(self, item: Item, subs: list[Subscription], chat_ids: list[int] | None = None) -> None:
        for chat_id in (self.recipients() if chat_ids is None else chat_ids):
            await self.send_to_chat(item, subs, chat_id)

    async def send_to_chat(self, item: Item, subs: list[Subscription], chat_id: int) -> None:
        """Success means Telegram accepted the photo or text; errors reach the outbox worker."""
        caption = ui.item_caption(item, subs)
        kb = ui.item_keyboard(item, subs)
        photo = None
        try:
            photo = await self.fetch_image(item.image) if item.image and self.fetch_image else None
        except Exception as e:
            log.warning("Фото не скачалось: %s", e)
        if photo:
            try:
                await self.bot.send_photo(chat_id, BufferedInputFile(photo, f"{item.id}.jpg"),
                                          caption=caption, reply_markup=kb)
                return
            except (TelegramRetryAfter, TelegramForbiddenError):
                raise
            except Exception as exc:
                log.warning("Photo failed, trying text: %s", type(exc).__name__)
        await self.bot.send_message(chat_id, caption, reply_markup=kb,
                                    link_preview_options=LinkPreviewOptions(is_disabled=True))


async def _delete(message: Message | None) -> None:
    if message is None:
        return
    try:
        await message.delete()
    except Exception:
        pass
