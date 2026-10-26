import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

from aiogram.methods import AnswerCallbackQuery, EditMessageText, SendMessage
from aiogram.types import CallbackQuery, Chat, Message, MessageEntity, Update, User

from avito_bot.bot import TelegramBot, Wizard
from avito_bot.db import DB
from avito_bot.ui import Nav, Wiz


def test_invited_member_can_use_commands_and_save_wizard_but_not_manage_access(tmp_path):
    async def scenario():
        db = DB(tmp_path / 'bot.sqlite3')
        tg = TelegramBot('123456:TEST-token', db, owner_id=1, city='kazan')
        tg.monitor = SimpleNamespace(status='работает', last_ok=None)
        number = 0

        def message(uid, text, *, sender=None):
            return Message(message_id=100, date=datetime.now(timezone.utc),
                           chat=Chat(id=uid, type='private'),
                           from_user=User(id=sender or uid, is_bot=False, first_name='Test'),
                           text=text, entities=[MessageEntity(type='bot_command', offset=0,
                                                             length=len(text.split()[0]))]
                           if text.startswith('/') else None)

        async def request(bot, method, **kwargs):
            if isinstance(method, (SendMessage, EditMessageText)):
                return message(method.chat_id, method.text, sender=123456)
            return True

        tg.bot.session.make_request = AsyncMock(side_effect=request)

        async def send(uid, text=None, callback=None):
            nonlocal number
            number += 1
            if callback:
                update = Update(update_id=number, callback_query=CallbackQuery(
                    id=str(number), from_user=User(id=uid, is_bot=False, first_name='Test'),
                    chat_instance='test', message=message(uid, 'screen', sender=123456), data=callback))
            else:
                update = Update(update_id=number, message=message(uid, text))
            await tg.dp.feed_update(tg.bot, update)

        try:
            db.create_invite('invite-code')
            await send(2, '/start inv_invite-code')
            assert tg.allowed(2)
            await send(2, '/menu')
            state = tg.dp.fsm.get_context(bot=tg.bot, chat_id=2, user_id=2)
            await send(2, callback=Nav(to='add').pack())
            assert await state.get_state() == Wizard.category.state
            await send(2, callback=Wiz(action='cat', value='gpu').pack())
            assert await state.get_state() == Wizard.query.state
            await send(2, 'rtx 3060')
            assert await state.get_state() == Wizard.price.state
            await send(2, '10000-20000')
            assert await state.get_state() == Wizard.minus.state
            await send(2, callback=Wiz(action='done').pack())
            assert await state.get_state() == Wizard.confirm.state
            await send(2, callback=Wiz(action='save').pack())
            assert await state.get_state() is None
            sub, = db.subscriptions()
            assert (sub.query, sub.min_price, sub.max_price) == ('rtx 3060', 10000, 20000)
            await send(2, '/add ryzen 5600 | 5000-9000')
            assert await state.get_state() == Wizard.confirm.state
            await send(2, callback=Wiz(action='save').pack())
            assert len(db.subscriptions()) == 2

            # A revoked or unknown user cannot invoke commands or saved wizard buttons.
            db.remove_member(2)
            for uid in (2, 3):
                before = tg.bot.session.make_request.await_count
                await send(uid, '/add unauthorized')
                await send(uid, callback=Wiz(action='save').pack())
                assert tg.bot.session.make_request.await_count == before
            assert len(db.subscriptions()) == 2

            db.add_member(2, 'Test')
            before = tg.bot.session.make_request.await_count
            await send(2, callback=Nav(to='invite').pack())
            calls = tg.bot.session.make_request.await_args_list[before:]
            assert not any('start=inv_' in str(call) for call in calls)
            assert any(isinstance(call.args[1], AnswerCallbackQuery) for call in calls)
            # Owner commands continue working through the same access filter.
            await send(1, '/add owner query')
            owner_state = tg.dp.fsm.get_context(bot=tg.bot, chat_id=1, user_id=1)
            assert await owner_state.get_state() == Wizard.confirm.state
        finally:
            await tg.bot.session.close()
            await tg.dp.storage.close()
            db.close()

    asyncio.run(scenario())
