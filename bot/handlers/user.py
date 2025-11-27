
from __future__ import annotations

from typing import Dict, Optional

from aiogram import Router, F
from aiogram.filters import CommandStart, Command, CommandObject
from aiogram.types import Message, CallbackQuery

from ..db import save_challenge_answer, get_user_answers_for_user
from ..services.challenges import get_challenge_by_id, generate_challenge_qa_answer
from ..keyboards.user import user_main_kb, answer_kb, qa_kb

router = Router(name="user")

# user_id -> challenge_id (режим ответа)
_answer_state: Dict[int, int] = {}
# user_id -> challenge_id (режим Q&A)
_qa_state: Dict[int, int] = {}


async def _show_user_home(target: Message | CallbackQuery) -> None:
    """
    Показ кабинета пользователя и сброс всех режимов.
    """
    if isinstance(target, CallbackQuery):
        user_id = target.from_user.id
    else:
        user_id = target.from_user.id

    _answer_state.pop(user_id, None)
    _qa_state.pop(user_id, None)

    text = (
        "👤 <b>Твой кабинет</b>\n\n"
        "Здесь можно:\n"
        "• посмотреть историю своих ответов на челленджи;\n"
        "• при необходимости обратиться в поддержку.\n\n"
        "Чтобы ответить на конкретный челлендж — нажми кнопку "
        "«Ответить» под постом в канале. Бот откроется сразу в режиме "
        "ответа именно на этот пост."
    )

    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, reply_markup=user_main_kb())
        await target.answer()
    else:
        await target.answer(text, reply_markup=user_main_kb())


@router.message(CommandStart())
async def user_start(message: Message, command: CommandObject) -> None:
    """
    /start
    /start ans_<id>
    /start info_<id>
    """
    user_id = message.from_user.id
    payload: Optional[str] = command.args

    # ----- /start ans_<id> -----
    if payload and payload.startswith("ans_"):
        try:
            ch_id = int(payload.split("_", maxsplit=1)[1])
        except Exception:
            await message.answer(
                "Не удалось определить челлендж для ответа.\n"
                "Попробуй ещё раз через кнопку «Ответить» под постом."
            )
            return

        ch = await get_challenge_by_id(ch_id)
        if not ch:
            await message.answer("Этот челлендж уже недоступен.")
            return

        _answer_state[user_id] = ch_id
        _qa_state.pop(user_id, None)

        await message.answer(
            f"✅ Ты перешёл(а) к ответу на челлендж:\n\n"
            f"📅 {ch['challenge_date'].isoformat()}\n"
            f"💪 {ch['title']}\n\n"
            f"{ch['body']}\n\n"
            "✍️ Напиши свой ответ ОДНИМ сообщением.\n"
            "Его увидят только админы сообщества.",
            reply_markup=answer_kb(),
        )
        return

    # ----- /start info_<id> -----
    if payload and payload.startswith("info_"):
        try:
            ch_id = int(payload.split("_", maxsplit=1)[1])
        except Exception:
            await message.answer(
                "Не удалось определить челлендж для режима «Узнать больше».\n"
                "Попробуй ещё раз через кнопку под постом."
            )
            return

        ch = await get_challenge_by_id(ch_id)
        if not ch:
            await message.answer("Этот челлендж уже недоступен.")
            return

        _qa_state[user_id] = ch_id
        _answer_state.pop(user_id, None)

        await message.answer(
            f"ℹ️ Подробности по челленджу «{ch['title']}»:\n\n"
            f"{ch['body']}\n\n"
            "Теперь ты можешь задать любой вопрос по этому челленджу — "
            "просто напиши его текстом, и модель ответит.",
            reply_markup=qa_kb(),
        )
        return

    # обычный /start
    await _show_user_home(message)


@router.message(Command("cabinet"))
async def cmd_cabinet(message: Message) -> None:
    await _show_user_home(message)


@router.callback_query(F.data == "user_home")
async def cb_user_home(callback: CallbackQuery) -> None:
    await _show_user_home(callback)


@router.callback_query(F.data == "user_support")
async def cb_user_support(callback: CallbackQuery) -> None:
    await callback.message.edit_text(
        "🆘 <b>Поддержка</b>\n\n"
        "Если у тебя есть вопрос или техническая проблема, "
        "напиши, пожалуйста, админу сообщества в личные сообщения.\n\n"
        "В следующих версиях здесь появится полноценный интерфейс поддержки.",
        reply_markup=user_main_kb(),
    )
    await callback.answer("Раздел поддержки")


@router.callback_query(F.data == "user_cancel")
async def cb_user_cancel(callback: CallbackQuery) -> None:
    user_id = callback.from_user.id
    _answer_state.pop(user_id, None)
    _qa_state.pop(user_id, None)

    await callback.message.edit_text(
        "Режим ответа/вопросов сброшен.\n\n"
        "Чтобы снова ответить на челлендж или задать вопрос по нему — "
        "перейди по кнопке под постом в канале или открой свой кабинет.",
        reply_markup=user_main_kb(),
    )
    await callback.answer("Режим сброшен")


@router.callback_query(F.data == "user_history")
async def cb_user_history(callback: CallbackQuery) -> None:
    user_id = callback.from_user.id
    rows = await get_user_answers_for_user(user_id)

    if not rows:
        await callback.message.edit_text(
            "У тебя пока нет сохранённых ответов на челленджи.",
            reply_markup=user_main_kb(),
        )
        await callback.answer()
        return

    lines = ["Твои последние ответы:\n"]
    for r in rows:
        dt = r["created_at"]
        ch_date = r["challenge_date"]
        title = r["title"]
        answer_text = r["answer_text"]
        lines.append(
            f"📅 {ch_date.isoformat()} · {dt.strftime('%Y-%m-%d %H:%M')}\n"
            f"💪 {title}\n"
            f"✍️ {answer_text[:200]}{'…' if len(answer_text) > 200 else ''}\n"
        )

    await callback.message.edit_text("\n".join(lines), reply_markup=user_main_kb())
    await callback.answer()


@router.message()
async def handle_user_message(message: Message) -> None:
    user_id = message.from_user.id
    text = (message.text or "").strip()
    if not text:
        return

    # --- режим ответа ---
    if user_id in _answer_state:
        ch_id = _answer_state.pop(user_id)

        await save_challenge_answer(
            challenge_id=ch_id,
            tg_user_id=user_id,
            username=message.from_user.username,
            full_name=" ".join(
                part
                for part in [
                    message.from_user.first_name,
                    message.from_user.last_name,
                ]
                if part
            ),
            answer_text=text,
        )

        await message.answer(
            "✅ Спасибо! Твой ответ сохранён.\n\n"
            "Админы смогут посмотреть его в аналитике.",
            reply_markup=user_main_kb(),
        )
        return

    # --- режим Q&A ---
    if user_id in _qa_state:
        ch_id = _qa_state[user_id]
        ch = await get_challenge_by_id(ch_id)
        if not ch:
            _qa_state.pop(user_id, None)
            await message.answer(
                "Этот челлендж уже недоступен.\n"
                "Дождись новых постов в канале и перейди по кнопке снова.",
                reply_markup=user_main_kb(),
            )
            return

        try:
            model_answer = await generate_challenge_qa_answer(ch, text)
        except Exception:
            await message.answer(
                "⚠️ Не получилось получить ответ модели.\n"
                "Твой вопрос сохранён и будет виден админам.",
                reply_markup=qa_kb(),
            )
            return

        await message.answer(
            f"❓ Твой вопрос по челленджу «{ch['title']}»:\n"
            f"«{text}»\n\n"
            f"🤖 Ответ модели:\n{model_answer}",
            reply_markup=qa_kb(),
        )
        return

    # --- нет активного режима ---
    await message.answer(
        "Это бот с ежедневными челленджами.\n\n"
        "Чтобы ответить на конкретный челлендж — нажми кнопку «Ответить» под постом в канале.\n"
        "Чтобы открыть свой кабинет и посмотреть историю ответов, отправь /cabinet.",
        reply_markup=user_main_kb(),
    )
