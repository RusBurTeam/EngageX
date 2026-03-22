from __future__ import annotations

from typing import Dict, Optional

from aiogram import F, Router
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import CallbackQuery, Message

from ..db import get_user_answers_for_user, save_challenge_answer
from ..keyboards.user import answer_kb, qa_kb, user_main_kb
from ..services.challenges import generate_challenge_qa_answer, get_challenge_by_id

router = Router(name="user")

_answer_state: Dict[int, int] = {}
_qa_state: Dict[int, int] = {}


async def _show_user_home(target: Message | CallbackQuery) -> None:
    """Show user dashboard and reset active modes."""
    user_id = target.from_user.id
    _answer_state.pop(user_id, None)
    _qa_state.pop(user_id, None)

    text = (
        "<b>Your Dashboard</b>\n\n"
        "Here you can:\n"
        "• review your recent challenge answers;\n"
        "• open support information.\n\n"
        "To answer a specific challenge, tap the button under the channel post. "
        "The bot will open directly in answer mode for that challenge."
    )

    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, reply_markup=user_main_kb())
        await target.answer()
    else:
        await target.answer(text, reply_markup=user_main_kb())


@router.message(CommandStart())
async def user_start(message: Message, command: CommandObject) -> None:
    """Handle /start, /start ans_<id>, and /start info_<id>."""
    user_id = message.from_user.id
    payload: Optional[str] = command.args

    if payload and payload.startswith("ans_"):
        try:
            ch_id = int(payload.split("_", maxsplit=1)[1])
        except Exception:
            await message.answer(
                "Could not resolve the challenge for answer mode. "
                "Please tap 'Submit Answer' under the post again."
            )
            return

        ch = await get_challenge_by_id(ch_id)
        if not ch:
            await message.answer("This challenge is no longer available.")
            return

        _answer_state[user_id] = ch_id
        _qa_state.pop(user_id, None)

        await message.answer(
            f"You are now answering challenge:\n\n"
            f"{ch['challenge_date'].isoformat()}\n"
            f"{ch['title']}\n\n"
            f"{ch['body']}\n\n"
            "Send your answer as a single message. "
            "Only community admins can view it.",
            reply_markup=answer_kb(),
        )
        return

    if payload and payload.startswith("info_"):
        try:
            ch_id = int(payload.split("_", maxsplit=1)[1])
        except Exception:
            await message.answer(
                "Could not resolve the challenge for info mode. "
                "Please tap 'Learn More' under the post again."
            )
            return

        ch = await get_challenge_by_id(ch_id)
        if not ch:
            await message.answer("This challenge is no longer available.")
            return

        _qa_state[user_id] = ch_id
        _answer_state.pop(user_id, None)

        await message.answer(
            f"Challenge details: {ch['title']}\n\n"
            f"{ch['body']}\n\n"
            "You can now ask any question about this challenge. "
            "Send your question as a normal message.",
            reply_markup=qa_kb(),
        )
        return

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
        "<b>Support</b>\n\n"
        "If you have a question or a technical issue, "
        "please contact your community admin directly.",
        reply_markup=user_main_kb(),
    )
    await callback.answer("Support")


@router.callback_query(F.data == "user_cancel")
async def cb_user_cancel(callback: CallbackQuery) -> None:
    user_id = callback.from_user.id
    _answer_state.pop(user_id, None)
    _qa_state.pop(user_id, None)

    await callback.message.edit_text(
        "Answer/Q&A mode has been reset.\n\n"
        "To continue with a specific challenge, use the button under that channel post.",
        reply_markup=user_main_kb(),
    )
    await callback.answer("Reset")


@router.callback_query(F.data == "user_history")
async def cb_user_history(callback: CallbackQuery) -> None:
    user_id = callback.from_user.id
    rows = await get_user_answers_for_user(user_id)

    if not rows:
        await callback.message.edit_text(
            "You do not have saved challenge answers yet.",
            reply_markup=user_main_kb(),
        )
        await callback.answer()
        return

    lines = ["Your latest answers:\n"]
    for r in rows:
        dt = r["created_at"]
        ch_date = r["challenge_date"]
        title = r["title"]
        answer_text = r["answer_text"]
        preview = answer_text[:200] + ("..." if len(answer_text) > 200 else "")
        lines.append(
            f"{ch_date.isoformat()} · {dt.strftime('%Y-%m-%d %H:%M')}\n"
            f"{title}\n"
            f"{preview}\n"
        )

    await callback.message.edit_text("\n".join(lines), reply_markup=user_main_kb())
    await callback.answer()


@router.message()
async def handle_user_message(message: Message) -> None:
    user_id = message.from_user.id
    text = (message.text or "").strip()
    if not text:
        return

    if user_id in _answer_state:
        ch_id = _answer_state.pop(user_id)
        await save_challenge_answer(
            challenge_id=ch_id,
            tg_user_id=user_id,
            username=message.from_user.username,
            full_name=" ".join(
                part for part in [message.from_user.first_name, message.from_user.last_name] if part
            ),
            answer_text=text,
        )

        await message.answer(
            "Thank you. Your answer has been saved and is now available to admins.",
            reply_markup=user_main_kb(),
        )
        return

    if user_id in _qa_state:
        ch_id = _qa_state[user_id]
        ch = await get_challenge_by_id(ch_id)
        if not ch:
            _qa_state.pop(user_id, None)
            await message.answer(
                "This challenge is no longer available. Please use a new challenge link.",
                reply_markup=user_main_kb(),
            )
            return

        try:
            model_answer = await generate_challenge_qa_answer(ch, text)
        except Exception:
            await message.answer(
                "Could not get a model response right now. Please try again shortly.",
                reply_markup=qa_kb(),
            )
            return

        await message.answer(
            f"Your question about '{ch['title']}':\n"
            f"{text}\n\n"
            f"Model answer:\n{model_answer}",
            reply_markup=qa_kb(),
        )
        return

    await message.answer(
        "This bot is used for daily challenges.\n\n"
        "To answer a specific challenge, tap the button under the channel post.\n"
        "To open your dashboard and answer history, send /cabinet.",
        reply_markup=user_main_kb(),
    )
