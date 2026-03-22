from __future__ import annotations

from datetime import date
from typing import Any, Dict, List

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import BaseFilter, CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.utils.markdown import hbold

from ..config import ADMIN_IDS, BOT_USERNAME, CHANNEL_CHAT
from ..db import (
    get_community_settings,
    get_schedule_settings,
    set_schedule_mode,
    update_current_week,
    update_product,
    update_topic,
    update_tone,
)
from ..keyboards.admin import (
    admin_challenge_actions_kb,
    admin_challenge_edit_menu_kb,
    admin_gen_menu_kb,
    admin_main_kb,
    admin_mode_kb,
    admin_settings_kb,
)
from ..services.challenges import (
    delete_challenge,
    generate_range,
    get_analytics,
    get_challenge_by_id,
    list_challenges,
    mark_challenge_sent,
    regenerate_challenge,
    save_generated,
    update_challenge_date,
    update_challenge_text,
    update_challenge_week,
)

router = Router(name="admin")


class AdminFilter(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        return bool(message.from_user and message.from_user.id in ADMIN_IDS)


class AdminCbFilter(BaseFilter):
    async def __call__(self, callback: CallbackQuery) -> bool:
        return bool(callback.from_user and callback.from_user.id in ADMIN_IDS)


_edit_setting_state: Dict[int, str] = {}
_edit_challenge_state: Dict[int, Dict[str, Any]] = {}


def _render_admin_home_text(settings: Dict[str, Any]) -> str:
    return (
        f"{hbold('Challenge Bot Admin Panel')}\n\n"
        f"Community: {settings['community_name']}\n"
        f"Topic: {settings['topic']}\n"
        f"Product: {settings['product']}\n"
        f"Tone: {settings['tone']}\n"
        f"Current cycle week: {settings['current_week']}\n\n"
        "Choose an action:"
    )


def _render_challenge_card(ch: Dict[str, Any], title_prefix: str = "") -> str:
    prefix = f"{title_prefix}\n\n" if title_prefix else ""
    return (
        f"{prefix}"
        f"ID {ch['id']} · {ch['challenge_date'].isoformat()} · week {ch['week']}\n"
        f"Status: {ch.get('status', 'generated')}\n\n"
        f"{hbold(ch['title'])}\n\n"
        f"{ch['body']}"
    )


@router.message(CommandStart(), AdminFilter())
async def admin_start(message: Message) -> None:
    settings = await get_community_settings()
    await message.answer(_render_admin_home_text(settings), reply_markup=admin_main_kb())


@router.callback_query(AdminCbFilter(), F.data == "admin_main")
async def cb_admin_main(callback: CallbackQuery) -> None:
    settings = await get_community_settings()
    await callback.message.edit_text(_render_admin_home_text(settings), reply_markup=admin_main_kb())
    await callback.answer()


@router.callback_query(AdminCbFilter(), F.data == "admin_gen_menu")
async def cb_admin_gen_menu(callback: CallbackQuery) -> None:
    await callback.message.edit_text(
        "Choose how many days to generate challenges for (starting today):",
        reply_markup=admin_gen_menu_kb(),
    )
    await callback.answer()


async def _do_generate(callback: CallbackQuery, days: int) -> None:
    settings = await get_community_settings()
    start = date.today()
    week = settings["current_week"]

    await callback.message.edit_text("Generating challenges, please wait...")

    try:
        generated = await generate_range(
            start_date=start,
            days=days,
            week=week,
            topic=settings["topic"],
            product=settings["product"],
            tone=settings["tone"],
            community_name=settings["community_name"],
        )
        ids = await save_generated(generated, week=week)
    except Exception as exc:
        await callback.message.edit_text(f"Generation failed: {exc}", reply_markup=admin_main_kb())
        await callback.answer()
        return

    lines: List[str] = [f"Generated {len(ids)} challenge(s) starting from {start.isoformat()}:\n"]
    for ch, ch_id in zip(generated, ids):
        lines.append(f"ID {ch_id} · {ch['challenge_date'].isoformat()} · {ch['title']}")

    lines.append("\nOpen 'Challenge List' to review or publish them.")
    await callback.message.edit_text("\n".join(lines), reply_markup=admin_main_kb())
    await callback.answer("Done")


@router.callback_query(AdminCbFilter(), F.data == "admin_gen_1")
async def cb_admin_gen_1(callback: CallbackQuery) -> None:
    await _do_generate(callback, days=1)


@router.callback_query(AdminCbFilter(), F.data == "admin_gen_2")
async def cb_admin_gen_2(callback: CallbackQuery) -> None:
    await _do_generate(callback, days=2)


@router.callback_query(AdminCbFilter(), F.data == "admin_gen_3")
async def cb_admin_gen_3(callback: CallbackQuery) -> None:
    await _do_generate(callback, days=3)


@router.callback_query(AdminCbFilter(), F.data == "admin_gen_4")
async def cb_admin_gen_4(callback: CallbackQuery) -> None:
    await _do_generate(callback, days=4)


@router.callback_query(AdminCbFilter(), F.data == "admin_gen_5")
async def cb_admin_gen_5(callback: CallbackQuery) -> None:
    await _do_generate(callback, days=5)


@router.callback_query(AdminCbFilter(), F.data == "admin_gen_6")
async def cb_admin_gen_6(callback: CallbackQuery) -> None:
    await _do_generate(callback, days=6)


@router.callback_query(AdminCbFilter(), F.data == "admin_gen_7")
async def cb_admin_gen_7(callback: CallbackQuery) -> None:
    await _do_generate(callback, days=7)


@router.callback_query(AdminCbFilter(), F.data == "admin_list_challenges")
async def cb_admin_list_challenges(callback: CallbackQuery) -> None:
    rows = await list_challenges()
    rows = [r for r in rows if str(r.get("status")) != "sent"]

    if not rows:
        await callback.message.edit_text(
            "There are no unsent challenges right now.",
            reply_markup=admin_main_kb(),
        )
        await callback.answer()
        return

    lines: List[str] = ["Unsent challenges:\n"]
    for r in rows:
        lines.append(f"ID {r['id']} · {r['challenge_date'].isoformat()} · {r['title']}")
    lines.append("\nTap a challenge ID to open actions.")

    kb_rows = [[InlineKeyboardButton(text=f"ID {r['id']}", callback_data=f"admin_ch_{r['id']}")] for r in rows]
    kb_rows.append([InlineKeyboardButton(text="Back", callback_data="admin_main")])

    await callback.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows),
    )
    await callback.answer()


@router.callback_query(AdminCbFilter(), F.data.startswith("admin_ch_"))
async def cb_admin_open_challenge(callback: CallbackQuery) -> None:
    try:
        ch_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Invalid ID", show_alert=True)
        return

    ch = await get_challenge_by_id(ch_id)
    if not ch:
        await callback.answer("Challenge not found", show_alert=True)
        return

    await callback.message.edit_text(
        _render_challenge_card(ch),
        reply_markup=admin_challenge_actions_kb(ch_id),
    )
    await callback.answer()


@router.callback_query(AdminCbFilter(), F.data.startswith("admin_send_"))
async def cb_admin_send(callback: CallbackQuery) -> None:
    if CHANNEL_CHAT is None:
        await callback.answer("CHANNEL_ID or CHANNEL_USERNAME is not configured in .env", show_alert=True)
        return

    try:
        ch_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Invalid ID", show_alert=True)
        return

    ch = await get_challenge_by_id(ch_id)
    if not ch:
        await callback.answer("Challenge not found", show_alert=True)
        return

    if not BOT_USERNAME:
        await callback.answer("BOT_USERNAME is not configured in .env", show_alert=True)
        return

    text = (
        f"<b>{ch['title']}</b>\n\n"
        f"{ch['body']}\n\n"
        "Ready to join? Tap the button below."
    )

    ans_url = f"https://t.me/{BOT_USERNAME}?start=ans_{ch_id}"
    info_url = f"https://t.me/{BOT_USERNAME}?start=info_{ch_id}"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Submit Answer", url=ans_url)],
            [InlineKeyboardButton(text="Learn More", url=info_url)],
        ]
    )

    try:
        await callback.bot.send_message(CHANNEL_CHAT, text, reply_markup=kb)
        await mark_challenge_sent(ch_id)
    except Exception as exc:
        await callback.answer(f"Send failed: {exc}", show_alert=True)
        return

    await callback.answer("Sent to channel", show_alert=True)


@router.callback_query(AdminCbFilter(), F.data.startswith("admin_delete_"))
async def cb_admin_delete(callback: CallbackQuery) -> None:
    try:
        ch_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Invalid ID", show_alert=True)
        return

    await delete_challenge(ch_id)
    await callback.message.edit_text(f"Challenge ID {ch_id} was deleted.", reply_markup=admin_main_kb())
    await callback.answer("Deleted")


@router.callback_query(AdminCbFilter(), F.data.startswith("admin_regen_"))
async def cb_admin_regen(callback: CallbackQuery) -> None:
    try:
        ch_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Invalid ID", show_alert=True)
        return

    await callback.answer("Regenerating...")

    try:
        ch = await regenerate_challenge(ch_id)
    except Exception as exc:
        await callback.answer(f"Generation failed: {exc}", show_alert=True)
        return

    status = ch.get("status", "generated")
    payload = dict(ch)
    payload["status"] = status

    await callback.message.edit_text(
        _render_challenge_card(payload, title_prefix="Challenge updated"),
        reply_markup=admin_challenge_actions_kb(ch["id"]),
    )


@router.callback_query(AdminCbFilter(), F.data.regexp(r"^admin_edit_\d+$"))
async def cb_admin_edit(callback: CallbackQuery) -> None:
    """Open the field selection menu for challenge editing."""
    try:
        ch_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Invalid ID", show_alert=True)
        return

    ch = await get_challenge_by_id(ch_id)
    if not ch:
        await callback.answer("Challenge not found", show_alert=True)
        return

    _edit_challenge_state[callback.from_user.id] = {"id": ch_id, "field": None}

    text = _render_challenge_card(ch, title_prefix="Edit challenge")
    try:
        await callback.message.edit_text(text, reply_markup=admin_challenge_edit_menu_kb(ch_id))
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc):
            raise

    await callback.answer()


@router.callback_query(AdminCbFilter(), F.data.startswith("admin_edit_title_"))
async def cb_admin_edit_title(callback: CallbackQuery) -> None:
    ch_id = int(callback.data.split("_")[-1])
    _edit_challenge_state[callback.from_user.id] = {"id": ch_id, "field": "title"}
    await callback.message.edit_text(
        f"Enter a new title for challenge ID {ch_id}.\n\n"
        "Send one message. It will be used as the new title.",
        reply_markup=None,
    )
    await callback.answer("Waiting for title")


@router.callback_query(AdminCbFilter(), F.data.startswith("admin_edit_body_"))
async def cb_admin_edit_body(callback: CallbackQuery) -> None:
    ch_id = int(callback.data.split("_")[-1])
    _edit_challenge_state[callback.from_user.id] = {"id": ch_id, "field": "body"}
    await callback.message.edit_text(
        f"Enter new body text for challenge ID {ch_id}.\n\n"
        "Send one message. It will replace the challenge body.",
        reply_markup=None,
    )
    await callback.answer("Waiting for body text")


@router.callback_query(AdminCbFilter(), F.data.startswith("admin_edit_date_"))
async def cb_admin_edit_date(callback: CallbackQuery) -> None:
    ch_id = int(callback.data.split("_")[-1])
    _edit_challenge_state[callback.from_user.id] = {"id": ch_id, "field": "date"}
    await callback.message.edit_text(
        f"Enter a new date for challenge ID {ch_id}.\n\n"
        "Format: <code>YYYY-MM-DD</code>, for example <code>2026-04-15</code>.",
        reply_markup=None,
    )
    await callback.answer("Waiting for date")


@router.callback_query(AdminCbFilter(), F.data.startswith("admin_edit_week_"))
async def cb_admin_edit_week(callback: CallbackQuery) -> None:
    ch_id = int(callback.data.split("_")[-1])
    _edit_challenge_state[callback.from_user.id] = {"id": ch_id, "field": "week"}
    await callback.message.edit_text(
        f"Enter a new cycle week for challenge ID {ch_id}.\n\n"
        "Allowed values: 1, 2, 3, or 4.",
        reply_markup=None,
    )
    await callback.answer("Waiting for week number")


@router.callback_query(AdminCbFilter(), F.data == "admin_settings")
async def cb_admin_settings(callback: CallbackQuery) -> None:
    settings = await get_community_settings()
    schedule = await get_schedule_settings()
    mode = (schedule or {}).get("mode", "manual")
    send_time = (schedule or {}).get("send_time")
    mode_label = "Auto" if mode == "auto" else "Manual"
    time_label = send_time.strftime("%H:%M") if send_time else "not set"

    text = (
        f"{hbold('Community Settings')}\n\n"
        f"Topic: {settings['topic']}\n"
        f"Product: {settings['product']}\n"
        f"Tone: {settings['tone']}\n"
        f"Current cycle week: {settings['current_week']}\n"
        f"Posting mode: {mode_label}\n"
        f"Auto-post time: {time_label}\n\n"
        "Choose what to update:"
    )
    await callback.message.edit_text(text, reply_markup=admin_settings_kb())
    await callback.answer()


@router.callback_query(AdminCbFilter(), F.data == "admin_set_topic")
async def cb_admin_set_topic(callback: CallbackQuery) -> None:
    _edit_setting_state[callback.from_user.id] = "topic"
    await callback.message.edit_text("Enter a new community topic (for example: fitness, nutrition, productivity):")
    await callback.answer("Waiting for topic")


@router.callback_query(AdminCbFilter(), F.data == "admin_set_product")
async def cb_admin_set_product(callback: CallbackQuery) -> None:
    _edit_setting_state[callback.from_user.id] = "product"
    await callback.message.edit_text("Enter a new product/service name to reference in challenges:")
    await callback.answer("Waiting for product")


@router.callback_query(AdminCbFilter(), F.data == "admin_set_tone")
async def cb_admin_set_tone(callback: CallbackQuery) -> None:
    _edit_setting_state[callback.from_user.id] = "tone"
    await callback.message.edit_text("Enter a new communication tone (for example: friendly, supportive, concise):")
    await callback.answer("Waiting for tone")


@router.callback_query(AdminCbFilter(), F.data == "admin_set_week")
async def cb_admin_set_week(callback: CallbackQuery) -> None:
    _edit_setting_state[callback.from_user.id] = "week"
    await callback.message.edit_text("Enter the current cycle week (1-4):")
    await callback.answer("Waiting for week")


@router.message(AdminFilter())
async def admin_text_input(message: Message) -> None:
    user_id = message.from_user.id
    text = (message.text or "").strip()
    if not text:
        return

    if user_id in _edit_setting_state:
        field = _edit_setting_state.pop(user_id)

        if field == "topic":
            await update_topic(text)
            await message.answer(f"Topic updated: {text}", reply_markup=admin_main_kb())
            return

        if field == "product":
            await update_product(text)
            await message.answer(f"Product updated: {text}", reply_markup=admin_main_kb())
            return

        if field == "tone":
            await update_tone(text)
            await message.answer(f"Tone updated: {text}", reply_markup=admin_main_kb())
            return

        if field == "week":
            try:
                week = int(text)
                if week not in (1, 2, 3, 4):
                    raise ValueError
            except Exception:
                _edit_setting_state[user_id] = "week"
                await message.answer("Week must be an integer from 1 to 4. Please try again:")
                return

            await update_current_week(week)
            await message.answer(f"Current cycle week updated: {week}", reply_markup=admin_main_kb())
            return

    if user_id in _edit_challenge_state:
        state = _edit_challenge_state.get(user_id) or {}
        ch_id = state.get("id")
        field = state.get("field")

        if not ch_id or not field:
            _edit_challenge_state.pop(user_id, None)
            await message.answer(
                "Editing state is incomplete. Open the edit menu again.",
                reply_markup=admin_main_kb(),
            )
            return

        ch = await get_challenge_by_id(ch_id)
        if not ch:
            _edit_challenge_state.pop(user_id, None)
            await message.answer("Challenge not found.", reply_markup=admin_main_kb())
            return

        if field == "title":
            await update_challenge_text(ch_id, text, ch["body"])
            await message.answer("Title updated.", reply_markup=admin_challenge_actions_kb(ch_id))

        elif field == "body":
            await update_challenge_text(ch_id, ch["title"], text)
            await message.answer("Body updated.", reply_markup=admin_challenge_actions_kb(ch_id))

        elif field == "date":
            try:
                new_date = date.fromisoformat(text)
            except ValueError:
                await message.answer("Invalid date format. Use <code>YYYY-MM-DD</code>.")
                return

            await update_challenge_date(ch_id, new_date)
            await message.answer(
                f"Challenge date updated to {new_date.isoformat()}.",
                reply_markup=admin_challenge_actions_kb(ch_id),
            )

        elif field == "week":
            try:
                new_week = int(text)
                if new_week not in (1, 2, 3, 4):
                    raise ValueError
            except ValueError:
                await message.answer("Week must be an integer from 1 to 4.")
                return

            await update_challenge_week(ch_id, new_week)
            await message.answer(
                f"Challenge week updated to {new_week}.",
                reply_markup=admin_challenge_actions_kb(ch_id),
            )

        _edit_challenge_state.pop(user_id, None)
        return

    await message.answer(
        "You are in admin mode. Use /start to open the admin menu.",
        reply_markup=admin_main_kb(),
    )


@router.callback_query(AdminCbFilter(), F.data.startswith("admin_week_"))
async def cb_admin_week_choice(callback: CallbackQuery) -> None:
    try:
        week = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Invalid week value", show_alert=True)
        return

    if week not in (1, 2, 3, 4):
        await callback.answer("Week must be between 1 and 4", show_alert=True)
        return

    await update_current_week(week)
    await callback.message.edit_text(
        f"Current cycle week updated: {week}",
        reply_markup=admin_main_kb(),
    )
    await callback.answer("Updated")


@router.callback_query(AdminCbFilter(), F.data == "admin_set_mode")
async def cb_admin_set_mode(callback: CallbackQuery) -> None:
    schedule = await get_schedule_settings()
    mode = (schedule or {}).get("mode", "manual")
    await callback.message.edit_text(
        "Choose posting mode:",
        reply_markup=admin_mode_kb(mode),
    )
    await callback.answer()


@router.callback_query(AdminCbFilter(), F.data == "admin_mode_manual")
async def cb_admin_mode_manual(callback: CallbackQuery) -> None:
    await set_schedule_mode("manual")
    schedule = await get_schedule_settings()
    await callback.message.edit_text(
        "Posting mode updated.",
        reply_markup=admin_mode_kb(schedule.get("mode", "manual")),
    )
    await callback.answer("Mode: manual")


@router.callback_query(AdminCbFilter(), F.data == "admin_mode_auto")
async def cb_admin_mode_auto(callback: CallbackQuery) -> None:
    await set_schedule_mode("auto")
    schedule = await get_schedule_settings()
    await callback.message.edit_text(
        "Posting mode updated.",
        reply_markup=admin_mode_kb(schedule.get("mode", "auto")),
    )
    await callback.answer("Mode: auto")


@router.callback_query(AdminCbFilter(), F.data == "admin_analytics")
async def cb_admin_analytics(callback: CallbackQuery) -> None:
    rows = await get_analytics(limit=10)
    if not rows:
        await callback.message.edit_text(
            "No sent challenges are available for analytics yet.",
            reply_markup=admin_main_kb(),
        )
        await callback.answer()
        return

    lines: List[str] = ["Latest challenge analytics:\n"]
    for r in rows:
        sent_at = r["sent_at"]
        sent_str = sent_at.strftime("%d.%m %H:%M") if sent_at else "-"
        lines.append(
            f"• {r['challenge_date'].isoformat()} · week {r['week']}\n"
            f"  {r['title']}\n"
            f"  Answers: {r['answers_count']}, sent: {sent_str}"
        )

    await callback.message.edit_text("\n".join(lines), reply_markup=admin_main_kb())
    await callback.answer()
