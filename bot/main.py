from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from datetime import datetime

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from .config import BOT_TOKEN, BOT_USERNAME, CHANNEL_CHAT
from .db import (
    close_db,
    get_community_settings,
    get_schedule_settings,
    init_db,
    set_schedule_last_auto_date,
)
from .handlers import admin as admin_handlers
from .handlers import user as user_handlers
from .services.challenges import (
    generate_range,
    get_challenge_for_date,
    mark_challenge_sent,
    save_generated,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


async def auto_poster_worker(bot: Bot) -> None:
    """Background worker that auto-posts daily challenges."""
    while True:
        try:
            settings = await get_schedule_settings()
            if not settings or settings.get("mode") != "auto":
                await asyncio.sleep(60)
                continue

            send_time = settings.get("send_time")
            if send_time is None:
                await asyncio.sleep(60)
                continue

            now = datetime.now()
            today = now.date()
            target_dt = datetime.combine(today, send_time)
            last_date = settings.get("last_auto_date")

            if last_date == today:
                await asyncio.sleep(60)
                continue

            if now < target_dt:
                await asyncio.sleep(30)
                continue

            challenge = await get_challenge_for_date(today)
            if challenge is None:
                community = await get_community_settings()
                generated = await generate_range(
                    start_date=today,
                    days=1,
                    week=community["current_week"],
                    topic=community["topic"],
                    product=community["product"],
                    tone=community["tone"],
                    community_name=community["community_name"],
                )
                await save_generated(generated, week=community["current_week"])
                challenge = await get_challenge_for_date(today)

            if challenge is None:
                await set_schedule_last_auto_date(today)
                await asyncio.sleep(60)
                continue

            challenge_id = int(challenge["id"])
            text = (
                f"<b>{challenge['title']}</b>\n\n"
                f"{challenge['body']}\n\n"
                "Ready to join? Tap the button below."
            )

            if not BOT_USERNAME or CHANNEL_CHAT is None:
                logging.warning("CHANNEL_CHAT or BOT_USERNAME is not configured. Skipping auto-post.")
                await set_schedule_last_auto_date(today)
                await asyncio.sleep(60)
                continue

            answer_url = f"https://t.me/{BOT_USERNAME}?start=ans_{challenge_id}"
            info_url = f"https://t.me/{BOT_USERNAME}?start=info_{challenge_id}"

            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Submit Answer", url=answer_url)],
                    [InlineKeyboardButton(text="Learn More", url=info_url)],
                ]
            )

            await bot.send_message(CHANNEL_CHAT, text, reply_markup=keyboard)
            await mark_challenge_sent(challenge_id)
            await set_schedule_last_auto_date(today)
        except Exception:
            logging.exception("Auto-post worker failed")
        finally:
            await asyncio.sleep(60)


async def main() -> None:
    await init_db()

    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    dp.include_router(admin_handlers.router)
    dp.include_router(user_handlers.router)

    await bot.delete_webhook(drop_pending_updates=True)

    auto_task = asyncio.create_task(auto_poster_worker(bot))
    try:
        await dp.start_polling(bot)
    finally:
        auto_task.cancel()
        with suppress(asyncio.CancelledError):
            await auto_task
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
