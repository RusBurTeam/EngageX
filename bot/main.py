from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from datetime import datetime

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties

from .config import BOT_TOKEN, CHANNEL_CHAT, BOT_USERNAME
from .db import (
    init_db,
    close_db,
    get_schedule_settings,
    set_schedule_last_auto_date,
    get_community_settings,
)
from .handlers import admin as admin_handlers
from .handlers import user as user_handlers
from .services.challenges import (
    get_challenge_for_date,
    generate_range,
    save_generated,
    mark_challenge_sent,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


async def auto_poster_worker(bot: Bot) -> None:
    """Фоновая задача для автоматической публикации челленджей."""
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

            # Уже отправляли сегодня — ждём завтрашнего дня
            if last_date == today:
                await asyncio.sleep(60)
                continue

            # Время ещё не наступило — проверяем позже
            if now < target_dt:
                await asyncio.sleep(30)
                continue

            # Время отправки наступило
            ch = await get_challenge_for_date(today)

            # Если челленджа на сегодня нет — сгенерируем один
            if ch is None:
                community = await get_community_settings()
                week = community["current_week"]
                generated = await generate_range(
                    start_date=today,
                    days=1,
                    week=week,
                    topic=community["topic"],
                    product=community["product"],
                    tone=community["tone"],
                    community_name=community["community_name"],
                )
                await save_generated(generated, week=week)
                ch = await get_challenge_for_date(today)

            if ch is None:
                # Такого быть не должно, но на всякий случай отметим дату
                await set_schedule_last_auto_date(today)
                await asyncio.sleep(60)
                continue

            ch_id = int(ch["id"])
            text = (
                f"💪 <b>{ch['title']}</b>\n\n"
                f"{ch['body']}\n\n"
                "Готов(а) включиться? Жмём кнопку 👇"
            )

            if not BOT_USERNAME or CHANNEL_CHAT is None:
                logging.warning(
                    "CHANNEL_CHAT или BOT_USERNAME не настроены, "
                    "пропускаю авто-постинг"
                )
                await set_schedule_last_auto_date(today)
                await asyncio.sleep(60)
                continue

            ans_url = f"https://t.me/{BOT_USERNAME}?start=ans_{ch_id}"
            info_url = f"https://t.me/{BOT_USERNAME}?start=info_{ch_id}"

            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="✅ Ответить",
                            url=ans_url,
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            text="ℹ️ Подробнее о задании",
                            url=info_url,
                        )
                    ],
                ]
            )

            await bot.send_message(CHANNEL_CHAT, text, reply_markup=kb)
            await mark_challenge_sent(ch_id)
            await set_schedule_last_auto_date(today)
        except Exception:
            logging.exception("Ошибка в авто-постинге челленджей")
        finally:
            await asyncio.sleep(60)


async def main() -> None:
    await init_db()

    bot = Bot(
        BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()

    # Порядок важен: сначала админ, затем пользовательский роутер
    dp.include_router(admin_handlers.router)
    dp.include_router(user_handlers.router)

    # Сбрасываем апдэйты
    await bot.delete_webhook(drop_pending_updates=True)

    # Запускаем фоновую задачу авто-постинга
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
