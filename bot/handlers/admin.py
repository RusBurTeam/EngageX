from __future__ import annotations

from datetime import date
from typing import List, Dict, Any

from aiogram import Router, F
from aiogram.filters import CommandStart, BaseFilter
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.utils.markdown import hbold
from aiogram.exceptions import TelegramBadRequest  # <-- добавили

from ..config import ADMIN_IDS, CHANNEL_CHAT, BOT_USERNAME
from ..db import (
    get_community_settings,
    update_topic,
    update_product,
    update_tone,
    update_current_week,
    get_schedule_settings,
    set_schedule_mode,
)
from ..services.challenges import (
    generate_range,
    save_generated,
    list_challenges,
    get_challenge_by_id,
    mark_challenge_sent,
    delete_challenge,
    regenerate_challenge,
    update_challenge_text,
    update_challenge_date,
    update_challenge_week,
    get_analytics,
)
from ..keyboards.admin import (
    admin_main_kb,
    admin_gen_menu_kb,
    admin_challenge_actions_kb,
    admin_challenge_edit_menu_kb,
    admin_settings_kb,
    admin_mode_kb,
    admin_week_kb,
)

router = Router(name="admin")


class AdminFilter(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        return bool(message.from_user and message.from_user.id in ADMIN_IDS)


class AdminCbFilter(BaseFilter):
    async def __call__(self, callback: CallbackQuery) -> bool:
        return bool(callback.from_user and callback.from_user.id in ADMIN_IDS)


# ====== простое состояние для изменения настроек и редактирования челленджей ======

_edit_setting_state: Dict[int, str] = {}          # user_id -> field ("topic", "product", "tone", "week")
_edit_challenge_state: Dict[int, Dict[str, Any]] = {}  # user_id -> {"id": int, "field": str | None}


# ===================== /start для админа =====================

@router.message(CommandStart(), AdminFilter())
async def admin_start(message: Message) -> None:
    settings = await get_community_settings()
    text = (
        f"👋 {hbold('Админ-панель челлендж-бота')}\n\n"
        f"Сообщество: {settings['community_name']}\n"
        f"Тематика: {settings['topic']}\n"
        f"Продукт: {settings['product']}\n"
        f"Тон: {settings['tone']}\n"
        f"Текущая неделя цикла: {settings['current_week']}\n\n"
        "Выбери действие:"
    )
    await message.answer(text, reply_markup=admin_main_kb())


@router.callback_query(AdminCbFilter(), F.data == "admin_main")
async def cb_admin_main(callback: CallbackQuery) -> None:
    settings = await get_community_settings()
    text = (
        f"👋 {hbold('Админ-панель челлендж-бота')}\n\n"
        f"Сообщество: {settings['community_name']}\n"
        f"Тематика: {settings['topic']}\n"
        f"Продукт: {settings['product']}\n"
        f"Тон: {settings['tone']}\n"
        f"Текущая неделя цикла: {settings['current_week']}\n\n"
        "Выбери действие:"
    )
    await callback.message.edit_text(text, reply_markup=admin_main_kb())
    await callback.answer()


# ===================== генерация =====================

@router.callback_query(AdminCbFilter(), F.data == "admin_gen_menu")
async def cb_admin_gen_menu(callback: CallbackQuery) -> None:
    await callback.message.edit_text(
        "Выбери, на сколько дней сгенерировать челленджи "
        "от сегодняшнего дня включительно:",
        reply_markup=admin_gen_menu_kb(),
    )
    await callback.answer()


async def _do_generate(callback: CallbackQuery, days: int) -> None:
    settings = await get_community_settings()
    start = date.today()
    week = settings["current_week"]

    await callback.message.edit_text("Генерирую челленджи, подожди…")

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
    except Exception as e:
        await callback.message.edit_text(
            f"⚠️ Ошибка генерации: {e}",
            reply_markup=admin_main_kb(),
        )
        await callback.answer()
        return

    lines: List[str] = [
        f"✅ Сгенерировано {len(ids)} челлендж(ей) начиная с {start.isoformat()}:\n"
    ]
    for ch, ch_id in zip(generated, ids):
        lines.append(
            f"ID {ch_id} · {ch['challenge_date'].isoformat()} · {ch['title']}"
        )

    lines.append("\nОткрой «📋 Список челленджей», чтобы отправить их в канал.")
    await callback.message.edit_text("\n".join(lines), reply_markup=admin_main_kb())
    await callback.answer("Готово")


@router.callback_query(AdminCbFilter(), F.data == "admin_gen_1")
async def cb_admin_gen_1(callback: CallbackQuery) -> None:
    await _do_generate(callback, days=1)


@router.callback_query(AdminCbFilter(), F.data == "admin_gen_3")
async def cb_admin_gen_3(callback: CallbackQuery) -> None:
    await _do_generate(callback, days=3)


@router.callback_query(AdminCbFilter(), F.data == "admin_gen_7")
async def cb_admin_gen_7(callback: CallbackQuery) -> None:
    await _do_generate(callback, days=7)


# ===================== список челленджей (НЕ показываем отправленные) =====================

@router.callback_query(AdminCbFilter(), F.data == "admin_list_challenges")
async def cb_admin_list_challenges(callback: CallbackQuery) -> None:
    rows = await list_challenges()

    # убираем уже отправленные
    rows = [r for r in rows if str(r.get("status")) != "sent"]

    if not rows:
        await callback.message.edit_text(
            "Пока нет челленджей, которые ещё не отправлены в канал.",
            reply_markup=admin_main_kb(),
        )
        await callback.answer()
        return

    lines: List[str] = ["📋 Челленджи (ещё не были отправлены):\n"]
    for r in rows:
        lines.append(
            f"🕒 ID {r['id']} · {r['challenge_date'].isoformat()} · {r['title']}"
        )

    lines.append("\nНажми на ID челленджа ниже, чтобы открыть действия.")

    kb_rows = []
    for r in rows:
        kb_rows.append(
            [
                InlineKeyboardButton(
                    text=f"ID {r['id']}",
                    callback_data=f"admin_ch_{r['id']}",
                )
            ]
        )
    kb_rows.append(
        [
            InlineKeyboardButton(
                text="⬅️ Назад", callback_data="admin_main"
            )
        ]
    )
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    await callback.message.edit_text("\n".join(lines), reply_markup=kb)
    await callback.answer()


@router.callback_query(AdminCbFilter(), F.data.startswith("admin_ch_"))
async def cb_admin_open_challenge(callback: CallbackQuery) -> None:
    try:
        ch_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Некорректный ID", show_alert=True)
        return

    ch = await get_challenge_by_id(ch_id)
    if not ch:
        await callback.answer("Челлендж не найден", show_alert=True)
        return

    text = (
        f"ID {ch['id']} · {ch['challenge_date'].isoformat()} · неделя {ch['week']}\n"
        f"Статус: {ch['status']}\n\n"
        f"{hbold(ch['title'])}\n\n"
        f"{ch['body']}"
    )
    await callback.message.edit_text(
        text, reply_markup=admin_challenge_actions_kb(ch_id)
    )
    await callback.answer()


# ===================== отправка в канал =====================

@router.callback_query(AdminCbFilter(), F.data.startswith("admin_send_"))
async def cb_admin_send(callback: CallbackQuery) -> None:
    if CHANNEL_CHAT is None:
        await callback.answer(
            "CHANNEL_ID или CHANNEL_USERNAME не настроены в .env",
            show_alert=True,
        )
        return

    try:
        ch_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Некорректный ID", show_alert=True)
        return

    ch = await get_challenge_by_id(ch_id)
    if not ch:
        await callback.answer("Челлендж не найден", show_alert=True)
        return

    # Текст поста
    text = (
        f"💪 <b>{ch['title']}</b>\n\n"
        f"{ch['body']}\n\n"
        "Готов(а) включиться? Жмём кнопку 👇"
    )

    # Кнопки с deep-link на бота
    if not BOT_USERNAME:
        await callback.answer("BOT_USERNAME не настроен в .env", show_alert=True)
        return

    ans_url = f"https://t.me/{BOT_USERNAME}?start=ans_{ch_id}"
    info_url = f"https://t.me/{BOT_USERNAME}?start=info_{ch_id}"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Ответить", url=ans_url
                )
            ],
            [
                InlineKeyboardButton(
                    text="ℹ️ Узнать больше", url=info_url
                )
            ],
        ]
    )

    try:
        await callback.bot.send_message(CHANNEL_CHAT, text, reply_markup=kb)
        await mark_challenge_sent(ch_id)
    except Exception as e:
        await callback.answer(f"Ошибка отправки: {e}", show_alert=True)
        return

    await callback.answer("Отправлено в канал", show_alert=True)


# ===================== удаление =====================

@router.callback_query(AdminCbFilter(), F.data.startswith("admin_delete_"))
async def cb_admin_delete(callback: CallbackQuery) -> None:
    try:
        ch_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Некорректный ID", show_alert=True)
        return

    await delete_challenge(ch_id)
    await callback.message.edit_text(
        f"Челлендж ID {ch_id} удалён.", reply_markup=admin_main_kb()
    )
    await callback.answer("Удалено")


# ===================== перегенерация =====================

@router.callback_query(AdminCbFilter(), F.data.startswith("admin_regen_"))
async def cb_admin_regen(callback: CallbackQuery) -> None:
    try:
        ch_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Некорректный ID", show_alert=True)
        return

    await callback.answer("Перегенерирую…")

    try:
        # regenerate_challenge возвращает обновлённый челлендж
        ch = await regenerate_challenge(ch_id)
    except Exception as e:
        await callback.answer(f"Ошибка генерации: {e}", show_alert=True)
        return

    # на случай, если статус не приходит из БД – подставляем generated
    status = ch.get("status", "generated")

    text = (
        f"✏️ Редактирование челленджа ID {ch['id']}\n\n"
        f"📅 Дата: {ch['challenge_date'].isoformat()}\n"
        f"📆 Неделя: {ch['week']}\n"
        f"Статус: {status}\n\n"
        f"{hbold(ch['title'])}\n\n"
        f"{ch['body']}"
    )

    await callback.message.edit_text(
        text,
        reply_markup=admin_challenge_actions_kb(ch["id"]),
    )



# ===================== редактирование челленджа (меню «что менять») =====================

@router.callback_query(AdminCbFilter(), F.data.regexp(r"^admin_edit_\d+$"))
async def cb_admin_edit(callback: CallbackQuery) -> None:
    """
    Нажали кнопку ✏️ Редактировать у конкретного челленджа.
    Показываем пост + меню, что менять.
    """
    try:
        ch_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Некорректный ID", show_alert=True)
        return




    ch = await get_challenge_by_id(ch_id)
    if not ch:
        await callback.answer("Челлендж не найден", show_alert=True)
        return

    user_id = callback.from_user.id
    _edit_challenge_state[user_id] = {
        "id": ch_id,
        "field": None,
    }

    text = (
        f"✏️ Редактирование челленджа ID {ch_id}\n\n"
        f"📅 Дата: {ch['challenge_date']}\n"
        f"📆 Неделя: {ch['week']}\n"
        f"Статус: {ch['status']}\n\n"
        f"<b>{ch['title']}</b>\n\n"
        f"{ch['body']}"
    )

    try:
        await callback.message.edit_text(
            text,
            reply_markup=admin_challenge_edit_menu_kb(ch_id),
        )
    except TelegramBadRequest as e:
        # если пользователь просто повторно нажал ту же кнопку —
        # игнорируем "message is not modified"
        if "message is not modified" not in str(e):
            raise

    await callback.answer()


@router.callback_query(AdminCbFilter(), F.data.startswith("admin_edit_title_"))
async def cb_admin_edit_title(callback: CallbackQuery) -> None:
    try:
        ch_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Некорректный ID", show_alert=True)
        return

    user_id = callback.from_user.id
    _edit_challenge_state[user_id] = {
        "id": ch_id,
        "field": "title",
    }

    await callback.message.edit_text(
        f"📝 Введи новый заголовок для челленджа ID {ch_id}.\n\n"
        "Отправь одно сообщение — это будет новый заголовок.",
        reply_markup=None,
    )
    await callback.answer("Жду заголовок")


@router.callback_query(AdminCbFilter(), F.data.startswith("admin_edit_body_"))
async def cb_admin_edit_body(callback: CallbackQuery) -> None:
    try:
        ch_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Некорректный ID", show_alert=True)
        return

    user_id = callback.from_user.id
    _edit_challenge_state[user_id] = {
        "id": ch_id,
        "field": "body",
    }

    await callback.message.edit_text(
        f"🧾 Введи новый текст для челленджа ID {ch_id}.\n\n"
        "Отправь одно сообщение — это будет новый текст поста.",
        reply_markup=None,
    )
    await callback.answer("Жду текст")


@router.callback_query(AdminCbFilter(), F.data.startswith("admin_edit_date_"))
async def cb_admin_edit_date(callback: CallbackQuery) -> None:
    try:
        ch_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Некорректный ID", show_alert=True)
        return

    user_id = callback.from_user.id
    _edit_challenge_state[user_id] = {
        "id": ch_id,
        "field": "date",
    }

    await callback.message.edit_text(
        f"📅 Введи новую дату для челленджа ID {ch_id}.\n\n"
        "Формат: <code>ГГГГ-ММ-ДД</code>, например: <code>2025-11-30</code>.",
        reply_markup=None,
    )
    await callback.answer("Жду дату")


@router.callback_query(AdminCbFilter(), F.data.startswith("admin_edit_week_"))
async def cb_admin_edit_week(callback: CallbackQuery) -> None:
    try:
        ch_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Некорректный ID", show_alert=True)
        return

    user_id = callback.from_user.id
    _edit_challenge_state[user_id] = {
        "id": ch_id,
        "field": "week",
    }

    await callback.message.edit_text(
        f"📆 Введи новую неделю цикла для челленджа ID {ch_id}.\n\n"
        "Число от 1 до 4.",
        reply_markup=None,
    )
    await callback.answer("Жду номер недели")


# ===================== настройки сообщества =====================


@router.callback_query(AdminCbFilter(), F.data == "admin_settings")
async def cb_admin_settings(callback: CallbackQuery) -> None:
    settings = await get_community_settings()
    schedule = await get_schedule_settings()
    mode = (schedule or {}).get("mode", "manual")
    send_time = (schedule or {}).get("send_time")
    mode_label = "🤖 Авто" if mode == "auto" else "📤 Ручной"
    time_label = send_time.strftime("%H:%M") if send_time else "не задано"

    text = (
        f"⚙️ {hbold('Настройки сообщества')}\n\n"
        f"Тематика: {settings['topic']}\n"
        f"Продукт: {settings['product']}\n"
        f"Тон: {settings['tone']}\n"
        f"Текущая неделя цикла: {settings['current_week']}\n"
        f"Режим публикации: {mode_label}\n"
        f"Время автоотправки: {time_label}\n\n"
        "Выбери, что хочешь изменить:"
    )
    await callback.message.edit_text(text, reply_markup=admin_settings_kb())
    await callback.answer()


@router.callback_query(AdminCbFilter(), F.data == "admin_set_topic")
async def cb_admin_set_topic(callback: CallbackQuery) -> None:
    user_id = callback.from_user.id
    _edit_setting_state[user_id] = "topic"
    await callback.message.edit_text(
        "🧩 Введи новую тематику сообщества (например: фитнес, питание, SaaS...):"
    )
    await callback.answer("Жду текст")


@router.callback_query(AdminCbFilter(), F.data == "admin_set_product")
async def cb_admin_set_product(callback: CallbackQuery) -> None:
    user_id = callback.from_user.id
    _edit_setting_state[user_id] = "product"
    await callback.message.edit_text(
        "📦 Введи название продукта/сервиса (что мы продвигаем через челленджи):"
    )
    await callback.answer("Жду текст")


@router.callback_query(AdminCbFilter(), F.data == "admin_set_tone")
async def cb_admin_set_tone(callback: CallbackQuery) -> None:
    user_id = callback.from_user.id
    _edit_setting_state[user_id] = "tone"
    await callback.message.edit_text(
        "🎙 Опиши тональность сообщений (например: дружелюбный и поддерживающий, без токсичности):"
    )
    await callback.answer("Жду текст")


@router.callback_query(AdminCbFilter(), F.data == "admin_set_week")
async def cb_admin_set_week(callback: CallbackQuery) -> None:
    user_id = callback.from_user.id
    _edit_setting_state[user_id] = "week"
    await callback.message.edit_text(
        "📅 Введи номер недели цикла (1–4):"
    )
    await callback.answer("Жду число")


# ===================== обработка текстов от админа (настройки / редактирование) =====================

@router.message(AdminFilter())
async def admin_text_input(message: Message) -> None:
    user_id = message.from_user.id
    text = (message.text or "").strip()
    if not text:
        return

    # --- изменение настроек сообщества ---
    if user_id in _edit_setting_state:
        field = _edit_setting_state.pop(user_id)

        if field == "topic":
            await update_topic(text)
            await message.answer(
                f"Тематика обновлена на: {text}", reply_markup=admin_main_kb()
            )
            return

        if field == "product":
            await update_product(text)
            await message.answer(
                f"Продукт обновлён на: {text}", reply_markup=admin_main_kb()
            )
            return

        if field == "tone":
            await update_tone(text)
            await message.answer(
                f"Тональность обновлена на: {text}", reply_markup=admin_main_kb()
            )
            return

        if field == "week":
            try:
                week = int(text)
                if week < 1 or week > 4:
                    raise ValueError
            except Exception:
                _edit_setting_state[user_id] = "week"
                await message.answer(
                    "Нужно ввести целое число от 1 до 4. Попробуй ещё раз:"
                )
                return

            await update_current_week(week)
            await message.answer(
                f"Номер недели цикла обновлён на: {week}",
                reply_markup=admin_main_kb(),
            )
            return

    # --- редактирование челленджа ---
    if user_id in _edit_challenge_state:
        state = _edit_challenge_state.get(user_id) or {}
        ch_id = state.get("id")
        field = state.get("field")

        if not ch_id or not field:
            # Неполное состояние — на всякий случай сбросим
            _edit_challenge_state.pop(user_id, None)
            await message.answer(
                "Не понимаю, что редактируем. Попробуй ещё раз через меню редактирования.",
                reply_markup=admin_main_kb(),
            )
            return

        ch = await get_challenge_by_id(ch_id)
        if not ch:
            _edit_challenge_state.pop(user_id, None)
            await message.answer("Челлендж не найден.", reply_markup=admin_main_kb())
            return

        # -------- заголовок --------
        if field == "title":
            new_title = text
            await update_challenge_text(ch_id, new_title, ch["body"])
            await message.answer(
                "✅ Заголовок обновлён.",
                reply_markup=admin_challenge_actions_kb(ch_id),
            )

        # -------- текст --------
        elif field == "body":
            new_body = text
            await update_challenge_text(ch_id, ch["title"], new_body)
            await message.answer(
                "✅ Текст поста обновлён.",
                reply_markup=admin_challenge_actions_kb(ch_id),
            )

        # -------- дата --------
        elif field == "date":
            try:
                new_date = date.fromisoformat(text)
            except ValueError:
                await message.answer(
                    "Некорректная дата. Формат: <code>ГГГГ-ММ-ДД</code>. Попробуй ещё раз."
                )
                return

            await update_challenge_date(ch_id, new_date)
            await message.answer(
                f"✅ Дата челленджа обновлена на {new_date.isoformat()}.",
                reply_markup=admin_challenge_actions_kb(ch_id),
            )

        # -------- неделя --------
        elif field == "week":
            try:
                new_week = int(text)
            except ValueError:
                await message.answer("Неделя должна быть числом от 1 до 4. Попробуй ещё раз.")
                return

            if new_week not in (1, 2, 3, 4):
                await message.answer("Неделя должна быть числом от 1 до 4. Попробуй ещё раз.")
                return

            await update_challenge_week(ch_id, new_week)
            await message.answer(
                f"✅ Неделя челленджа обновлена на {new_week}.",
                reply_markup=admin_challenge_actions_kb(ch_id),
            )

        _edit_challenge_state.pop(user_id, None)
        return

    # --- если нет активного режима ---
    await message.answer(
        "Это админ-режим. Используй /start, чтобы открыть меню.",
        reply_markup=admin_main_kb(),
    )


# ===================== выбор недели через кнопки =====================


@router.callback_query(AdminCbFilter(), F.data.startswith("admin_week_"))
async def cb_admin_week_choice(callback: CallbackQuery) -> None:
    try:
        week = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Некорректная неделя", show_alert=True)
        return

    if week < 1 or week > 4:
        await callback.answer("Неделя должна быть от 1 до 4", show_alert=True)
        return

    await update_current_week(week)
    await callback.message.edit_text(
        f"Номер недели цикла обновлён на: {week}",
        reply_markup=admin_main_kb(),
    )
    await callback.answer("Обновлено")


# ===================== режим публикации (manual / auto) =====================


@router.callback_query(AdminCbFilter(), F.data == "admin_set_mode")
async def cb_admin_set_mode(callback: CallbackQuery) -> None:
    schedule = await get_schedule_settings()
    mode = (schedule or {}).get("mode", "manual")
    await callback.message.edit_text(
        "Выбери режим публикации:",
        reply_markup=admin_mode_kb(mode),
    )
    await callback.answer()


@router.callback_query(AdminCbFilter(), F.data == "admin_mode_manual")
async def cb_admin_mode_manual(callback: CallbackQuery) -> None:
    await set_schedule_mode("manual")
    schedule = await get_schedule_settings()
    await callback.message.edit_text(
        "Режим публикации обновлён.",
        reply_markup=admin_mode_kb(schedule.get("mode", "manual")),
    )
    await callback.answer("Режим: ручной")


@router.callback_query(AdminCbFilter(), F.data == "admin_mode_auto")
async def cb_admin_mode_auto(callback: CallbackQuery) -> None:
    await set_schedule_mode("auto")
    schedule = await get_schedule_settings()
    await callback.message.edit_text(
        "Режим публикации обновлён.",
        reply_markup=admin_mode_kb(schedule.get("mode", "auto")),
    )
    await callback.answer("Режим: авто")


# ===================== аналитика по челленджам =====================


@router.callback_query(AdminCbFilter(), F.data == "admin_analytics")
async def cb_admin_analytics(callback: CallbackQuery) -> None:
    rows = await get_analytics(limit=10)
    if not rows:
        await callback.message.edit_text(
            "Пока нет отправленных челленджей для аналитики.",
            reply_markup=admin_main_kb(),
        )
        await callback.answer()
        return

    lines: List[str] = ["📊 Аналитика по последним челленджам:\n"]
    for r in rows:
        date_str = r["challenge_date"].isoformat()
        week = r["week"]
        title = r["title"]
        answers = r["answers_count"]
        sent_at = r["sent_at"]
        sent_str = sent_at.strftime("%d.%m %H:%M") if sent_at else "-"
        lines.append(
            f"• {date_str} · неделя {week}\n"
            f"  {title}\n"
            f"  Ответов: {answers}, отправлен: {sent_str}"
        )

    text = "\n".join(lines)
    await callback.message.edit_text(text, reply_markup=admin_main_kb())
    await callback.answer()
