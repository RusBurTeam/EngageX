from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def admin_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🧩 Сгенерировать челленджи",
                    callback_data="admin_gen_menu",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📋 Список челленджей",
                    callback_data="admin_list_challenges",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📊 Аналитика",
                    callback_data="admin_analytics",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⚙️ Настройки",
                    callback_data="admin_settings",
                )
            ],
        ]
    )


def admin_gen_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="1 день", callback_data="admin_gen_1"),
                InlineKeyboardButton(text="2 дня", callback_data="admin_gen_2"),
                InlineKeyboardButton(text="3 дня", callback_data="admin_gen_3"),
            ],
            [
                InlineKeyboardButton(text="4 дня", callback_data="admin_gen_4"),
                InlineKeyboardButton(text="5 дней", callback_data="admin_gen_5"),
                InlineKeyboardButton(text="6 дней", callback_data="admin_gen_6"),
            ],
            [
                InlineKeyboardButton(text="7 дней", callback_data="admin_gen_7"),
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Назад",
                    callback_data="admin_main",
                )
            ],
        ]
    )


def admin_challenge_actions_kb(ch_id: int) -> InlineKeyboardMarkup:
    """
    Кнопки действий для конкретного челленджа.
    """
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📤 Отправить в канал",
                    callback_data=f"admin_send_{ch_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="✏️ Редактировать",
                    callback_data=f"admin_edit_{ch_id}",
                ),
                InlineKeyboardButton(
                    text="♻️ Перегенерировать",
                    callback_data=f"admin_regen_{ch_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🗑 Удалить",
                    callback_data=f"admin_delete_{ch_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Назад к списку",
                    callback_data="admin_list_challenges",
                ),
            ],
        ]
    )


def admin_challenge_edit_menu_kb(ch_id: int) -> InlineKeyboardMarkup:
    """
    Меню «что именно редактировать».
    """
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📝 Изменить заголовок",
                    callback_data=f"admin_edit_title_{ch_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🧾 Изменить текст",
                    callback_data=f"admin_edit_body_{ch_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📅 Изменить дату",
                    callback_data=f"admin_edit_date_{ch_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📆 Изменить неделю",
                    callback_data=f"admin_edit_week_{ch_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Назад к списку",
                    callback_data="admin_list_challenges",
                ),
            ],
        ]
    )


def admin_settings_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🧩 Тематика",
                    callback_data="admin_set_topic",
                ),
                InlineKeyboardButton(
                    text="📦 Продукт",
                    callback_data="admin_set_product",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🎙 Тональность",
                    callback_data="admin_set_tone",
                ),
                InlineKeyboardButton(
                    text="📅 Неделя цикла",
                    callback_data="admin_set_week",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🤖 Режим публикации",
                    callback_data="admin_set_mode",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ В меню",
                    callback_data="admin_main",
                ),
            ],
        ]
    )


def admin_mode_kb(current_mode: str) -> InlineKeyboardMarkup:
    manual_label = "✅ Ручной" if current_mode == "manual" else "Ручной"
    auto_label = "✅ Авто" if current_mode == "auto" else "Авто"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=manual_label,
                    callback_data="admin_mode_manual",
                ),
                InlineKeyboardButton(
                    text=auto_label,
                    callback_data="admin_mode_auto",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Назад",
                    callback_data="admin_settings",
                ),
            ],
        ]
    )


def admin_week_kb(current_week: int) -> InlineKeyboardMarkup:
    row = []
    for w in range(1, 5):
        label = f"Неделя {w}"
        if w == current_week:
            label = f"✅ Неделя {w}"
        row.append(
            InlineKeyboardButton(
                text=label,
                callback_data=f"admin_week_{w}",
            )
        )

    return InlineKeyboardMarkup(
        inline_keyboard=[
            row,
            [
                InlineKeyboardButton(
                    text="⬅️ Назад",
                    callback_data="admin_settings",
                ),
            ],
        ]
    )
