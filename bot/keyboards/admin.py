from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def admin_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Generate Challenges", callback_data="admin_gen_menu")],
            [InlineKeyboardButton(text="Challenge List", callback_data="admin_list_challenges")],
            [InlineKeyboardButton(text="Analytics", callback_data="admin_analytics")],
            [InlineKeyboardButton(text="Settings", callback_data="admin_settings")],
        ]
    )


def admin_gen_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="1 day", callback_data="admin_gen_1"),
                InlineKeyboardButton(text="2 days", callback_data="admin_gen_2"),
                InlineKeyboardButton(text="3 days", callback_data="admin_gen_3"),
            ],
            [
                InlineKeyboardButton(text="4 days", callback_data="admin_gen_4"),
                InlineKeyboardButton(text="5 days", callback_data="admin_gen_5"),
                InlineKeyboardButton(text="6 days", callback_data="admin_gen_6"),
            ],
            [InlineKeyboardButton(text="7 days", callback_data="admin_gen_7")],
            [InlineKeyboardButton(text="Back", callback_data="admin_main")],
        ]
    )


def admin_challenge_actions_kb(ch_id: int) -> InlineKeyboardMarkup:
    """Return keyboard with actions for a single challenge."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Send to Channel", callback_data=f"admin_send_{ch_id}")],
            [
                InlineKeyboardButton(text="Edit", callback_data=f"admin_edit_{ch_id}"),
                InlineKeyboardButton(text="Regenerate", callback_data=f"admin_regen_{ch_id}"),
            ],
            [InlineKeyboardButton(text="Delete", callback_data=f"admin_delete_{ch_id}")],
            [InlineKeyboardButton(text="Back to List", callback_data="admin_list_challenges")],
        ]
    )


def admin_challenge_edit_menu_kb(ch_id: int) -> InlineKeyboardMarkup:
    """Return keyboard for choosing which challenge field to edit."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Edit Title", callback_data=f"admin_edit_title_{ch_id}")],
            [InlineKeyboardButton(text="Edit Body", callback_data=f"admin_edit_body_{ch_id}")],
            [InlineKeyboardButton(text="Edit Date", callback_data=f"admin_edit_date_{ch_id}")],
            [InlineKeyboardButton(text="Edit Week", callback_data=f"admin_edit_week_{ch_id}")],
            [InlineKeyboardButton(text="Back to List", callback_data="admin_list_challenges")],
        ]
    )


def admin_settings_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Topic", callback_data="admin_set_topic"),
                InlineKeyboardButton(text="Product", callback_data="admin_set_product"),
            ],
            [
                InlineKeyboardButton(text="Tone", callback_data="admin_set_tone"),
                InlineKeyboardButton(text="Cycle Week", callback_data="admin_set_week"),
            ],
            [InlineKeyboardButton(text="Posting Mode", callback_data="admin_set_mode")],
            [InlineKeyboardButton(text="Back to Menu", callback_data="admin_main")],
        ]
    )


def admin_mode_kb(current_mode: str) -> InlineKeyboardMarkup:
    manual_label = "Selected: Manual" if current_mode == "manual" else "Manual"
    auto_label = "Selected: Auto" if current_mode == "auto" else "Auto"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=manual_label, callback_data="admin_mode_manual"),
                InlineKeyboardButton(text=auto_label, callback_data="admin_mode_auto"),
            ],
            [InlineKeyboardButton(text="Back", callback_data="admin_settings")],
        ]
    )


def admin_week_kb(current_week: int) -> InlineKeyboardMarkup:
    row = []
    for week_num in range(1, 5):
        label = f"Week {week_num}"
        if week_num == current_week:
            label = f"Selected: Week {week_num}"
        row.append(InlineKeyboardButton(text=label, callback_data=f"admin_week_{week_num}"))

    return InlineKeyboardMarkup(
        inline_keyboard=[
            row,
            [InlineKeyboardButton(text="Back", callback_data="admin_settings")],
        ]
    )
