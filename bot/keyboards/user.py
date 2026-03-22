from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def user_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="My Answers", callback_data="user_history")],
            [InlineKeyboardButton(text="Support", callback_data="user_support")],
        ]
    )


def answer_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="My Answers", callback_data="user_history")],
            [InlineKeyboardButton(text="Open Dashboard", callback_data="user_home")],
            [InlineKeyboardButton(text="Cancel Reply", callback_data="user_cancel")],
        ]
    )


def qa_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="My Answers", callback_data="user_history")],
            [InlineKeyboardButton(text="Open Dashboard", callback_data="user_home")],
            [InlineKeyboardButton(text="Exit Q&A Mode", callback_data="user_cancel")],
        ]
    )
