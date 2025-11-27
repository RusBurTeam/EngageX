
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def user_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📜 Мои ответы",
                    callback_data="user_history",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🆘 Поддержка",
                    callback_data="user_support",
                )
            ],
        ]
    )


def answer_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📜 Мои ответы",
                    callback_data="user_history",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🚀 В кабинет",
                    callback_data="user_home",
                )
            ],
            [
                InlineKeyboardButton(
                    text="❌ Отменить ответ",
                    callback_data="user_cancel",
                )
            ],
        ]
    )


def qa_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📜 Мои ответы",
                    callback_data="user_history",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🚀 В кабинет",
                    callback_data="user_home",
                )
            ],
            [
                InlineKeyboardButton(
                    text="❌ Выйти из режима вопросов",
                    callback_data="user_cancel",
                )
            ],
        ]
    )
