# channel.py - скрипт для публикации постов в канал
import asyncio
from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# Токен бота из @BotFather
BOT_TOKEN = "8575150413:AAGgakfoStc9K-J5mZLU8P1Ae8XPnBsSQSU"


# Функция для создания инлайн-кнопки "Ответить на вопрос"
def create_answer_button(question_id: str):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="📝 Ответить на вопрос",
                url=f"https://t.me/test19299399_bot?start=answer_{question_id}"
            )]
        ]
    )
    return keyboard


async def post_question_to_channel():
    bot = Bot(token=BOT_TOKEN)

    # Используем ID канала
    channel_id = -1003234616660

    try:
        # Проверим информацию о канале
        chat = await bot.get_chat(channel_id)
        print(f"📢 Канал найден: {chat.title}")

        # Создаем несколько тестовых вопросов
        questions = [
            ("1", "Как настроить уведомления в системе?"),
            ("2", "Где найти документацию по проекту?"),
            ("3", "Как сменить пароль от аккаунта?"),
            ("4", "Проблема с авторизацией в личном кабинете")
        ]

        for question_id, question_text in questions:
            # Создаем кнопку для ответа
            keyboard = create_answer_button(question_id)

            # Публикуем пост в канал
            message = await bot.send_message(
                chat_id=channel_id,
                text=f"❓ <b>Вопрос #{question_id}</b>\n\n"
                     f"{question_text}\n\n"
                     f"<i>Нажмите кнопку ниже чтобы ответить на этот вопрос</i>",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )

            print(f"✅ Вопрос #{question_id} отправлен в канал!")
            print(f"🔗 Ссылка на пост: https://t.me/testtest120934/{message.message_id}")
            print(f"🔗 Deep link: https://t.me/test19299399_bot?start=answer_{question_id}")
            print("-" * 50)

            # Пауза между сообщениями
            await asyncio.sleep(2)

        print("\n🎉 Все вопросы опубликованы!")
        print("📋 Для тестирования:")
        print("1. Перейдите в канал: https://t.me/testtest120934")
        print("2. Нажмите на кнопку '📝 Ответить на вопрос' под любым постом")
        print("3. Должен открыться бот с готовой формой ответа")

    except Exception as e:
        print(f"❌ Ошибка при отправке сообщения: {e}")

    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(post_question_to_channel())