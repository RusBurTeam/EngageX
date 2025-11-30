# main.py - основной файл бота
import asyncio
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove

# Токен бота из @BotFather
BOT_TOKEN = "8575150413:AAGgakfoStc9K-J5mZLU8P1Ae8XPnBsSQSU"

# Создаем объекты бота
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

# База данных для хранения вопросов
questions_db = {
    "1": "Как настроить уведомления в системе?",
    "2": "Где найти документацию по проекту?",
    "3": "Как сменить пароль от аккаунта?",
    "4": "Проблема с авторизацией в личном кабинете"
}

# Глобальный словарь для хранения данных пользователя
user_data = {}


# Создаем клавиатуру с кнопками
def get_main_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Узнать больше")],
            [KeyboardButton(text="Ответить")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    return keyboard


# Обработчик команды /start с параметрами
@dp.message(Command("start"))
async def cmd_start(message: Message, command: CommandObject):
    # Получаем параметры после /start
    if command.args:
        # Если передан параметр с question_id
        if command.args.startswith("answer_"):
            question_id = command.args.replace("answer_", "")

            if question_id in questions_db:
                # Сохраняем question_id в контекст пользователя
                user_data[message.from_user.id] = question_id

                await message.answer(
                    f"🔄 <b>Вы отвечаете на вопрос из канала</b>\n\n"
                    f"<b>Вопрос #{question_id}:</b>\n"
                    f"{questions_db[question_id]}\n\n"
                    f"📝 <b>Пожалуйста, напишите ваш ответ:</b>",
                    reply_markup=ReplyKeyboardRemove(),
                    parse_mode=ParseMode.HTML
                )
                return
            else:
                await message.answer("❌ Вопрос не найден в базе данных.")
                return

    # Обычный старт без параметров
    await message.answer(
        "👋 Привет! Я бот для ответов на вопросы из канала EngageX.\n\n"
        "Когда в канале публикуется новый вопрос, нажмите кнопку "
        "'📝 Ответить на вопрос' под постом, и я помогу вам отправить ответ.",
        reply_markup=get_main_keyboard()
    )


# Обработчик кнопки "Узнать больше"
@dp.message(lambda message: message.text == "Узнать больше")
async def learn_more_handler(message: Message):
    await message.answer(
        "🤖 <b>EngageX Support Bot</b>\n\n"
        "Я создан для обработки вопросов из Telegram-канала.\n\n"
        "<b>Как это работает:</b>\n"
        "1. В канале публикуется вопрос с кнопкой\n"
        "2. Вы нажимаете кнопку '📝 Ответить на вопрос'\n"
        "3. Я автоматически открываюсь с готовой формой ответа\n"
        "4. Вы пишете ответ, и он сохраняется\n\n"
        "Попробуйте нажать кнопку под постом в канале!",
        reply_markup=get_main_keyboard(),
        parse_mode=ParseMode.HTML
    )


# Обработчик кнопки "Ответить"
@dp.message(lambda message: message.text == "Ответить")
async def reply_handler(message: Message):
    await message.answer(
        "💬 <b>Режим ответа</b>\n\n"
        "Отправьте мне любое текстовое сообщение, и я его повторю.\n\n"
        "Для ответа на вопросы из канала используйте кнопки под постами.",
        reply_markup=ReplyKeyboardRemove(),
        parse_mode=ParseMode.HTML
    )


# Обработчик текстовых сообщений
@dp.message()
async def echo_handler(message: Message):
    user_id = message.from_user.id

    # Проверяем, отвечает ли пользователь на конкретный вопрос
    if user_id in user_data:
        question_id = user_data[user_id]
        question_text = questions_db.get(question_id, "Неизвестный вопрос")

        # Обрабатываем ответ на вопрос
        await message.answer(
            f"✅ <b>Ответ сохранен!</b>\n\n"
            f"<b>Вопрос #{question_id}:</b>\n"
            f"{question_text}\n\n"
            f"<b>Ваш ответ:</b>\n"
            f"{message.text}\n\n"
            f"📤 <i>Ответ будет передан модераторам</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )

        # Здесь можно сохранить ответ в БД
        print(f"💾 Ответ на вопрос {question_id}: {message.text}")

        # Дополнительно: можно отправить ответ обратно в канал или админам
        # await send_answer_to_channel(question_id, message.text, message.from_user)

        # Удаляем временные данные
        del user_data[user_id]
        return

    # Обычный ответ (если не в режиме ответа на вопрос)
    if message.text in ["/start", "Узнать больше", "Ответить"]:
        return

    await message.answer(
        f"💬 Вы сказали: {message.text}\n\n"
        "Для ответа на вопросы из канала используйте кнопки под постами.\n"
        "Используйте /start для возврата в главное меню.",
        reply_markup=get_main_keyboard()
    )


# Главная функция запуска
async def main():
    print("🤖 Бот запускается...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())