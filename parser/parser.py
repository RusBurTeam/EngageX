from telethon import TelegramClient
from telethon.tl.types import Channel
import asyncio
import json
from collections import defaultdict
import asyncpg
from datetime import datetime
import os
import re
from dotenv import load_dotenv

# Путь к корню проекта: .../EngageX
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, '.env')

# Загружаем .env из корня
load_dotenv(dotenv_path=ENV_PATH)

# Данные Telegram API
api_id = int(os.getenv('API_ID'))
api_hash = os.getenv('API_HASH')
phone = os.getenv('PHONE')

# Данные PostgreSQL
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'engagex'),
    'user': os.getenv('POSTGRES_USER', 'engagex'),
    'password': os.getenv('POSTGRES_PASSWORD', 'engagex')
}

# Глобальные настройки
COMMENTS_LIMIT_PER_POST = 50
POSTS_LIMIT = 100000


def clean_text(raw: str) -> str:
    """
    Базовая очистка текста для дальнейшей аналитики/обучения:
    - убираем ссылки, @юзернеймы, хэштеги
    - убираем лишние спецсимволы и эмодзи
    - нормализуем пробелы
    """
    if not raw:
        return ""

    text = raw

    # Удаляем ссылки
    text = re.sub(r"http\S+|www\.\S+", " ", text)

    # Удаляем @username
    text = re.sub(r"@\w+", " ", text)

    # Удаляем хэштеги (оставляя слово можно, но пока вырежем целиком)
    text = re.sub(r"#\w+", " ", text)

    # Чистим от всего, кроме букв/цифр/базовой пунктуации
    text = re.sub(r"[^a-zA-Zа-яА-Я0-9\s.,!?;:()\-%]", " ", text)

    # Схлопываем пробелы
    text = re.sub(r"\s+", " ", text)

    return text.strip()


class DatabaseManager:
    def __init__(self, config):
        self.config = config
        self.connection = None

    async def connect(self):
        """Установка соединения с БД"""
        try:
            self.connection = await asyncpg.connect(**self.config)
            print("✅ Подключение к PostgreSQL установлено")
        except Exception as e:
            print(f"❌ Ошибка подключения к PostgreSQL: {e}")
            raise

    async def disconnect(self):
        """Закрытие соединения с БД"""
        if self.connection:
            await self.connection.close()
            print("🔌 Соединение с PostgreSQL закрыто")

    async def init_database(self):
        """Инициализация таблиц в БД"""
        try:
            # Таблица каналов
            await self.connection.execute('''
                CREATE TABLE IF NOT EXISTS telegram_channels (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(255) UNIQUE NOT NULL,
                    title VARCHAR(500),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Таблица постов (сырые данные)
            await self.connection.execute('''
                CREATE TABLE IF NOT EXISTS posts (
                    id SERIAL PRIMARY KEY,
                    channel_username VARCHAR(255) NOT NULL,
                    post_id BIGINT NOT NULL,
                    post_date TIMESTAMP NOT NULL,
                    post_text TEXT,
                    views INTEGER DEFAULT 0,
                    forwards INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(channel_username, post_id)
                )
            ''')

            # Таблица очищенных постов (нормализованный текст для модели)
            await self.connection.execute('''
                CREATE TABLE IF NOT EXISTS clean_posts (
                    id SERIAL PRIMARY KEY,
                    source_post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
                    channel_username VARCHAR(255) NOT NULL,
                    clean_text TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(source_post_id)
                )
            ''')

            # Таблица реакций
            await self.connection.execute('''
                CREATE TABLE IF NOT EXISTS reactions (
                    id SERIAL PRIMARY KEY,
                    post_id INTEGER REFERENCES posts(id),
                    channel_username VARCHAR(255) NOT NULL,
                    reaction_type VARCHAR(100) NOT NULL,
                    reaction_count INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Таблица комментариев (сырые)
            await self.connection.execute('''
                CREATE TABLE IF NOT EXISTS comments (
                    id SERIAL PRIMARY KEY,
                    post_id INTEGER REFERENCES posts(id),
                    channel_username VARCHAR(255) NOT NULL,
                    comment_text TEXT NOT NULL,
                    comment_date TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Таблица очищённых комментариев (нормализованный текст для модели)
            await self.connection.execute('''
                CREATE TABLE IF NOT EXISTS clean_comments (
                    id SERIAL PRIMARY KEY,
                    source_comment_id INTEGER REFERENCES comments(id) ON DELETE CASCADE,
                    channel_username VARCHAR(255) NOT NULL,
                    clean_text TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(source_comment_id)
                )
            ''')

            # Таблица статистики
            await self.connection.execute('''
                CREATE TABLE IF NOT EXISTS parsing_stats (
                    id SERIAL PRIMARY KEY,
                    channel_username VARCHAR(255) NOT NULL,
                    posts_count INTEGER DEFAULT 0,
                    comments_count INTEGER DEFAULT 0,
                    reactions_count INTEGER DEFAULT 0,
                    parsing_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            print("✅ Таблицы в PostgreSQL инициализированы")

        except Exception as e:
            print(f"❌ Ошибка инициализации БД: {e}")

    async def save_post(self, channel_username, post_data):
        """Сохранение поста в БД + создание записи в clean_posts"""
        try:
            post_date = post_data['date']
            if post_date.tzinfo is not None:
                post_date = post_date.replace(tzinfo=None)

            # Сохраняем сырой пост / обновляем при повторном парсинге
            post_db_id = await self.connection.fetchval('''
                INSERT INTO posts (channel_username, post_id, post_date, post_text, views, forwards)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (channel_username, post_id) 
                DO UPDATE SET 
                    post_text = EXCLUDED.post_text,
                    views = EXCLUDED.views,
                    forwards = EXCLUDED.forwards
                RETURNING id
            ''', channel_username,
                 post_data['id'],
                 post_date,
                 post_data['text'],
                 post_data['views'],
                 post_data['forwards'])

            # Чистим текст и сохраняем в clean_posts
            await self.save_clean_post(post_db_id, channel_username, post_data['text'])

            return post_db_id

        except Exception as e:
            print(f"❌ Ошибка сохранения поста: {e}")
            return None

    async def save_clean_post(self, source_post_id: int, channel_username: str, raw_text: str):
        """Сохранение очищенного текста поста в clean_posts (idempotent)"""
        try:
            cleaned = clean_text(raw_text)
            if not cleaned:
                return

            await self.connection.execute('''
                INSERT INTO clean_posts (source_post_id, channel_username, clean_text)
                VALUES ($1, $2, $3)
                ON CONFLICT (source_post_id)
                DO UPDATE SET clean_text = EXCLUDED.clean_text
            ''', source_post_id, channel_username, cleaned)

        except Exception as e:
            print(f"❌ Ошибка сохранения clean_post: {e}")

    async def save_clean_comment(self, source_comment_id: int, channel_username: str, raw_text: str):
        """Сохранение очищённого текста комментария (idempotent)"""
        try:
            cleaned = clean_text(raw_text)
            if not cleaned:
                return

            await self.connection.execute('''
                INSERT INTO clean_comments (source_comment_id, channel_username, clean_text)
                VALUES ($1, $2, $3)
                ON CONFLICT (source_comment_id)
                DO UPDATE SET clean_text = EXCLUDED.clean_text
            ''', source_comment_id, channel_username, cleaned)

        except Exception as e:
            print(f"❌ Ошибка сохранения clean_comment: {e}")

    async def save_reactions(self, post_db_id, channel_username, reactions_dict):
        """Сохранение реакций в БД"""
        try:
            for reaction_type, count in reactions_dict.items():
                await self.connection.execute('''
                    INSERT INTO reactions (post_id, channel_username, reaction_type, reaction_count)
                    VALUES ($1, $2, $3, $4)
                ''', post_db_id, channel_username, reaction_type, count)

            print(f"💾 Сохранено {len(reactions_dict)} типов реакций")
        except Exception as e:
            print(f"❌ Ошибка сохранения реакций: {e}")

    async def save_comments(self, post_db_id, channel_username, comments_list):
        """Сохранение комментариев в БД с последующей очисткой каждого комментария"""
        try:
            saved = 0
            for comment_text in comments_list:
                # вставляем в comments и получаем id записи
                comment_db_id = await self.connection.fetchval('''
                    INSERT INTO comments (post_id, channel_username, comment_text, comment_date)
                    VALUES ($1, $2, $3, $4)
                    RETURNING id
                ''', post_db_id, channel_username, comment_text, datetime.now())

                if comment_db_id:
                    await self.save_clean_comment(comment_db_id, channel_username, comment_text)
                    saved += 1

            print(f"💾 Сохранено {saved} комментариев (и их очищённых версий при наличии).")
        except Exception as e:
            print(f"❌ Ошибка сохранения комментариев: {e}")

    async def save_parsing_stats(self, channel_username, stats):
        """Сохранение статистики парсинга"""
        try:
            await self.connection.execute('''
                INSERT INTO parsing_stats (channel_username, posts_count, comments_count, reactions_count)
                VALUES ($1, $2, $3, $4)
            ''', channel_username, stats['posts_count'], stats['comments_count'], stats['reactions_count'])

            print(f"📊 Статистика сохранена для {channel_username}")
        except Exception as e:
            print(f"❌ Ошибка сохранения статистики: {e}")


async def parse_channel_to_postgres():
    """Парсинг новых постов из Telegram с сохранением в PostgreSQL"""

    db = DatabaseManager(DB_CONFIG)

    try:
        await db.connect()
        await db.init_database()

        client = TelegramClient('session_name', api_id, api_hash)
        await client.start(phone)

        channel_username = 'toncoin_rus'  # TODO: вынести в конфиг
        print(f"🔍 Анализируем канал: @{channel_username}")

        channel = await client.get_entity(channel_username)

        # Получаем ID последнего поста, сохранённого в БД
        last_post_id = await db.connection.fetchval('''
            SELECT MAX(post_id) FROM posts WHERE channel_username = $1
        ''', channel_username)

        if last_post_id:
            print(f"➡️ Найден последний пост ID {last_post_id}, парсим только новые...")
        else:
            print("🆕 В БД нет записей — парсим весь канал с нуля")

        total_posts = 0
        total_comments = 0
        total_reactions = 0

        print("📥 Собираем посты...")

        # Итерация по сообщениям: только новые
        async for message in client.iter_messages(
                channel,
                limit=POSTS_LIMIT,
                offset_id=last_post_id or 0):  # если None — начнёт с нуля
            if message.text:
                total_posts += 1

                post_data = {
                    'id': message.id,
                    'date': message.date,
                    'text': message.text,
                    'views': getattr(message, 'views', 0),
                    'forwards': getattr(message, 'forwards', 0)
                }

                post_db_id = await db.save_post(channel_username, post_data)

                if post_db_id:
                    # Реакции
                    reactions_dict = defaultdict(int)
                    reactions_count = await extract_reactions_to_dict(message, reactions_dict)
                    total_reactions += reactions_count
                    await db.save_reactions(post_db_id, channel_username, dict(reactions_dict))

                    # Комментарии
                    comments_list = await extract_comments_as_strings(client, channel, message)
                    total_comments += len(comments_list)
                    await db.save_comments(post_db_id, channel_username, comments_list)

                    print(f"✅ Новый пост {message.id}: {len(comments_list)} коммент., {reactions_count} реакц.")

        stats = {
            'posts_count': total_posts,
            'comments_count': total_comments,
            'reactions_count': total_reactions
        }
        await db.save_parsing_stats(channel_username, stats)

        print("\n" + "=" * 60)
        print("📊 РЕЗУЛЬТАТЫ ПАРСИНГА:")
        print("=" * 60)
        print(f"📄 Новых постов сохранено: {total_posts}")
        print(f"💬 Комментариев сохранено: {total_comments}")
        print(f"🎭 Реакций сохранено: {total_reactions}")
        print(f"💾 Все данные сохранены в БД 'engagex'")

        await client.disconnect()

    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await db.disconnect()


async def extract_reactions_to_dict(message, reactions_dict):
    """Извлекает реакции и добавляет в словарь"""
    total_reactions = 0

    if not message.reactions:
        return total_reactions

    try:
        if hasattr(message.reactions, 'results'):
            for reaction in message.reactions.results:
                if hasattr(reaction.reaction, 'emoticon'):
                    emoji = reaction.reaction.emoticon
                    reactions_dict[emoji] += reaction.count
                    total_reactions += reaction.count
                elif hasattr(reaction.reaction, 'document_id'):
                    custom_emoji = f"custom_emoji_{reaction.reaction.document_id}"
                    reactions_dict[custom_emoji] += reaction.count
                    total_reactions += reaction.count
                else:
                    reactions_dict['unknown'] += reaction.count
                    total_reactions += reaction.count

    except Exception as e:
        print(f"Ошибка при извлечении реакций: {e}")

    return total_reactions


async def extract_comments_as_strings(client, channel, message):
    """Извлекает комментарии как список строк"""
    comments_strings = []

    try:
        # Метод 1: ответы на пост в том же канале
        async for potential_comment in client.iter_messages(channel, limit=COMMENTS_LIMIT_PER_POST):
            if (hasattr(potential_comment, 'reply_to') and
                    potential_comment.reply_to and
                    hasattr(potential_comment.reply_to, 'reply_to_msg_id') and
                    potential_comment.reply_to.reply_to_msg_id == message.id):

                comment_text = potential_comment.text or potential_comment.message or ''
                if comment_text.strip():
                    comments_strings.append(comment_text)

        # Метод 2: обсуждения (если подключен чат)
        if hasattr(channel, 'username') and channel.username:
            try:
                async for comment in client.iter_messages(channel, reply_to=message.id,
                                                         limit=COMMENTS_LIMIT_PER_POST):
                    comment_text = comment.text or comment.message or ''
                    if comment_text.strip():
                        comments_strings.append(comment_text)
            except:
                pass

    except Exception as e:
        print(f"Ошибка при извлечении комментариев к посту {message.id}: {e}")

    return comments_strings


async def view_saved_data():
    """Просмотр сохраненных данных из БД"""
    db = DatabaseManager(DB_CONFIG)

    try:
        await db.connect()
        stats = await db.connection.fetch('''
            SELECT channel_username, posts_count, comments_count, reactions_count, parsing_date
            FROM parsing_stats 
            ORDER BY parsing_date DESC 
            LIMIT 5
        ''')

        print("\n📈 ПОСЛЕДНЯЯ СТАТИСТИКА ПАРСИНГА:")
        for stat in stats:
            print(f"   Канал: {stat['channel_username']}")
            print(f"   Посты: {stat['posts_count']}, Комментарии: {stat['comments_count']}, Реакции: {stat['reactions_count']}")
            print(f"   Дата: {stat['parsing_date']}")
            print("   " + "-" * 40)

    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await db.disconnect()


if __name__ == "__main__":
    print("🚀 Запуск парсера Telegram с сохранением в PostgreSQL")
    print(f"⚙️  Настройки: {POSTS_LIMIT} постов, до {COMMENTS_LIMIT_PER_POST} комментариев на пост")

    asyncio.run(parse_channel_to_postgres())
    # Для проверки:
    # asyncio.run(view_saved_data())
