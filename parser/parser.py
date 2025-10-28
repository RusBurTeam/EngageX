from telethon import TelegramClient
from telethon.tl.types import Channel
import asyncio
import json
from collections import defaultdict
import asyncpg
from datetime import datetime
import os
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Данные Telegram API
api_id = 21818830
api_hash = 'f327a7df09260e8e3ae648399db7f445'
phone = '+79234905464'

# Данные PostgreSQL из .env
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', 5432),
    'database': os.getenv('POSTGRES_DB', 'engagex'),
    'user': os.getenv('POSTGRES_USER', 'engagex'),
    'password': os.getenv('POSTGRES_PASSWORD', 'engagex')
}

# Глобальные настройки
COMMENTS_LIMIT_PER_POST = 50
POSTS_LIMIT = 20


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

            # Таблица постов
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

            # Таблица комментариев
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
        """Сохранение поста в БД"""
        try:
            # Преобразуем datetime в timezone-naive для PostgreSQL
            post_date = post_data['date']
            if post_date.tzinfo is not None:
                post_date = post_date.replace(tzinfo=None)

            # Вставляем или обновляем пост
            post_id = await self.connection.fetchval('''
                INSERT INTO posts (channel_username, post_id, post_date, post_text, views, forwards)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (channel_username, post_id) 
                DO UPDATE SET 
                    post_text = EXCLUDED.post_text,
                    views = EXCLUDED.views,
                    forwards = EXCLUDED.forwards
                RETURNING id
            ''', channel_username, post_data['id'], post_date,
                                                     post_data['text'], post_data['views'], post_data['forwards'])

            return post_id
        except Exception as e:
            print(f"❌ Ошибка сохранения поста: {e}")
            return None

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
        """Сохранение комментариев в БД"""
        try:
            for comment_text in comments_list:
                await self.connection.execute('''
                    INSERT INTO comments (post_id, channel_username, comment_text, comment_date)
                    VALUES ($1, $2, $3, $4)
                ''', post_db_id, channel_username, comment_text, datetime.now())

            print(f"💾 Сохранено {len(comments_list)} комментариев")
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
    """Основная функция парсинга с сохранением в PostgreSQL"""

    # Инициализация менеджера БД
    db = DatabaseManager(DB_CONFIG)

    try:
        # Подключаемся к БД
        await db.connect()
        await db.init_database()

        # Подключаемся к Telegram
        client = TelegramClient('session_name', api_id, api_hash)
        await client.start(phone)

        channel_username = 'durov'  # Можно изменить на любой канал

        print(f"🔍 Анализируем канал: @{channel_username}")
        channel = await client.get_entity(channel_username)

        # Статистика
        total_posts = 0
        total_comments = 0
        total_reactions = 0

        # Собираем посты
        print("📥 Собираем посты...")
        async for message in client.iter_messages(channel, limit=POSTS_LIMIT):
            if message.text:  # Только посты с текстом
                total_posts += 1

                # Данные поста
                post_data = {
                    'id': message.id,
                    'date': message.date,
                    'text': message.text,
                    'views': getattr(message, 'views', 0),
                    'forwards': getattr(message, 'forwards', 0)
                }

                # Сохраняем пост в БД
                post_db_id = await db.save_post(channel_username, post_data)

                if post_db_id:
                    # Собираем реакции
                    reactions_dict = defaultdict(int)
                    reactions_count = await extract_reactions_to_dict(message, reactions_dict)
                    total_reactions += reactions_count

                    # Сохраняем реакции
                    await db.save_reactions(post_db_id, channel_username, dict(reactions_dict))

                    # Собираем комментарии
                    comments_list = await extract_comments_as_strings(client, channel, message)
                    total_comments += len(comments_list)

                    # Сохраняем комментарии
                    await db.save_comments(post_db_id, channel_username, comments_list)

                    print(f"✅ Пост {message.id}: {len(comments_list)} коммент., {reactions_count} реакц.")

        # Сохраняем статистику
        stats = {
            'posts_count': total_posts,
            'comments_count': total_comments,
            'reactions_count': total_reactions
        }
        await db.save_parsing_stats(channel_username, stats)

        # Выводим итоги
        print("\n" + "=" * 60)
        print("📊 РЕЗУЛЬТАТЫ СОХРАНЕНИЯ В POSTGRESQL:")
        print("=" * 60)
        print(f"📄 Постов сохранено: {total_posts}")
        print(f"💬 Комментариев сохранено: {total_comments}")
        print(f"🎭 Реакций сохранено: {total_reactions}")
        print(f"💾 Все данные сохранены в БД 'engagex'")

        await client.disconnect()

    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await db.disconnect()


# Функции для извлечения данных (остаются без изменений)
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
        # Метод 1: Ищем сообщения, которые являются ответами на этот пост
        async for potential_comment in client.iter_messages(channel, limit=COMMENTS_LIMIT_PER_POST):
            if (hasattr(potential_comment, 'reply_to') and
                    potential_comment.reply_to and
                    hasattr(potential_comment.reply_to, 'reply_to_msg_id') and
                    potential_comment.reply_to.reply_to_msg_id == message.id):

                comment_text = potential_comment.text or potential_comment.message or ''
                if comment_text.strip():
                    comments_strings.append(comment_text)

        # Метод 2: Для каналов с обсуждениями
        if hasattr(channel, 'username') and channel.username:
            try:
                async for comment in client.iter_messages(channel, reply_to=message.id, limit=COMMENTS_LIMIT_PER_POST):
                    comment_text = comment.text or comment.message or ''
                    if comment_text.strip():
                        comments_strings.append(comment_text)
            except:
                pass

    except Exception as e:
        print(f"Ошибка при извлечении комментариев к посту {message.id}: {e}")

    return comments_strings


# Дополнительные функции для работы с данными
async def view_saved_data():
    """Просмотр сохраненных данных из БД"""
    db = DatabaseManager(DB_CONFIG)

    try:
        await db.connect()

        # Получаем статистику
        stats = await db.connection.fetch('''
            SELECT channel_username, posts_count, comments_count, reactions_count, parsing_date
            FROM parsing_stats 
            ORDER BY parsing_date DESC 
            LIMIT 5
        ''')

        print("\n📈 ПОСЛЕДНЯЯ СТАТИСТИКА ПАРСИНГА:")
        for stat in stats:
            print(f"   Канал: {stat['channel_username']}")
            print(
                f"   Посты: {stat['posts_count']}, Комментарии: {stat['comments_count']}, Реакции: {stat['reactions_count']}")
            print(f"   Дата: {stat['parsing_date']}")
            print("   " + "-" * 40)

    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await db.disconnect()


if __name__ == "__main__":
    print("🚀 Запуск парсера Telegram с сохранением в PostgreSQL")
    print(f"⚙️  Настройки: {POSTS_LIMIT} постов, до {COMMENTS_LIMIT_PER_POST} комментариев на пост")

    # Запуск парсера
    asyncio.run(parse_channel_to_postgres())

    # Просмотр сохраненных данных
    # asyncio.run(view_saved_data())