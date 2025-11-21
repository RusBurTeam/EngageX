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
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

BASE_DIR = os.path.dirname(SCRIPT_DIR)
ENV_PATH = os.path.join(BASE_DIR, '.env')

CHANNELS_CONFIG_PATH = os.path.join(SCRIPT_DIR, 'channels.json')

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
    Базовая очистка текста:
    - убираем ссылки, @юзернеймы, хэштеги
    - убираем лишние спецсимволы и эмодзи
    - нормализуем пробелы
    """
    if not raw:
        return ""

    text = raw
    text = re.sub(r"http\S+|www\.\S+", " ", text)          # ссылки
    text = re.sub(r"@\w+", " ", text)                      # @username
    text = re.sub(r"#\w+", " ", text)                      # хэштеги
    text = re.sub(r"[^a-zA-Zа-яА-Я0-9\s.,!?;:()\-%]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def load_channels_from_config():
    """
    Загружаем список каналов из channels.json.
    Формат:
    {
      "channels": ["toncoin_rus", "another_channel"]
    }
    """
    try:
        with open(CHANNELS_CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        # fallback — один канал из .env или дефолтный
        ch = os.getenv("TG_CHANNEL_USERNAME", "toncoin_rus")
        print(f"⚠️ channels.json не найден, используем один канал: {ch}")
        return [ch]

    if isinstance(data, dict) and "channels" in data and isinstance(data["channels"], list):
        channels = [str(c).strip().lstrip("@") for c in data["channels"] if str(c).strip()]
        if not channels:
            raise RuntimeError("channels.json прочитан, но список channels пуст.")
        return channels

    raise RuntimeError("Некорректный формат channels.json. Ожидается {\"channels\": [..]}.")


class DatabaseManager:
    def __init__(self, config):
        self.config = config
        self.connection: asyncpg.Connection | None = None

    async def connect(self):
        try:
            self.connection = await asyncpg.connect(**self.config)
            print("✅ Подключение к PostgreSQL установлено")
        except Exception as e:
            print(f"❌ Ошибка подключения к PostgreSQL: {e}")
            raise

    async def disconnect(self):
        if self.connection:
            await self.connection.close()
            print("🔌 Соединение с PostgreSQL закрыто")

    async def init_database(self):
        """Инициализация таблиц в БД (ядро проекта + ingest_status)."""
        try:
            # Базовое создание таблиц
            await self.connection.execute('''
                CREATE TABLE IF NOT EXISTS posts (
                    id SERIAL PRIMARY KEY,
                    channel_username VARCHAR(255) NOT NULL,
                    post_id BIGINT NOT NULL,
                    post_date TIMESTAMP NOT NULL,
                    post_text TEXT,
                    views INTEGER DEFAULT 0,
                    forwards INTEGER DEFAULT 0,
                    processing_status VARCHAR(32) NOT NULL DEFAULT 'new',
                    processor_pid INTEGER,
                    processing_started_at TIMESTAMP,
                    attempts INTEGER DEFAULT 0,
                    ingest_status VARCHAR(16) NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(channel_username, post_id)
                )
            ''')

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

            await self.connection.execute('''
                CREATE TABLE IF NOT EXISTS reactions (
                    id SERIAL PRIMARY KEY,
                    post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
                    channel_username VARCHAR(255) NOT NULL,
                    reaction_type VARCHAR(100) NOT NULL,
                    reaction_count INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            await self.connection.execute('''
                CREATE TABLE IF NOT EXISTS comments (
                    id SERIAL PRIMARY KEY,
                    post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
                    channel_username VARCHAR(255) NOT NULL,
                    comment_text TEXT NOT NULL,
                    comment_date TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            await self.connection.execute('''
                CREATE TABLE IF NOT EXISTS post_quality (
                    post_id INTEGER PRIMARY KEY REFERENCES posts(id) ON DELETE CASCADE,
                    channel_username VARCHAR(255) NOT NULL,
                    quality_score NUMERIC,
                    is_good BOOLEAN,
                    signals JSONB,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Мягкая миграция: добавить колонку ingest_status, если таблица уже существовала
            await self.connection.execute('''
                ALTER TABLE posts
                ADD COLUMN IF NOT EXISTS ingest_status VARCHAR(16) NOT NULL DEFAULT 'pending'
            ''')

            print("✅ Таблицы в PostgreSQL инициализированы (ядро проекта + ingest_status)")

        except Exception as e:
            print(f"❌ Ошибка инициализации БД: {e}")
            raise

    async def save_post(self, channel_username, post_data):
        """Сохранение поста в БД + clean_posts.
        На этом уровне считаем, что если запрос прошёл — статус можно будет перевести в 'done' снаружи.
        """
        try:
            post_date = post_data['date']
            if post_date.tzinfo is not None:
                post_date = post_date.replace(tzinfo=None)

            post_db_id = await self.connection.fetchval('''
                INSERT INTO posts (
                    channel_username, post_id, post_date, post_text, views, forwards
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (channel_username, post_id) 
                DO UPDATE SET 
                    post_text   = EXCLUDED.post_text,
                    views       = EXCLUDED.views,
                    forwards    = EXCLUDED.forwards
                RETURNING id
            ''',
                channel_username,
                post_data['id'],
                post_date,
                post_data['text'],
                post_data['views'],
                post_data['forwards']
            )

            await self.save_clean_post(post_db_id, channel_username, post_data['text'])
            return post_db_id

        except Exception as e:
            print(f"❌ Ошибка сохранения поста: {e}")
            return None

    async def save_clean_post(self, source_post_id: int, channel_username: str, raw_text: str):
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
            raise

    async def save_reactions(self, post_db_id, channel_username, reactions_dict):
        try:
            if not reactions_dict:
                return

            for reaction_type, count in reactions_dict.items():
                await self.connection.execute('''
                    INSERT INTO reactions (post_id, channel_username, reaction_type, reaction_count)
                    VALUES ($1, $2, $3, $4)
                ''', post_db_id, channel_username, reaction_type, count)

            print(f"💾 Сохранено {len(reactions_dict)} типов реакций")
        except Exception as e:
            print(f"❌ Ошибка сохранения реакций: {e}")
            raise

    async def save_comments(self, post_db_id, channel_username, comments_list):
        try:
            saved = 0
            for comment_text in comments_list:
                comment_text = (comment_text or '').strip()
                if not comment_text:
                    continue

                await self.connection.execute('''
                    INSERT INTO comments (post_id, channel_username, comment_text, comment_date)
                    VALUES ($1, $2, $3, $4)
                ''', post_db_id, channel_username, comment_text, datetime.now())
                saved += 1

            print(f"💾 Сохранено {saved} комментариев.")
        except Exception as e:
            print(f"❌ Ошибка сохранения комментариев: {e}")
            raise

    async def update_ingest_status(self, post_db_id: int, status: str):
        """Обновляем ingest_status для конкретного поста: 'done' / 'error' / 'pending'."""
        try:
            await self.connection.execute(
                '''UPDATE posts SET ingest_status = $1 WHERE id = $2''',
                status,
                post_db_id
            )
        except Exception as e:
            print(f"❌ Ошибка обновления ingest_status для post_id={post_db_id}: {e}")

    async def print_ingest_status_stats(self):
        """Простая аналитика: сколько постов в каком статусе ingest_status."""
        try:
            rows = await self.connection.fetch('''
                SELECT ingest_status, COUNT(*) AS cnt
                FROM posts
                GROUP BY ingest_status
                ORDER BY ingest_status
            ''')
            print("\n📈 Статусы парсинга постов (posts.ingest_status):")
            if not rows:
                print("   (таблица posts пуста)")
                return
            for row in rows:
                status = row["ingest_status"]
                cnt = row["cnt"]
                print(f"   {status:>7}: {cnt}")
        except Exception as e:
            print(f"❌ Ошибка аналитики статуса постов: {e}")


async def parse_single_channel(db: DatabaseManager, client: TelegramClient, channel_username: str):
    """Парсит один канал и пишет в БД только новые посты.
    Для каждого поста:
    - по умолчанию считаем статус 'error';
    - если весь пайплайн (post + reactions + comments) отработал — ставим 'done'.
    """
    print(f"\n🔍 Анализируем канал: @{channel_username}")

    channel = await client.get_entity(channel_username)

    last_post_id = await db.connection.fetchval('''
        SELECT MAX(post_id) FROM posts WHERE channel_username = $1
    ''', channel_username)

    if last_post_id:
        print(f"➡️ Последний Telegram post_id={last_post_id}, парсим ТОЛЬКО новые (id > {last_post_id})...")
    else:
        print("🆕 В БД нет записей по этому каналу — парсим весь канал с нуля")

    total_posts = 0
    total_comments = 0
    total_reactions = 0

    print("📥 Собираем посты...")

    async for message in client.iter_messages(
        channel,
        limit=POSTS_LIMIT,
        min_id=last_post_id or 0
    ):
        if not message.text:
            continue

        total_posts += 1

        # По умолчанию считаем, что что-то пойдёт не так
        post_status = 'error'
        post_db_id = None
        post_comments_count = 0
        post_reactions_count = 0

        try:
            post_data = {
                'id': message.id,
                'date': message.date,
                'text': message.text,
                'views': getattr(message, 'views', 0),
                'forwards': getattr(message, 'forwards', 0)
            }

            post_db_id = await db.save_post(channel_username, post_data)
            if not post_db_id:
                raise RuntimeError("save_post вернул None — строка в posts не создана")

            # Реакции
            reactions_dict = defaultdict(int)
            post_reactions_count = await extract_reactions_to_dict(message, reactions_dict)
            total_reactions += post_reactions_count
            await db.save_reactions(post_db_id, channel_username, dict(reactions_dict))

            # Комментарии
            comments_list = await extract_comments_as_strings(client, channel, message)
            post_comments_count = len(comments_list)
            total_comments += post_comments_count
            await db.save_comments(post_db_id, channel_username, comments_list)

            # Если дошли до сюда без исключений — считаем, что всё ок
            post_status = 'done'

            print(
                f"✅ Новый пост {message.id}: "
                f"{post_comments_count} коммент., {post_reactions_count} реакц. "
                f"[ingest_status={post_status}]"
            )

        except Exception as e:
            print(f"❌ Ошибка при обработке поста {message.id}: {e}")

        finally:
            # Если хотя бы пост в posts создан — отмечаем его статус
            if post_db_id:
                await db.update_ingest_status(post_db_id, post_status)

    print("\n" + "-" * 60)
    print(f"📊 ИТОГИ КАНАЛА @{channel_username}:")
    print(f"📄 Новых постов: {total_posts}")
    print(f"💬 Комментариев: {total_comments}")
    print(f"🎭 Реакций: {total_reactions}")
    print("-" * 60)


async def parse_channel_to_postgres():
    """Парсинг всех каналов из channels.json в PostgreSQL + аналитика по ingest_status."""
    db = DatabaseManager(DB_CONFIG)

    try:
        channels = load_channels_from_config()

        print("📚 Каналы из конфигурации (после нормализации):")
        for c in channels:
            print(f"   - @{c}")

        await db.connect()
        await db.init_database()

        client = TelegramClient('session_name', api_id, api_hash)
        await client.start(phone)

        for ch in channels:
            try:
                print("\n==============================")
                print(f"▶ Старт парсинга канала @{ch}")
                print("==============================")
                await parse_single_channel(db, client, ch)
            except Exception as e:
                print(f"❌ Ошибка при обработке канала @{ch}: {e}")

        # После обхода всех каналов — выводим аналитику по ingest_status
        print("\n📊 Сводка по полю posts.ingest_status после парсинга:")
        await db.print_ingest_status_stats()

        await client.disconnect()
        print("\n✅ Парсинг всех каналов завершён")

    except Exception as e:
        print(f"❌ Общая ошибка верхнего уровня: {e}")
    finally:
        await db.disconnect()


async def extract_reactions_to_dict(message, reactions_dict):
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
        raise

    return total_reactions


async def extract_comments_as_strings(client, channel, message):
    comments_strings = []

    try:
        async for comment in client.iter_messages(
            channel,
            reply_to=message.id,
            limit=COMMENTS_LIMIT_PER_POST
        ):
            comment_text = comment.text or comment.message or ''
            if comment_text.strip():
                comments_strings.append(comment_text)

    except Exception as e:
        print(f"Ошибка при извлечении комментариев к посту {message.id}: {e}")
        raise

    return comments_strings


if __name__ == "__main__":
    print("🚀 Запуск парсера Telegram с сохранением в PostgreSQL")
    print(f"⚙️  Настройки: {POSTS_LIMIT} постов, до {COMMENTS_LIMIT_PER_POST} комментариев на пост")
    asyncio.run(parse_channel_to_postgres())
