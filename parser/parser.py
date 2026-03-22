from __future__ import annotations

import asyncio
import json
import os
import re
from collections import defaultdict
from datetime import datetime

import asyncpg
from dotenv import load_dotenv
from telethon import TelegramClient

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
ENV_PATH = os.path.join(BASE_DIR, '.env')
CHANNELS_CONFIG_PATH = os.path.join(SCRIPT_DIR, 'channels.json')

load_dotenv(dotenv_path=ENV_PATH)

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
PHONE = os.getenv('PHONE')

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'engagex'),
    'user': os.getenv('POSTGRES_USER', 'engagex'),
    'password': os.getenv('POSTGRES_PASSWORD', 'engagex'),
}

COMMENTS_LIMIT_PER_POST = 50
POSTS_LIMIT = 100000


def clean_text(raw: str) -> str:
    """Normalize raw post text before storing in clean_posts."""
    if not raw:
        return ""

    text = raw
    text = re.sub(r"http\S+|www\.\S+", " ", text)
    text = re.sub(r"@\w+", " ", text)
    text = re.sub(r"#\w+", " ", text)
    text = re.sub(r"[^a-zA-Z\u0400-\u04FF0-9\s.,!?;:()\-%]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def load_channels_from_config() -> list[str]:
    """Load channel usernames from channels.json."""
    try:
        with open(CHANNELS_CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        ch = os.getenv("TG_CHANNEL_USERNAME", "toncoin_rus")
        print(f"[warn] channels.json not found, using a single channel: {ch}")
        return [ch]

    if isinstance(data, dict) and isinstance(data.get("channels"), list):
        channels = [str(c).strip().lstrip("@") for c in data["channels"] if str(c).strip()]
        if not channels:
            raise RuntimeError("channels.json was loaded but the channels list is empty.")
        return channels

    raise RuntimeError("Invalid channels.json format. Expected: {\"channels\": [..]}.")


class DatabaseManager:
    def __init__(self, config: dict):
        self.config = config
        self.connection: asyncpg.Connection | None = None

    async def connect(self) -> None:
        try:
            self.connection = await asyncpg.connect(**self.config)
            print("[ok] Connected to PostgreSQL")
        except Exception as exc:
            print(f"[error] PostgreSQL connection failed: {exc}")
            raise

    async def disconnect(self) -> None:
        if self.connection:
            await self.connection.close()
            print("[ok] PostgreSQL connection closed")

    async def init_database(self) -> None:
        """Create required tables if they do not exist."""
        try:
            await self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS posts (
                    id SERIAL PRIMARY KEY,
                    channel_username VARCHAR(255) NOT NULL,
                    post_id BIGINT NOT NULL,
                    post_date TIMESTAMP NOT NULL,
                    post_text TEXT,
                    views INTEGER DEFAULT 0,
                    forwards INTEGER DEFAULT 0,
                    ingest_status VARCHAR(16) NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(channel_username, post_id)
                )
                """
            )

            await self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS clean_posts (
                    id SERIAL PRIMARY KEY,
                    source_post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
                    channel_username VARCHAR(255) NOT NULL,
                    clean_text TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(source_post_id)
                )
                """
            )

            await self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS reactions (
                    id SERIAL PRIMARY KEY,
                    post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
                    channel_username VARCHAR(255) NOT NULL,
                    reaction_type VARCHAR(100) NOT NULL,
                    reaction_count INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            await self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS comments (
                    id SERIAL PRIMARY KEY,
                    post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
                    channel_username VARCHAR(255) NOT NULL,
                    comment_text TEXT NOT NULL,
                    comment_date TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            await self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS post_quality (
                    post_id INTEGER PRIMARY KEY REFERENCES posts(id) ON DELETE CASCADE,
                    channel_username VARCHAR(255) NOT NULL,
                    quality_score NUMERIC,
                    is_good BOOLEAN,
                    signals JSONB,
                    gen_status VARCHAR(32) NOT NULL DEFAULT 'ok',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            print("[ok] Database schema is initialized")
        except Exception as exc:
            print(f"[error] Database initialization failed: {exc}")
            raise

    async def save_post(self, channel_username: str, post_data: dict) -> int | None:
        """Insert/update a post and save its cleaned text."""
        try:
            post_date = post_data['date']
            if post_date.tzinfo is not None:
                post_date = post_date.replace(tzinfo=None)

            post_db_id = await self.connection.fetchval(
                """
                INSERT INTO posts (
                    channel_username, post_id, post_date, post_text, views, forwards
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (channel_username, post_id)
                DO UPDATE SET
                    post_text = EXCLUDED.post_text,
                    views = EXCLUDED.views,
                    forwards = EXCLUDED.forwards
                RETURNING id
                """,
                channel_username,
                post_data['id'],
                post_date,
                post_data['text'],
                post_data['views'],
                post_data['forwards'],
            )

            await self.save_clean_post(post_db_id, channel_username, post_data['text'])
            return post_db_id
        except Exception as exc:
            print(f"[error] Failed to save post: {exc}")
            return None

    async def save_clean_post(self, source_post_id: int, channel_username: str, raw_text: str) -> None:
        cleaned = clean_text(raw_text)
        if not cleaned:
            return

        try:
            await self.connection.execute(
                """
                INSERT INTO clean_posts (source_post_id, channel_username, clean_text)
                VALUES ($1, $2, $3)
                ON CONFLICT (source_post_id)
                DO UPDATE SET clean_text = EXCLUDED.clean_text
                """,
                source_post_id,
                channel_username,
                cleaned,
            )
        except Exception as exc:
            print(f"[error] Failed to save clean_post: {exc}")
            raise

    async def save_reactions(self, post_db_id: int, channel_username: str, reactions_dict: dict) -> None:
        if not reactions_dict:
            return

        try:
            for reaction_type, count in reactions_dict.items():
                await self.connection.execute(
                    """
                    INSERT INTO reactions (post_id, channel_username, reaction_type, reaction_count)
                    VALUES ($1, $2, $3, $4)
                    """,
                    post_db_id,
                    channel_username,
                    reaction_type,
                    count,
                )
            print(f"[ok] Saved {len(reactions_dict)} reaction types")
        except Exception as exc:
            print(f"[error] Failed to save reactions: {exc}")
            raise

    async def save_comments(self, post_db_id: int, channel_username: str, comments_list: list[str]) -> None:
        try:
            saved = 0
            for comment_text in comments_list:
                comment_text = (comment_text or '').strip()
                if not comment_text:
                    continue

                await self.connection.execute(
                    """
                    INSERT INTO comments (post_id, channel_username, comment_text, comment_date)
                    VALUES ($1, $2, $3, $4)
                    """,
                    post_db_id,
                    channel_username,
                    comment_text,
                    datetime.now(),
                )
                saved += 1
            print(f"[ok] Saved {saved} comments")
        except Exception as exc:
            print(f"[error] Failed to save comments: {exc}")
            raise

    async def update_ingest_status(self, post_db_id: int, status: str) -> None:
        try:
            await self.connection.execute(
                "UPDATE posts SET ingest_status = $1 WHERE id = $2",
                status,
                post_db_id,
            )
        except Exception as exc:
            print(f"[error] Failed to update ingest_status for post_id={post_db_id}: {exc}")

    async def get_done_post_ids(self, channel_username: str) -> set[int]:
        rows = await self.connection.fetch(
            """
            SELECT post_id
            FROM posts
            WHERE channel_username = $1 AND ingest_status = 'done'
            """,
            channel_username,
        )
        return {row['post_id'] for row in rows}

    async def print_ingest_status_stats(self) -> None:
        try:
            rows = await self.connection.fetch(
                """
                SELECT ingest_status, COUNT(*) AS cnt
                FROM posts
                GROUP BY ingest_status
                ORDER BY ingest_status
                """
            )
            print("\n[stats] posts.ingest_status:")
            if not rows:
                print("  posts table is empty")
                return
            for row in rows:
                print(f"  {row['ingest_status']:>7}: {row['cnt']}")
        except Exception as exc:
            print(f"[error] Failed to print ingest status stats: {exc}")


async def parse_single_channel(db: DatabaseManager, client: TelegramClient, channel_username: str) -> None:
    print(f"\n[scan] Channel: @{channel_username}")
    channel = await client.get_entity(channel_username)

    done_ids = await db.get_done_post_ids(channel_username)
    print(f"[info] Already done: {len(done_ids)} posts")

    total_posts = 0
    total_comments = 0
    total_reactions = 0
    processed_new = 0

    print("[info] Reading full post history (oldest to newest)...")
    async for message in client.iter_messages(channel, limit=POSTS_LIMIT, reverse=True):
        if not message.text or message.id in done_ids:
            continue

        total_posts += 1
        post_status = 'error'
        post_db_id = None

        try:
            post_data = {
                'id': message.id,
                'date': message.date,
                'text': message.text,
                'views': getattr(message, 'views', 0),
                'forwards': getattr(message, 'forwards', 0),
            }

            post_db_id = await db.save_post(channel_username, post_data)
            if not post_db_id:
                raise RuntimeError("save_post returned None")

            reactions_dict = defaultdict(int)
            post_reactions_count = await extract_reactions_to_dict(message, reactions_dict)
            total_reactions += post_reactions_count
            await db.save_reactions(post_db_id, channel_username, dict(reactions_dict))

            comments_list = await extract_comments_as_strings(client, channel, message)
            post_comments_count = len(comments_list)
            total_comments += post_comments_count
            await db.save_comments(post_db_id, channel_username, comments_list)

            post_status = 'done'
            processed_new += 1
            print(
                f"[ok] Post {message.id}: "
                f"{post_comments_count} comments, {post_reactions_count} reactions "
                f"[ingest_status={post_status}]"
            )
        except Exception as exc:
            print(f"[error] Failed to process post {message.id}: {exc}")
        finally:
            if post_db_id:
                await db.update_ingest_status(post_db_id, post_status)

    print("\n" + "-" * 60)
    print(f"[summary] Channel @{channel_username}")
    print(f"  Processed new/problem posts: {processed_new}")
    print(f"  Scanned posts (excluding already-done): {total_posts}")
    print(f"  Saved comments: {total_comments}")
    print(f"  Saved reactions: {total_reactions}")
    print("-" * 60)


async def parse_channel_to_postgres() -> None:
    db = DatabaseManager(DB_CONFIG)

    try:
        channels = load_channels_from_config()
        print("[info] Channels from config:")
        for ch in channels:
            print(f"  - @{ch}")

        await db.connect()
        await db.init_database()

        client = TelegramClient('session_name', API_ID, API_HASH)
        await client.start(PHONE)

        for ch in channels:
            try:
                print("\n==============================")
                print(f"[start] Parsing @{ch}")
                print("==============================")
                await parse_single_channel(db, client, ch)
            except Exception as exc:
                print(f"[error] Failed to parse @{ch}: {exc}")

        print("\n[stats] Final posts.ingest_status summary:")
        await db.print_ingest_status_stats()

        await client.disconnect()
        print("\n[ok] Parsing completed")
    except Exception as exc:
        print(f"[error] Top-level failure: {exc}")
    finally:
        await db.disconnect()


async def extract_reactions_to_dict(message, reactions_dict: defaultdict) -> int:
    total_reactions = 0
    if not message.reactions:
        return total_reactions

    try:
        if hasattr(message.reactions, 'results'):
            for reaction in message.reactions.results:
                if hasattr(reaction.reaction, 'emoticon'):
                    key = reaction.reaction.emoticon
                elif hasattr(reaction.reaction, 'document_id'):
                    key = f"custom_emoji_{reaction.reaction.document_id}"
                else:
                    key = 'unknown'

                reactions_dict[key] += reaction.count
                total_reactions += reaction.count
    except Exception as exc:
        print(f"[error] Failed to extract reactions: {exc}")
        raise

    return total_reactions


async def extract_comments_as_strings(client, channel, message) -> list[str]:
    """Return comment texts and do not fail the whole post on comment errors."""
    comments = []

    try:
        async for comment in client.iter_messages(
            channel,
            reply_to=message.id,
            limit=COMMENTS_LIMIT_PER_POST,
        ):
            comment_text = comment.text or comment.message or ''
            if comment_text.strip():
                comments.append(comment_text)
    except Exception as exc:
        print(f"[warn] Failed to fetch comments for post {message.id}: {exc}")

    return comments


if __name__ == "__main__":
    print("[start] Telegram parser with PostgreSQL storage")
    print(f"[config] POSTS_LIMIT={POSTS_LIMIT}, COMMENTS_LIMIT_PER_POST={COMMENTS_LIMIT_PER_POST}")
    asyncio.run(parse_channel_to_postgres())
