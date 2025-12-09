from __future__ import annotations

import asyncpg
from datetime import date
from typing import Optional, Dict, Any, List

from .config import DATABASE_URL

pool: Optional[asyncpg.Pool] = None


async def init_db() -> None:
    """
    Инициализация пула соединений и создание таблиц.
    """
    global pool
    if pool is not None:
        return

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")

    pool = await asyncpg.create_pool(DATABASE_URL)

    async with pool.acquire() as conn:
        # -------- community_settings --------
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS community_settings (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                topic TEXT NOT NULL DEFAULT 'fitness',
                product TEXT NOT NULL DEFAULT 'Фитнес-онлайн-программа',
                language TEXT NOT NULL DEFAULT 'ru',
                tone TEXT NOT NULL DEFAULT 'дружелюбный, без токсичности',
                use_news BOOLEAN NOT NULL DEFAULT FALSE,
                current_week INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

        # -------- schedule_settings --------
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_settings (
                id INTEGER PRIMARY KEY,
                mode TEXT NOT NULL,
                send_time TIME NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        # гарантированно добавляем last_auto_date в существующую таблицу
        await conn.execute(
            """
            ALTER TABLE schedule_settings
            ADD COLUMN IF NOT EXISTS last_auto_date DATE;
            """
        )

        # -------- qa_settings --------
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS qa_settings (
                id INTEGER PRIMARY KEY,
                enabled BOOLEAN NOT NULL,
                limit_per_day INTEGER NOT NULL,
                max_length INTEGER NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

        # -------- challenges --------
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS challenges (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                challenge_date DATE NOT NULL,
                week INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'generated', -- generated / sent
                scheduled_for TIMESTAMPTZ,
                sent_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

        # -------- challenge_answers --------
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS challenge_answers (
                id SERIAL PRIMARY KEY,
                challenge_id INTEGER REFERENCES challenges(id) ON DELETE CASCADE,
                tg_user_id BIGINT NOT NULL,
                username TEXT,
                full_name TEXT,
                answer_text TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_challenge_answers_user
                ON challenge_answers(tg_user_id, created_at DESC);
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_challenge_answers_ch
                ON challenge_answers(challenge_id, created_at DESC);
            """
        )

        # -------- ensure single rows --------
        await conn.execute(
            """
            INSERT INTO community_settings (id, name)
            VALUES (1, 'Fitness Club')
            ON CONFLICT (id) DO NOTHING;
            """
        )
        await conn.execute(
            """
            INSERT INTO schedule_settings (id, mode, send_time)
            VALUES (1, 'manual', '10:00')
            ON CONFLICT (id) DO NOTHING;
            """
        )
        await conn.execute(
            """
            INSERT INTO qa_settings (id, enabled, limit_per_day, max_length)
            VALUES (1, TRUE, 5, 500)
            ON CONFLICT (id) DO NOTHING;
            """
        )


async def close_db() -> None:
    global pool
    if pool is not None:
        await pool.close()
        pool = None


def get_pool() -> asyncpg.Pool:
    """
    Возвращаем актуальный пул.
    """
    if pool is None:
        raise RuntimeError("DB pool is not initialized. init_db() not called?")
    return pool


# ============ COMMUNITY ============


async def get_community_settings() -> Dict[str, Any]:
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, name, topic, product, language, tone, use_news, current_week
            FROM community_settings
            WHERE id = 1;
            """
        )
    return {
        "id": row["id"],
        "community_name": row["name"],
        "topic": row["topic"],
        "product": row["product"],
        "language": row["language"],
        "tone": row["tone"],
        "use_news": row["use_news"],
        "current_week": row["current_week"],
    }


async def update_current_week(week: int) -> None:
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            UPDATE community_settings
            SET current_week = $1,
                updated_at = NOW()
            WHERE id = 1;
            """,
            week,
        )


async def update_topic(topic: str) -> None:
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            UPDATE community_settings
            SET topic = $1,
                updated_at = NOW()
            WHERE id = 1;
            """,
            topic,
        )


async def update_product(product: str) -> None:
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            UPDATE community_settings
            SET product = $1,
                updated_at = NOW()
            WHERE id = 1;
            """,
            product,
        )


async def update_tone(tone: str) -> None:
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            UPDATE community_settings
            SET tone = $1,
                updated_at = NOW()
            WHERE id = 1;
            """,
            tone,
        )


# ============ USER ANSWERS / CHALLENGE_ANSWERS ============


async def save_challenge_answer(
    challenge_id: int,
    tg_user_id: int,
    username: Optional[str],
    full_name: Optional[str],
    answer_text: str,
) -> None:
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO challenge_answers (challenge_id, tg_user_id, username, full_name, answer_text)
            VALUES ($1, $2, $3, $4, $5);
            """,
            challenge_id,
            tg_user_id,
            username,
            full_name,
            answer_text,
        )


async def get_user_answers_for_user(tg_user_id: int):
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT ca.id,
                   ca.created_at,
                   ca.answer_text,
                   c.title,
                   c.challenge_date
            FROM challenge_answers ca
            JOIN challenges c ON c.id = ca.challenge_id
            WHERE ca.tg_user_id = $1
            ORDER BY ca.created_at DESC
            LIMIT 20;
            """,
            tg_user_id,
        )
    return rows


# ============ SCHEDULE SETTINGS ============


async def get_schedule_settings() -> Dict[str, Any]:
    """
    Получаем настройки расписания авто-постинга.
    """
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, mode, send_time, last_auto_date
            FROM schedule_settings
            WHERE id = 1;
            """
        )
    if not row:
        return {
            "id": 1,
            "mode": "manual",
            "send_time": None,
            "last_auto_date": None,
        }
    return dict(row)


async def set_schedule_mode(mode: str) -> None:
    """
    Обновляем режим публикации: manual / auto.
    """
    if mode not in ("manual", "auto"):
        raise ValueError("mode must be 'manual' or 'auto'")
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            UPDATE schedule_settings
            SET mode = $1,
                updated_at = NOW()
            WHERE id = 1;
            """,
            mode,
        )


async def set_schedule_last_auto_date(d: date) -> None:
    """
    Запоминаем дату последней автоматической отправки.
    """
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            UPDATE schedule_settings
            SET last_auto_date = $1,
                updated_at = NOW()
            WHERE id = 1;
            """,
            d,
        )
