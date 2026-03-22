from __future__ import annotations
import asyncpg
from datetime import date
from typing import Optional, Dict, Any, List
from .config import DATABASE_URL
pool: Optional[asyncpg.Pool] = None

async def init_db() -> None:
    """Init db."""
    global pool
    if pool is not None:
        return
    if not DATABASE_URL:
        raise RuntimeError('DATABASE_URL is not set')
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS community_settings ( id INTEGER PRIMARY KEY, name TEXT NOT NULL, topic TEXT NOT NULL DEFAULT 'fitness', product TEXT NOT NULL DEFAULT 'Fitness Online Program', language TEXT NOT NULL DEFAULT 'ru', tone TEXT NOT NULL DEFAULT 'friendly, supportive, non-toxic', use_news BOOLEAN NOT NULL DEFAULT FALSE, current_week INTEGER NOT NULL DEFAULT 1, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW() );")
        await conn.execute('\n            CREATE TABLE IF NOT EXISTS schedule_settings (\n                id INTEGER PRIMARY KEY,\n                mode TEXT NOT NULL,\n                send_time TIME NOT NULL,\n                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n            );\n            ')
        await conn.execute('\n            ALTER TABLE schedule_settings\n            ADD COLUMN IF NOT EXISTS last_auto_date DATE;\n            ')
        await conn.execute('\n            CREATE TABLE IF NOT EXISTS qa_settings (\n                id INTEGER PRIMARY KEY,\n                enabled BOOLEAN NOT NULL,\n                limit_per_day INTEGER NOT NULL,\n                max_length INTEGER NOT NULL,\n                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n            );\n            ')
        await conn.execute("\n            CREATE TABLE IF NOT EXISTS challenges (\n                id SERIAL PRIMARY KEY,\n                title TEXT NOT NULL,\n                body TEXT NOT NULL,\n                challenge_date DATE NOT NULL,\n                week INTEGER NOT NULL,\n                status TEXT NOT NULL DEFAULT 'generated', -- generated / sent\n                scheduled_for TIMESTAMPTZ,\n                sent_at TIMESTAMPTZ,\n                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n            );\n            ")
        await conn.execute('\n            CREATE TABLE IF NOT EXISTS challenge_answers (\n                id SERIAL PRIMARY KEY,\n                challenge_id INTEGER REFERENCES challenges(id) ON DELETE CASCADE,\n                tg_user_id BIGINT NOT NULL,\n                username TEXT,\n                full_name TEXT,\n                answer_text TEXT NOT NULL,\n                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n            );\n            ')
        await conn.execute('\n            CREATE INDEX IF NOT EXISTS idx_challenge_answers_user\n                ON challenge_answers(tg_user_id, created_at DESC);\n            ')
        await conn.execute('\n            CREATE INDEX IF NOT EXISTS idx_challenge_answers_ch\n                ON challenge_answers(challenge_id, created_at DESC);\n            ')
        await conn.execute("\n            INSERT INTO community_settings (id, name)\n            VALUES (1, 'Fitness Club')\n            ON CONFLICT (id) DO NOTHING;\n            ")
        await conn.execute("\n            INSERT INTO schedule_settings (id, mode, send_time)\n            VALUES (1, 'manual', '10:00')\n            ON CONFLICT (id) DO NOTHING;\n            ")
        await conn.execute('\n            INSERT INTO qa_settings (id, enabled, limit_per_day, max_length)\n            VALUES (1, TRUE, 5, 500)\n            ON CONFLICT (id) DO NOTHING;\n            ')

async def close_db() -> None:
    global pool
    if pool is not None:
        await pool.close()
        pool = None

def get_pool() -> asyncpg.Pool:
    """Return pool."""
    if pool is None:
        raise RuntimeError('DB pool is not initialized. init_db() not called?')
    return pool

async def get_community_settings() -> Dict[str, Any]:
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow('\n            SELECT id, name, topic, product, language, tone, use_news, current_week\n            FROM community_settings\n            WHERE id = 1;\n            ')
    return {'id': row['id'], 'community_name': row['name'], 'topic': row['topic'], 'product': row['product'], 'language': row['language'], 'tone': row['tone'], 'use_news': row['use_news'], 'current_week': row['current_week']}

async def update_current_week(week: int) -> None:
    async with get_pool().acquire() as conn:
        await conn.execute('\n            UPDATE community_settings\n            SET current_week = $1,\n                updated_at = NOW()\n            WHERE id = 1;\n            ', week)

async def update_topic(topic: str) -> None:
    async with get_pool().acquire() as conn:
        await conn.execute('\n            UPDATE community_settings\n            SET topic = $1,\n                updated_at = NOW()\n            WHERE id = 1;\n            ', topic)

async def update_product(product: str) -> None:
    async with get_pool().acquire() as conn:
        await conn.execute('\n            UPDATE community_settings\n            SET product = $1,\n                updated_at = NOW()\n            WHERE id = 1;\n            ', product)

async def update_tone(tone: str) -> None:
    async with get_pool().acquire() as conn:
        await conn.execute('\n            UPDATE community_settings\n            SET tone = $1,\n                updated_at = NOW()\n            WHERE id = 1;\n            ', tone)

async def save_challenge_answer(challenge_id: int, tg_user_id: int, username: Optional[str], full_name: Optional[str], answer_text: str) -> None:
    async with get_pool().acquire() as conn:
        await conn.execute('\n            INSERT INTO challenge_answers (challenge_id, tg_user_id, username, full_name, answer_text)\n            VALUES ($1, $2, $3, $4, $5);\n            ', challenge_id, tg_user_id, username, full_name, answer_text)

async def get_user_answers_for_user(tg_user_id: int):
    async with get_pool().acquire() as conn:
        rows = await conn.fetch('\n            SELECT ca.id,\n                   ca.created_at,\n                   ca.answer_text,\n                   c.title,\n                   c.challenge_date\n            FROM challenge_answers ca\n            JOIN challenges c ON c.id = ca.challenge_id\n            WHERE ca.tg_user_id = $1\n            ORDER BY ca.created_at DESC\n            LIMIT 20;\n            ', tg_user_id)
    return rows

async def get_schedule_settings() -> Dict[str, Any]:
    """Return schedule settings."""
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow('\n            SELECT id, mode, send_time, last_auto_date\n            FROM schedule_settings\n            WHERE id = 1;\n            ')
    if not row:
        return {'id': 1, 'mode': 'manual', 'send_time': None, 'last_auto_date': None}
    return dict(row)

async def set_schedule_mode(mode: str) -> None:
    """Set schedule mode."""
    if mode not in ('manual', 'auto'):
        raise ValueError("mode must be 'manual' or 'auto'")
    async with get_pool().acquire() as conn:
        await conn.execute('\n            UPDATE schedule_settings\n            SET mode = $1,\n                updated_at = NOW()\n            WHERE id = 1;\n            ', mode)

async def set_schedule_last_auto_date(d: date) -> None:
    """Set schedule last auto date."""
    async with get_pool().acquire() as conn:
        await conn.execute('\n            UPDATE schedule_settings\n            SET last_auto_date = $1,\n                updated_at = NOW()\n            WHERE id = 1;\n            ', d)
