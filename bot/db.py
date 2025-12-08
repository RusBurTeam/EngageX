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
        # -------- challenge_metrics -------- (ОСНОВНАЯ ТАБЛИЦА МЕТРИК)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS challenge_metrics (
                id SERIAL PRIMARY KEY,
                challenge_id INTEGER NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,

                -- Тип недели цикла (из ТЗ)
                week_type VARCHAR(20) NOT NULL DEFAULT 'engagement',
                    -- 'engagement'    (неделя 1) - вовлечение
                    -- 'retention'     (неделя 2) - удержание  
                    -- 'conversion'    (неделя 3) - конверсия/продажи
                    -- 'reactivation'  (неделя 4) - реактивация

                -- Основные метрики вовлечения
                sent_to_count INTEGER DEFAULT 0,      -- скольким пользователям отправлено
                responses_count INTEGER DEFAULT 0,     -- сколько ответов получено
                response_rate DECIMAL(5,2) DEFAULT 0.0, -- процент ответов

                -- Метрики взаимодействия
                views_count INTEGER DEFAULT 0,        -- сколько раз просмотрели (если сможем отследить)
                clicks_count INTEGER DEFAULT 0,       -- клики на кнопки (ответить/узнать больше)

                -- Временные метки
                sent_at TIMESTAMPTZ,                  -- когда отправили в канал
                first_response_at TIMESTAMPTZ,        -- когда первый ответ
                last_response_at TIMESTAMPTZ,         -- когда последний ответ

                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                UNIQUE(challenge_id)  -- один челлендж = одна запись метрик
            );
        """)

        # -------- challenge_views -------- (если нужно отслеживать просмотры)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS challenge_views (
                id SERIAL PRIMARY KEY,
                challenge_id INTEGER NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
                tg_user_id BIGINT NOT NULL,
                viewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                source VARCHAR(20) DEFAULT 'channel', -- channel/direct/link

                UNIQUE(challenge_id, tg_user_id)  -- один пользователь = один просмотр
            );
        """)

        # -------- challenge_clicks -------- (отслеживание кликов по кнопкам)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS challenge_clicks (
                id SERIAL PRIMARY KEY,
                challenge_id INTEGER NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
                tg_user_id BIGINT NOT NULL,
                button_type VARCHAR(20) NOT NULL, -- 'answer' / 'learn_more'
                clicked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

        # Индексы для быстрых запросов
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_challenge_metrics_week 
            ON challenge_metrics(week_type, sent_at DESC);
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_challenge_metrics_rate 
            ON challenge_metrics(response_rate DESC, sent_at DESC);
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_challenge_views_user 
            ON challenge_views(tg_user_id, viewed_at DESC);
        """)

        # Добавляем колонку week_type в существующую таблицу challenges (если её нет)
        await conn.execute("""
            ALTER TABLE challenges 
            ADD COLUMN IF NOT EXISTS week_type VARCHAR(20);
        """)
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


# ============ МЕТРИКИ ============

async def create_challenge_metric(
        challenge_id: int,
        week_type: str,
        sent_to_count: int = 0
) -> int:
    """
    Создать запись метрик для нового челленджа.
    """
    async with get_pool().acquire() as conn:
        metric_id = await conn.fetchval("""
            INSERT INTO challenge_metrics 
            (challenge_id, week_type, sent_to_count, sent_at)
            VALUES ($1, $2, $3, NOW())
            RETURNING id
        """, challenge_id, week_type, sent_to_count)
    return metric_id


async def update_challenge_metric_on_answer(challenge_id: int) -> None:
    """
    Обновить метрики челленджа при новом ответе.
    """
    async with get_pool().acquire() as conn:
        # Получаем текущие данные
        row = await conn.fetchrow("""
            SELECT 
                COUNT(ca.id) as new_responses_count,
                cm.sent_to_count,
                cm.responses_count as old_responses_count
            FROM challenge_metrics cm
            LEFT JOIN challenge_answers ca ON ca.challenge_id = cm.challenge_id
            WHERE cm.challenge_id = $1
            GROUP BY cm.sent_to_count, cm.responses_count
        """, challenge_id)

        if not row:
            return

        # Обновляем счётчик ответов
        await conn.execute("""
            UPDATE challenge_metrics 
            SET responses_count = $2,
                updated_at = NOW()
            WHERE challenge_id = $1
        """, challenge_id, row['new_responses_count'])

        # Обновляем response_rate если есть sent_to_count
        if row['sent_to_count'] > 0:
            response_rate = (row['new_responses_count'] * 100.0) / row['sent_to_count']
            await conn.execute("""
                UPDATE challenge_metrics 
                SET response_rate = $2,
                    updated_at = NOW()
                WHERE challenge_id = $1
            """, challenge_id, round(response_rate, 2))

        # Обновляем временные метки ответов
        if row['old_responses_count'] == 0:  # Это первый ответ
            await conn.execute("""
                UPDATE challenge_metrics 
                SET first_response_at = NOW(),
                    updated_at = NOW()
                WHERE challenge_id = $1
            """, challenge_id)

        # Всегда обновляем last_response_at
        await conn.execute("""
            UPDATE challenge_metrics 
            SET last_response_at = NOW(),
                updated_at = NOW()
            WHERE challenge_id = $1
        """, challenge_id)


async def log_challenge_click(
        challenge_id: int,
        tg_user_id: int,
        button_type: str  # 'answer' или 'learn_more'
) -> None:
    """
    Записать клик по кнопке челленджа.
    """
    async with get_pool().acquire() as conn:
        # Записываем в таблицу кликов
        await conn.execute("""
            INSERT INTO challenge_clicks 
            (challenge_id, tg_user_id, button_type)
            VALUES ($1, $2, $3)
        """, challenge_id, tg_user_id, button_type)

        # Обновляем общий счётчик в метриках
        await conn.execute("""
            UPDATE challenge_metrics 
            SET clicks_count = clicks_count + 1,
                updated_at = NOW()
            WHERE challenge_id = $1
        """, challenge_id)


async def log_challenge_view(
        challenge_id: int,
        tg_user_id: int,
        source: str = "channel"
) -> None:
    """
    Записать просмотр челленджа пользователем.
    """
    async with get_pool().acquire() as conn:
        # Используем ON CONFLICT для избежания дублей
        await conn.execute("""
            INSERT INTO challenge_views 
            (challenge_id, tg_user_id, source)
            VALUES ($1, $2, $3)
            ON CONFLICT (challenge_id, tg_user_id) DO NOTHING
        """, challenge_id, tg_user_id, source)

        # Обновляем счётчик просмотров в метриках
        await conn.execute("""
            UPDATE challenge_metrics 
            SET views_count = (
                SELECT COUNT(*) FROM challenge_views 
                WHERE challenge_id = $1
            ),
            updated_at = NOW()
            WHERE challenge_id = $1
        """, challenge_id)


async def update_sent_to_count(challenge_id: int, sent_to_count: int) -> None:
    """
    Обновить количество пользователей, которым отправили челлендж.
    """
    async with get_pool().acquire() as conn:
        await conn.execute("""
            UPDATE challenge_metrics 
            SET sent_to_count = $2,
                updated_at = NOW()
            WHERE challenge_id = $1
        """, challenge_id, sent_to_count)

        # Пересчитать response_rate
        await conn.execute("""
            UPDATE challenge_metrics 
            SET response_rate = 
                CASE 
                    WHEN $2 > 0 
                    THEN (responses_count * 100.0) / $2 
                    ELSE 0 
                END
            WHERE challenge_id = $1
        """, challenge_id, sent_to_count)


async def get_challenge_metrics(challenge_id: int):
    """
    Получить все метрики для конкретного челленджа.
    """
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow("""
            SELECT 
                cm.*,
                c.title,
                c.challenge_date,
                c.week,
                COUNT(DISTINCT cv.tg_user_id) as unique_views,
                COUNT(DISTINCT cc.tg_user_id) as unique_clicks
            FROM challenge_metrics cm
            JOIN challenges c ON c.id = cm.challenge_id
            LEFT JOIN challenge_views cv ON cv.challenge_id = cm.challenge_id
            LEFT JOIN challenge_clicks cc ON cc.challenge_id = cm.challenge_id
            WHERE cm.challenge_id = $1
            GROUP BY cm.id, c.id
        """, challenge_id)
    return row


async def get_weekly_metrics(week_type: str = None, limit: int = 10):
    """
    Получить метрики по неделям или все.
    """
    async with get_pool().acquire() as conn:
        query = """
            SELECT 
                cm.*,
                c.title,
                c.challenge_date,
                c.week,
                RANK() OVER (ORDER BY cm.response_rate DESC) as rank_by_response
            FROM challenge_metrics cm
            JOIN challenges c ON c.id = cm.challenge_id
            WHERE cm.sent_at IS NOT NULL
        """

        params = []
        if week_type:
            query += " AND cm.week_type = $1"
            params.append(week_type)

        query += " ORDER BY cm.sent_at DESC LIMIT $2"
        params.append(limit)

        rows = await conn.fetch(query, *params)
    return rows


async def get_overall_stats():
    """
    Общая статистика по всем челленджам.
    """
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_challenges,
                SUM(sent_to_count) as total_reach,
                SUM(responses_count) as total_responses,
                AVG(response_rate) as avg_response_rate,
                SUM(clicks_count) as total_clicks,

                -- По типам недель
                COUNT(CASE WHEN week_type = 'engagement' THEN 1 END) as engagement_challenges,
                COUNT(CASE WHEN week_type = 'retention' THEN 1 END) as retention_challenges,
                COUNT(CASE WHEN week_type = 'conversion' THEN 1 END) as conversion_challenges,
                COUNT(CASE WHEN week_type = 'reactivation' THEN 1 END) as reactivation_challenges
            FROM challenge_metrics
            WHERE sent_at IS NOT NULL
        """)
    return row


#Функция для преобразования номера недели в тип
def get_week_type_from_number(week_number: int) -> str:
    """
    Преобразовать номер недели цикла в тип недели.
    """
    week_type_map = {
        1: "engagement",  # Неделя 1: Вовлечение
        2: "retention",  # Неделя 2: Удержание
        3: "conversion",  # Неделя 3: Конверсия/Продажи
        4: "reactivation"  # Неделя 4: Реактивация
    }
    return week_type_map.get(week_number, "engagement")


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
