
from __future__ import annotations

from datetime import date, timedelta
from typing import List, Dict, Any, Optional

import asyncpg

from ..db import get_pool, get_community_settings
from .llm import call_model


def _parse_title_body(raw: str) -> Dict[str, str]:
    """
    Парсим ответ модели в заголовок/тело.

    Поддерживает:
    1) "Заголовок: ...\nТекст: ...\n..."
    2) "# Заголовок\n\nТекст..."
    3) первая строка — заголовок, остальное — текст
    """
    text = (raw or "").strip()
    if not text:
        return {
            "title": "Челлендж",
            "body": "Описание челленджа не удалось распарсить.",
        }

    lines = text.splitlines()

    title = ""
    body_lines: List[str] = []

    for line in lines:
        low = line.lower().strip()
        if low.startswith("заголовок:"):
            title = line.split(":", 1)[1].strip()
        elif low.startswith("текст:"):
            # всё, что после "Текст:" и дальше, считаем телом
            part = line.split(":", 1)[1].strip()
            if part:
                body_lines.append(part)
        else:
            if body_lines:
                body_lines.append(line)

    if not title and lines:
        title = lines[0].lstrip("#").strip()
    if not body_lines and len(lines) > 1:
        body_lines = lines[1:]

    body = "\n".join(body_lines).strip()
    return {"title": title, "body": body}


async def _generate_single(
    target_date: date,
    week: int,
    topic: str,
    product: str,
    tone: str,
    community_name: str,
) -> Dict[str, Any]:
    system_msg = (
        "Ты — ИИ-модератор онлайн-фитнес-сообщества. "
        "Генерируешь ежедневные челленджи (вызовы) для повышения вовлечённости. "
        "Пиши по-русски."
    )

    user_lines = [
        f"Сообщество: {community_name}",
        f"Тематика: {topic}",
        f"Неделя цикла: {week}",
        f"Продукт/сервис: {product}",
        f"Тональность: {tone}",
        "",
        f"Нужно сгенерировать челлендж на дату: {target_date.isoformat()}",
        "",
        "Требования:",
        "- мотивирует участника сделать конкретное действие;",
        "- выполним за один день;",
        "- без токсичности, поддерживающе.",
        "",
        "Формат ответа:",
        "Заголовок: <короткий заголовок>",
        "Текст: <3–7 предложений с объяснением челленджа>",
    ]

    raw = await call_model(
        [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": "\n".join(user_lines)},
        ]
    )

    parsed = _parse_title_body(raw)
    return {
        "title": parsed["title"],
        "body": parsed["body"],
        "challenge_date": target_date,
    }


async def generate_range(
    *,
    start_date: date,
    days: int,
    week: int,
    topic: str,
    product: str,
    tone: str,
    community_name: str,
) -> List[Dict[str, Any]]:
    """
    Генерация диапазона челленджей от start_date включительно на days дней.
    """
    result: List[Dict[str, Any]] = []
    for i in range(days):
        d = start_date + timedelta(days=i)
        one = await _generate_single(
            target_date=d,
            week=week,
            topic=topic,
            product=product,
            tone=tone,
            community_name=community_name,
        )
        result.append(one)
    return result


# =================== операции с БД ===================

async def save_generated(
    challenges: List[Dict[str, Any]],
    *,
    week: int,
) -> List[int]:
    pool = get_pool()
    ids: List[int] = []

    async with pool.acquire() as conn:
        stmt = """
            INSERT INTO challenges (title, body, challenge_date, week, status)
            VALUES ($1, $2, $3, $4, 'generated')
            RETURNING id
        """
        for ch in challenges:
            new_id = await conn.fetchval(
                stmt,
                ch["title"],
                ch["body"],
                ch["challenge_date"],
                week,
            )
            ids.append(int(new_id))
    return ids


async def list_challenges(limit: int = 50) -> List[asyncpg.Record]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, title, body, challenge_date, week, status, created_at, sent_at
            FROM challenges
            ORDER BY challenge_date DESC, id DESC
            LIMIT $1;
            """,
            limit,
        )
    return rows


async def get_challenge_by_id(ch_id: int) -> Optional[asyncpg.Record]:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, title, body, challenge_date, week, status, created_at, sent_at
            FROM challenges
            WHERE id = $1;
            """,
            ch_id,
        )
    return row


async def mark_challenge_sent(ch_id: int) -> None:
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE challenges
            SET status = 'sent',
                sent_at = NOW(),
                updated_at = NOW()
            WHERE id = $1;
            """,
            ch_id,
        )


async def delete_challenge(ch_id: int) -> None:
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM challenges WHERE id = $1;",
            ch_id,
        )


# =================== Q&A по челленджу ===================

async def generate_more_about_challenge_text(ch) -> str:
    title = ch["title"]
    body = ch["body"]
    ch_date = ch["challenge_date"]

    system_msg = (
        "Ты — фитнес-коуч и психолог. "
        "Объясни челлендж простым языком, поддерживающе, без токсичности."
    )

    user_lines = [
        f"Дата челленджа: {ch_date.isoformat()}",
        f"Заголовок: {title}",
        "",
        "Текст челленджа:",
        body,
        "",
        "Нужно написать пояснение:",
        "- кому он подойдёт;",
        "- какую пользу даёт;",
        "- 3–5 конкретных шагов на сегодня.",
    ]

    raw = await call_model(
        [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": "\n".join(user_lines)},
        ]
    )
    return raw.strip()


async def generate_challenge_qa_answer(ch: dict, question: str) -> str:
    system_msg = (
        "Ты — поддерживающий фитнес-коуч. "
        "Отвечай на вопросы по челленджу коротко, по делу и дружелюбно. "
        "Говори на «ты», 3–7 предложений."
    )

    title = ch.get("title") or ""
    body = ch.get("body") or ""
    ch_date = ch.get("challenge_date")

    user_lines = [
        "Челлендж:",
        f"Дата: {ch_date.isoformat() if hasattr(ch_date, 'isoformat') else ch_date}",
        f"Заголовок: {title}",
        f"Текст: {body}",
        "",
        "Вопрос участника:",
        question,
    ]

    raw = await call_model(
        [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": "\n".join(user_lines)},
        ]
    )
    return raw.strip()


async def update_challenge_text(ch_id: int, title: str, body: str) -> None:
    """
    Ручное обновление заголовка и текста челленджа.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE challenges
            SET title = $2,
                body = $3,
                updated_at = NOW()
            WHERE id = $1;
            """,
            ch_id,
            title,
            body,
        )


async def regenerate_challenge(ch_id: int) -> Dict[str, Any]:
    """
    Перегенерация челленджа через модель с учётом текущих настроек сообщества.

    1. Берём дату и week из существующей записи.
    2. Подтягиваем community_settings (topic, product, tone, community_name).
    3. Генерируем новый текст.
    4. Обновляем запись в БД.
    5. Возвращаем словарь с новыми полями.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, challenge_date, week
            FROM challenges
            WHERE id = $1;
            """,
            ch_id,
        )

    if not row:
        raise ValueError(f"Challenge id={ch_id} not found")

    settings = await get_community_settings()

    new_ch = await _generate_single(
        target_date=row["challenge_date"],
        week=row["week"],
        topic=settings["topic"],
        product=settings["product"],
        tone=settings["tone"],
        community_name=settings["community_name"],
    )

    await update_challenge_text(ch_id, new_ch["title"], new_ch["body"])

    return {
        "id": ch_id,
        "challenge_date": row["challenge_date"],
        "week": row["week"],
        "title": new_ch["title"],
        "body": new_ch["body"],
    }


async def get_challenge_for_date(ch_date: date) -> Optional[asyncpg.Record]:
    """
    Берём первый ещё не отправленный челлендж на конкретную дату.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, title, body, challenge_date, week, status, created_at, sent_at
            FROM challenges
            WHERE challenge_date = $1
              AND status <> 'sent'
            ORDER BY id ASC
            LIMIT 1;
            """,
            ch_date,
        )
    return row


async def get_analytics(limit: int = 10) -> List[asyncpg.Record]:
    """
    Простая аналитика: последние отправленные челленджи + количество ответов.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT c.id,
                   c.challenge_date,
                   c.week,
                   c.title,
                   c.sent_at,
                   COUNT(a.id) AS answers_count
            FROM challenges c
            LEFT JOIN challenge_answers a ON a.challenge_id = c.id
            WHERE c.status = 'sent'
            GROUP BY c.id, c.challenge_date, c.week, c.title, c.sent_at
            ORDER BY c.sent_at DESC NULLS LAST
            LIMIT $1;
            """,
            limit,
        )
    return rows
async def update_challenge_date(ch_id: int, new_date: date) -> None:
    """
    Обновить дату публикации челленджа.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE challenges
            SET challenge_date = $1,
                updated_at = NOW()
            WHERE id = $2;
            """,
            new_date,
            ch_id,
        )


async def update_challenge_week(ch_id: int, new_week: int) -> None:
    """
    Обновить номер недели цикла для челленджа.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE challenges
            SET week = $1,
                updated_at = NOW()
            WHERE id = $2;
            """,
            new_week,
            ch_id,
        )
