# bot/services/challenges.py
#
# Логика работы с челленджами:
# - генерация через MODEL_SERVER_URL (LoRA writer)
# - сохранение в БД
# - выборка / статус / планирование
# - защита от лишних генераций (служебные функции — для хэндлеров)

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional

import aiohttp
import asyncpg

from ..config import MODEL_SERVER_URL
from .. import db
from ..db import get_community_settings


# ================== ВСПОМОГАТЕЛЬНЫЕ ШТУКИ ==================


WEEK_GOALS = {
    1: "Вовлечение: спровоцировать обсуждения, обмен опытом, активность в чате.",
    2: "Удержание: закрепить привычку участвовать, возвращаться в чат каждый день.",
    3: "Продажи / конверсия: мягко подвести к пробам продукта, апгрейдам, оплате.",
    4: "Реактивация: вернуть тех, кто давно молчит, дать им лёгкий повод вернуться.",
}


async def _model_generate(messages: List[Dict[str, str]]) -> str:
    """
    Вызов локального сервиса модели (Qwen + LoRA writer).
    Ожидаем JSON-ответ формата { "text": "<сгенерированный текст>" }.
    """
    if not MODEL_SERVER_URL:
        raise RuntimeError("MODEL_SERVER_URL не задан в .env")

    async with aiohttp.ClientSession() as session:
        async with session.post(
            MODEL_SERVER_URL,
            json={
                "mode": "writer",
                "messages": messages,
                "max_new_tokens": 512,
                "temperature": 0.7,
                "top_p": 0.9,
                "top_k": 50,
                "repetition_penalty": 1.05,
            },
            timeout=120,
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data["text"]


def _parse_title_body(raw: str) -> Dict[str, str]:
    """
    Парсим ответ модели вида:

    Заголовок: ...
    Текст: ...

    Если формат съехал — делаем максимально разумный разбор.
    """
    lines = [l.strip() for l in raw.splitlines() if l.strip()]
    title = ""
    body_lines: List[str] = []

    for line in lines:
        low = line.lower()
        if low.startswith("заголовок:"):
            title = line.split(":", 1)[1].strip()
        elif low.startswith("текст:"):
            # Всё, что после "Текст:" и ниже — считаем телом
            body_lines.append(line.split(":", 1)[1].strip())
        else:
            if body_lines:
                body_lines.append(line)

    if not title and lines:
        title = lines[0]
    if not body_lines and len(lines) > 1:
        body_lines = lines[1:]

    body = "\n".join(body_lines).strip()
    return {"title": title, "body": body}


async def _generate_single_challenge_for_date(
    target_date: date,
    week: int,
) -> Dict[str, Any]:
    """
    Генерируем ОДИН челлендж на конкретную дату с учётом:
    - тематики, тона, продукта (из community_settings)
    - цели недели (1–4)
    """
    settings = await get_community_settings()
    topic = settings.get("topic") or "fitness"
    tone = settings.get("tone") or "дружелюбный, мотивирующий"
    product = settings.get("product")
    language = settings.get("language") or "ru"

    week_goal = WEEK_GOALS.get(week, WEEK_GOALS[1])

    system_msg = (
        "Ты — ИИ-копирайтер, который придумывает фитнес-челленджи "
        "для онлайн-сообщества. Челленджи должны быть короткими, живыми "
        "и мотивирующими, без токсичности и морализаторства."
    )

    user_msg_lines = [
        f"Тематика сообщества: {topic}",
        f"Язык: {language}",
        f"Тон общения: {tone}",
        "",
        f"Неделя цикла: {week}",
        f"Цель недели: {week_goal}",
        f"Дата челленджа: {target_date.isoformat()}",
    ]
    if product:
        user_msg_lines.append(f"Продукт / сервис: {product}")
    user_msg_lines.extend(
        [
            "",
            "Сгенерируй один интерактивный челлендж для комьюнити, который:",
            "- побуждает пользователя что-то СДЕЛАТЬ (написать, поделиться, выполнить маленькое действие);",
            "- понятен и выполним за один день;",
            "- связан с фитнесом, здоровьем, привычками.",
            "",
            "Ответ строго в формате:",
            "Заголовок: <очень короткий, с эмодзи, без слова 'Челлендж'>",
            "Текст: <2–5 предложений, без лишнего вступления>",
        ]
    )

    raw = await _model_generate(
        [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": "\n".join(user_msg_lines)},
        ]
    )
    parsed = _parse_title_body(raw)
    return {
        "challenge_date": target_date,
        "title": parsed["title"],
        "body": parsed["body"],
    }


# ================== ГЕНЕРАЦИЯ ЧЕЛЛЕНДЖЕЙ ==================


async def generate_challenges_for_today(week: int, count: int = 3) -> List[Dict[str, Any]]:
    """
    Генерация 1–N челленджей на сегодня.
    """
    today = date.today()
    res: List[Dict[str, Any]] = []
    for _ in range(count):
        ch = await _generate_single_challenge_for_date(today, week)
        res.append(ch)
    return res


async def generate_challenges_for_week(
    week: int,
    start_date: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """
    Генерируем по одному челленджу на каждый день недели (7 дней подряд).
    По умолчанию старт — сегодня.
    """
    if start_date is None:
        start_date = date.today()

    res: List[Dict[str, Any]] = []
    for i in range(7):
        d = start_date + timedelta(days=i)
        ch = await _generate_single_challenge_for_date(d, week)
        res.append(ch)
    return res


# ================== РАБОТА С ТАБЛИЦЕЙ challenges ==================


async def save_generated_challenges(challenges: List[Dict[str, Any]], week: int) -> None:
    """
    Сохраняем сгенерированные челленджи в таблицу challenges
    со статусом 'generated'.
    """
    if db.pool is None:
        raise RuntimeError("DB pool is not initialized")

    async with db.pool.acquire() as conn:
        stmt = """
        INSERT INTO challenges (challenge_date, week, title, body, status, created_at)
        VALUES ($1, $2, $3, $4, 'generated', NOW())
        """
        for ch in challenges:
            await conn.execute(
                stmt,
                ch["challenge_date"],
                week,
                ch["title"],
                ch["body"],
            )


async def get_last_generated_challenges(limit: int = 10) -> List[asyncpg.Record]:
    """
    Последние НЕОТПРАВЛЕННЫЕ челленджи (любой статус, кроме 'sent'),
    отсортированные по дате и id.
    """
    if db.pool is None:
        raise RuntimeError("DB pool is not initialized")

    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, challenge_date, title, body, status, week, created_at
            FROM challenges
            WHERE status <> 'sent'
            ORDER BY challenge_date DESC, id DESC
            LIMIT $1
            """,
            limit,
        )
        return rows


async def get_challenge_by_id(ch_id: int) -> Optional[asyncpg.Record]:
    if db.pool is None:
        raise RuntimeError("DB pool is not initialized")

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, challenge_date, title, body, status, week, created_at, sent_at
            FROM challenges
            WHERE id = $1
            """,
            ch_id,
        )
        return row


async def update_challenge_status(ch_id: int, status: str) -> None:
    """
    Обновляем статус челленджа (generated / scheduled / disabled / sent).
    """
    if db.pool is None:
        raise RuntimeError("DB pool is not initialized")

    async with db.pool.acquire() as conn:
        await conn.execute(
            "UPDATE challenges SET status = $1 WHERE id = $2",
            status,
            ch_id,
        )


async def schedule_challenge(ch_id: int) -> None:
    """
    Делаем челлендж единственным 'scheduled' на свою дату.
    Остальные на эту же дату опускаем до 'generated'.
    """
    if db.pool is None:
        raise RuntimeError("DB pool is not initialized")

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT challenge_date FROM challenges WHERE id = $1",
            ch_id,
        )
        if not row:
            return
        ch_date: date = row["challenge_date"]

        async with conn.transaction():
            # Всё, что уже было scheduled на эту дату, переводим в generated
            await conn.execute(
                """
                UPDATE challenges
                SET status = 'generated'
                WHERE challenge_date = $1
                  AND status = 'scheduled'
                  AND id <> $2
                """,
                ch_date,
                ch_id,
            )
            # Делает выбранный челлендж scheduled
            await conn.execute(
                """
                UPDATE challenges
                SET status = 'scheduled'
                WHERE id = $1
                """,
                ch_id,
            )


async def mark_challenge_sent(ch_id: int) -> None:
    """
    Помечаем челлендж отправленным.
    ВАЖНО: мы никогда его не удаляем.
    """
    if db.pool is None:
        raise RuntimeError("DB pool is not initialized")

    async with db.pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE challenges
            SET status = 'sent',
                sent_at = NOW()
            WHERE id = $1
            """,
            ch_id,
        )


async def get_active_challenges_grouped() -> Dict[date, List[asyncpg.Record]]:
    """
    Активные челленджи (generated / scheduled), БЕЗ sent.
    Группировка по дате.
    """
    if db.pool is None:
        raise RuntimeError("DB pool is not initialized")

    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, challenge_date, title, body, status, week, created_at
            FROM challenges
            WHERE status IN ('generated', 'scheduled')
            ORDER BY challenge_date, id
            """
        )

    grouped: Dict[date, List[asyncpg.Record]] = {}
    for r in rows:
        d: date = r["challenge_date"]
        grouped.setdefault(d, []).append(r)
    return grouped


# ================== ЗАЩИТА ОТ ЛИШНИХ ГЕНЕРАЦИЙ ==================


async def count_active_challenges_between(start: date, end: date) -> int:
    """
    Считаем количество АКТИВНЫХ челленджей (generated / scheduled)
    в диапазоне дат [start; end].
    Отправленные (sent) не считаем.
    """
    if db.pool is None:
        raise RuntimeError("DB pool is not initialized")

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT COUNT(*) AS c
            FROM challenges
            WHERE challenge_date BETWEEN $1 AND $2
              AND status IN ('generated', 'scheduled')
            """,
            start,
            end,
        )
        return int(row["c"]) if row else 0


async def delete_unsent_challenges_between(start: date, end: date) -> None:
    """
    Удаляем ВСЕ НЕОТПРАВЛЕННЫЕ челленджи в диапазоне дат.
    ВАЖНО: status = 'sent' НЕ ТРОГАЕМ НИКОГДА.
    """
    if db.pool is None:
        raise RuntimeError("DB pool is not initialized")

    async with db.pool.acquire() as conn:
        await conn.execute(
            """
            DELETE FROM challenges
            WHERE challenge_date BETWEEN $1 AND $2
              AND status <> 'sent'
            """,
            start,
            end,
        )
