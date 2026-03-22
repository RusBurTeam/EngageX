from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import asyncpg

from ..db import get_pool, get_community_settings
from .llm import call_model


def _parse_title_body(raw: str) -> Dict[str, str]:
    """Parse model output into title/body fields."""
    text = (raw or "").strip()
    if not text:
        return {
            "title": "Challenge",
            "body": "Challenge description could not be parsed.",
        }

    lines = text.splitlines()
    title = ""
    body_lines: List[str] = []

    for line in lines:
        low = line.lower().strip()
        if low.startswith("title:"):
            title = line.split(":", 1)[1].strip()
        elif low.startswith("text:"):
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
    return {"title": title or "Challenge", "body": body or text}


async def _generate_single(
    target_date: date,
    week: int,
    topic: str,
    product: str,
    tone: str,
    community_name: str,
) -> Dict[str, Any]:
    system_msg = (
        "You are an AI community moderator for an online fitness group. "
        "Generate daily challenges that increase engagement. "
        "Write in English."
    )

    user_lines = [
        f"Community: {community_name}",
        f"Topic: {topic}",
        f"Cycle week: {week}",
        f"Product/service: {product}",
        f"Tone: {tone}",
        "",
        f"Generate one challenge for date: {target_date.isoformat()}",
        "",
        "Requirements:",
        "- Encourage the participant to do a concrete action;",
        "- Must be doable within one day;",
        "- Keep a supportive non-toxic tone.",
        "",
        "Output format:",
        "Title: <short title>",
        "Text: <3-7 concise sentences>",
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
    """Generate a sequence of challenges for a date range."""
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


async def save_generated(challenges: List[Dict[str, Any]], *, week: int) -> List[int]:
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
        await conn.execute("DELETE FROM challenges WHERE id = $1;", ch_id)


async def generate_more_about_challenge_text(ch) -> str:
    title = ch["title"]
    body = ch["body"]
    ch_date = ch["challenge_date"]

    system_msg = (
        "You are a supportive fitness coach. "
        "Explain the challenge in a simple and practical way."
    )

    user_lines = [
        f"Challenge date: {ch_date.isoformat()}",
        f"Title: {title}",
        "",
        "Challenge text:",
        body,
        "",
        "Provide:",
        "- who this challenge is for;",
        "- expected benefit;",
        "- 3-5 clear action steps for today.",
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
        "You are a supportive fitness coach. "
        "Answer challenge-related questions briefly and clearly in 3-7 sentences."
    )

    title = ch.get("title") or ""
    body = ch.get("body") or ""
    ch_date = ch.get("challenge_date")

    user_lines = [
        "Challenge:",
        f"Date: {ch_date.isoformat() if hasattr(ch_date, 'isoformat') else ch_date}",
        f"Title: {title}",
        f"Text: {body}",
        "",
        "Participant question:",
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
    """Update challenge title and body."""
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
    """Regenerate one challenge while keeping its date/week."""
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
    """Return first unsent challenge for a specific date."""
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
    """Return aggregate challenge analytics."""
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
    """Update challenge date."""
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
    """Update challenge cycle week."""
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
