from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import aiohttp
import asyncpg

from .. import db
from ..config import MODEL_SERVER_URL
from ..db import get_community_settings

WEEK_GOALS = {
    1: "Engagement: spark discussion and active chat participation.",
    2: "Retention: reinforce daily participation habits.",
    3: "Conversion: guide users toward trying or purchasing the product.",
    4: "Reactivation: bring back inactive community members.",
}


async def _model_generate(messages: List[Dict[str, str]]) -> str:
    """Send generation request to the model server."""
    if not MODEL_SERVER_URL:
        raise RuntimeError("MODEL_SERVER_URL is not set in .env")

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
    """Parse model output into title/body structure."""
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    title = ""
    body_lines: List[str] = []

    for line in lines:
        low = line.lower()
        if low.startswith("title:"):
            title = line.split(":", 1)[1].strip()
        elif low.startswith("text:"):
            body_lines.append(line.split(":", 1)[1].strip())
        elif body_lines:
            body_lines.append(line)

    if not title and lines:
        title = lines[0]
    if not body_lines and len(lines) > 1:
        body_lines = lines[1:]

    return {"title": title or "Challenge", "body": "\n".join(body_lines).strip() or raw.strip()}


async def _generate_single_challenge_for_date(target_date: date, week: int) -> Dict[str, Any]:
    """Generate one challenge for a date and cycle week."""
    settings = await get_community_settings()
    topic = settings.get("topic") or "fitness"
    tone = settings.get("tone") or "friendly, motivating"
    product = settings.get("product")
    language = settings.get("language") or "en"

    week_goal = WEEK_GOALS.get(week, WEEK_GOALS[1])

    system_msg = (
        "You are an AI copywriter for a fitness community. "
        "Create short, practical, motivating challenges without toxic tone or moralizing."
    )

    user_msg_lines = [
        f"Community topic: {topic}",
        f"Language: {language}",
        f"Tone: {tone}",
        "",
        f"Cycle week: {week}",
        f"Weekly goal: {week_goal}",
        f"Challenge date: {target_date.isoformat()}",
    ]
    if product:
        user_msg_lines.append(f"Product/service: {product}")

    user_msg_lines.extend(
        [
            "",
            "Generate one interactive community challenge that:",
            "- asks the user to perform a concrete action;",
            "- can be completed in one day;",
            "- stays relevant to fitness and healthy habits.",
            "",
            "Return strictly in format:",
            "Title: <very short title>",
            "Text: <2-5 concise sentences>",
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


async def generate_challenges_for_today(week: int, count: int = 3) -> List[Dict[str, Any]]:
    """Generate multiple challenges for today."""
    today = date.today()
    result: List[Dict[str, Any]] = []
    for _ in range(count):
        result.append(await _generate_single_challenge_for_date(today, week))
    return result


async def generate_challenges_for_week(week: int, start_date: Optional[date] = None) -> List[Dict[str, Any]]:
    """Generate seven challenges starting from start_date."""
    if start_date is None:
        start_date = date.today()

    result: List[Dict[str, Any]] = []
    for i in range(7):
        target = start_date + timedelta(days=i)
        result.append(await _generate_single_challenge_for_date(target, week))
    return result


async def save_generated_challenges(challenges: List[Dict[str, Any]], week: int) -> None:
    """Persist generated challenges into DB."""
    if db.pool is None:
        raise RuntimeError("DB pool is not initialized")

    async with db.pool.acquire() as conn:
        stmt = """
        INSERT INTO challenges (challenge_date, week, title, body, status, created_at)
        VALUES ($1, $2, $3, $4, 'generated', NOW())
        """
        for ch in challenges:
            await conn.execute(stmt, ch["challenge_date"], week, ch["title"], ch["body"])


async def get_last_generated_challenges(limit: int = 10) -> List[asyncpg.Record]:
    """Return latest unsent challenges."""
    if db.pool is None:
        raise RuntimeError("DB pool is not initialized")

    async with db.pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT id, challenge_date, title, body, status, week, created_at
            FROM challenges
            WHERE status <> 'sent'
            ORDER BY challenge_date DESC, id DESC
            LIMIT $1
            """,
            limit,
        )


async def get_challenge_by_id(ch_id: int) -> Optional[asyncpg.Record]:
    if db.pool is None:
        raise RuntimeError("DB pool is not initialized")

    async with db.pool.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT id, challenge_date, title, body, status, week, created_at, sent_at
            FROM challenges
            WHERE id = $1
            """,
            ch_id,
        )


async def update_challenge_status(ch_id: int, status: str) -> None:
    """Update challenge status."""
    if db.pool is None:
        raise RuntimeError("DB pool is not initialized")

    async with db.pool.acquire() as conn:
        await conn.execute("UPDATE challenges SET status = $1 WHERE id = $2", status, ch_id)


async def schedule_challenge(ch_id: int) -> None:
    """Set one challenge as scheduled and unschedule peers on same date."""
    if db.pool is None:
        raise RuntimeError("DB pool is not initialized")

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT challenge_date FROM challenges WHERE id = $1", ch_id)
        if not row:
            return
        challenge_date: date = row["challenge_date"]

        async with conn.transaction():
            await conn.execute(
                """
                UPDATE challenges
                SET status = 'generated'
                WHERE challenge_date = $1
                  AND status = 'scheduled'
                  AND id <> $2
                """,
                challenge_date,
                ch_id,
            )

            await conn.execute(
                """
                UPDATE challenges
                SET status = 'scheduled'
                WHERE id = $1
                """,
                ch_id,
            )


async def mark_challenge_sent(ch_id: int) -> None:
    """Mark challenge as sent."""
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
    """Return generated/scheduled challenges grouped by date."""
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
    for row in rows:
        grouped.setdefault(row["challenge_date"], []).append(row)
    return grouped


async def count_active_challenges_between(start: date, end: date) -> int:
    """Count generated/scheduled challenges in date range."""
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
    """Delete non-sent challenges in date range."""
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
