

import os
import sys
import json
import asyncio
import argparse
from datetime import datetime
from typing import Any, Dict, List, Optional

import asyncpg
from dotenv import load_dotenv
import pathlib

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

load_dotenv(os.path.join(BASE_DIR, ".env"))

DB = {
    "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "engagex"),
    "user": os.getenv("POSTGRES_USER", "engagex"),
    "password": os.getenv("POSTGRES_PASSWORD", "engagex"),
}

WRITER_SYSTEM_MSG = (
    "Ты — автор челленджей и коротких постов для Telegram-канала "
    "про фитнес и здоровый образ жизни. "
    "Пишешь ясно, по-деловому, без воды и кликбейта. "
    "Стиль можешь адаптировать под задачу (дружелюбный, спокойный, мотивационный и т.п.), "
    "но всегда остаёшься уважительным и поддерживающим. "
    "Опирайся на цель челленджа, фактуру и указанную цель недели, "
    "следи за структурой и логикой, не усложняй формулировки."
)

WRITER_USER_TEMPLATE = (
    "Канал: {channel}\n"
    "{maybe_week_goal}"
    "{maybe_style}"
    "Цель челленджа: {goal}\n\n"
    "Фактура (краткий бриф по теме челленджа):\n"
    "\"\"\"\n{brief}\n\"\"\"\n\n"
    "Напиши финальный текст челленджа для Telegram-канала."
)

def build_user_prompt(
    channel: Optional[str],
    goal: str,
    brief: str,
    week_goal: Optional[str] = None,
    style: Optional[str] = None,
) -> str:
    """Build user prompt."""
    ch = channel or "не указан"

    wg = (week_goal or "").strip()
    if wg:
        maybe_week_goal = f"Цель недели: {wg}\n"
    else:
        maybe_week_goal = ""

    st = (style or "").strip()
    if st:
        maybe_style = f"Стиль: {st}\n"
    else:
        maybe_style = ""

    return WRITER_USER_TEMPLATE.format(
        channel=ch,
        maybe_week_goal=maybe_week_goal,
        maybe_style=maybe_style,
        goal=(goal or "").strip(),
        brief=(brief or "").strip(),
    )

async def fetch_writer_challenges(
    conn: asyncpg.Connection,
    channel: Optional[str],
    limit: Optional[int],
) -> List[asyncpg.Record]:
    """Fetch writer challenges."""
    where_clauses = [
        "final_challenge IS NOT NULL",
        "trim(final_challenge) <> ''",
        "gen_status = 'ok'",
    ]
    params: List[Any] = []
    idx = 1

    if channel:
        where_clauses.append(f"channel_username = ${idx}")
        params.append(channel)
        idx += 1

    where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"

    limit_sql = ""
    if limit is not None and limit > 0:
        limit_sql = f"LIMIT {int(limit)}"

    sql = f"""
        SELECT
            id,
            source_post_id,
            channel_username,
            week_goal,
            goal,
            topic_brief,
            style,
            final_challenge AS final_post
        FROM writer_challenges
        WHERE {where_sql}
        ORDER BY id
        {limit_sql};
    """

    rows = await conn.fetch(sql, *params)
    return rows

async def count_writer_challenges_candidates(
    conn: asyncpg.Connection,
    channel: Optional[str],
) -> int:
    """Count writer challenges candidates."""
    where_clauses = ["final_challenge IS NOT NULL", "trim(final_challenge) <> ''"]
    params: List[Any] = []
    idx = 1

    if channel:
        where_clauses.append(f"channel_username = ${idx}")
        params.append(channel)
        idx += 1

    where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"

    sql = f"""
        SELECT COUNT(*)
        FROM writer_challenges
        WHERE {where_sql};
    """

    cnt = await conn.fetchval(sql, *params)
    return int(cnt or 0)

async def main():
    parser = argparse.ArgumentParser(
        description="Export SFT dataset for LoRA-writer ONLY from writer_challenges (fitness)."
    )

    parser.add_argument(
        "--out",
        type=str,

        default=os.path.join(BASE_DIR, "data", "writer_train.jsonl"),
        help="Путь к выходному JSONL (по умолчанию: ./data/writer_train.jsonl)",
    )
    parser.add_argument(
        "--channel",
        type=str,
        default=None,
        help="Фильтр по channel_username (по умолчанию: без фильтра).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Максимальное количество примеров (по умолчанию: без ограничения).",
    )

    args = parser.parse_args()

    out_path = args.out
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    print(f"[{datetime.now().isoformat()}] Подключаемся к БД {DB['database']}...")
    conn = await asyncpg.connect(**DB)

    try:

        print(
            f"[{datetime.now().isoformat()}] Загружаем из writer_challenges "
            f"(channel={args.channel})..."
        )

        total_challenges_candidates = await count_writer_challenges_candidates(
            conn=conn,
            channel=args.channel,
        )

        challenges_rows = await fetch_writer_challenges(
            conn=conn,
            channel=args.channel,
            limit=args.limit,
        )
        total_challenges = len(challenges_rows)
        skipped_challenges_by_status = max(
            total_challenges_candidates - total_challenges, 0
        )

        print(
            f"[{datetime.now().isoformat()}] Найдено {total_challenges} обучающих примеров в writer_challenges "
            f"(кандидатов по тексту: {total_challenges_candidates}, "
            f"отсеяно по статусу gen_status != 'ok': {skipped_challenges_by_status})."
        )

        if total_challenges == 0:
            print("⚠️ Нет данных в writer_challenges (после фильтрации).")
            return

        written = 0
        i = 0

        with open(out_path, "w", encoding="utf-8") as f:
            for r in challenges_rows:
                i += 1
                channel = r["channel_username"]
                goal = r["goal"] or ""
                week_goal = r["week_goal"] or ""
                brief = r["topic_brief"] or ""
                style = r.get("style") or ""
                final_post = (r["final_post"] or "").strip()

                if not final_post:
                    continue

                user_prompt = build_user_prompt(
                    channel=channel,
                    goal=goal,
                    brief=brief,
                    week_goal=week_goal,
                    style=style,
                )

                sample = {
                    "messages": [
                        {
                            "role": "system",
                            "content": WRITER_SYSTEM_MSG,
                        },
                        {
                            "role": "user",
                            "content": user_prompt,
                        },
                        {
                            "role": "assistant",
                            "content": final_post,
                        },
                    ]
                }

                f.write(json.dumps(sample, ensure_ascii=False) + "\n")
                written += 1

                if i % 50 == 0 or i == total_challenges:
                    print(
                        f"[{datetime.now().isoformat()}] Экспортировано {i}/{total_challenges} (записано {written})",
                        end="\r",
                        flush=True,
                    )

        print()
        print(f"[{datetime.now().isoformat()}] ✅ Экспорт завершён. Файл: {out_path}")
        print(
            f"[{datetime.now().isoformat()}] Всего записей: {written} "
            f"(writer_challenges={total_challenges})"
        )
        print(
            f"[{datetime.now().isoformat()}] Отброшено по статусу gen_status != 'ok': "
            f"writer_challenges={skipped_challenges_by_status}"
        )

    finally:
        await conn.close()
        print(f"[{datetime.now().isoformat()}] Соединение с БД закрыто.")

if __name__ == "__main__":
    asyncio.run(main())
