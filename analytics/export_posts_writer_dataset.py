# analytics/export_posts_writer_dataset.py
# Экспорт SFT-датасета для LoRA-writer из таблиц writer_samples и writer_challenges.
#
# writer_samples  — посты (sample_type='post' и т.п.)
# writer_challenges — челленджи с полем week_goal.
#
# В итоге в одном JSONL-файле будут и посты, и челленджи.

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

# -------------------------------------------------------
# База проекта и .env
# -------------------------------------------------------
BASE_DIR = str(pathlib.Path(__file__).resolve().parents[1])
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

load_dotenv(os.path.join(BASE_DIR, ".env"))

DB = {
    "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "engagex"),
    "user": os.getenv("POSTGRES_USER", "engagex"),
    "password": os.getenv("POSTGRES_PASSWORD", "engagex"),
}

# -------------------------------------------------------
# Промпты для writer-модели (обучающая разметка)
# -------------------------------------------------------

WRITER_SYSTEM_MSG = (
    "Ты — автор постов для Telegram-канала по крипте и IT. "
    "Пишешь ясно, по-деловому, без воды и кликбейта. "
    "Стиль: живой, но аккуратный, без токсичности и без фейков. "
    "Опирайся на тему и цель поста, следи за структурой и логикой."
)

# базовый шаблон: для постов будет как раньше,
# для челленджей добавим строку про Цель недели
WRITER_USER_TEMPLATE = (
    "Канал: {channel}\n"
    "{maybe_week_goal}"
    "Цель: {goal}\n\n"
    "Фактура (краткий бриф по теме):\n"
    "\"\"\"\n{brief}\n\"\"\"\n\n"
    "Напиши финальный пост для Telegram-канала."
)


def build_user_prompt(
    channel: Optional[str],
    goal: str,
    brief: str,
    week_goal: Optional[str] = None,
) -> str:
    ch = channel or "не указан"
    wg = (week_goal or "").strip()
    if wg:
        maybe_week_goal = f"Цель недели: {wg}\n"
    else:
        maybe_week_goal = ""
    return WRITER_USER_TEMPLATE.format(
        channel=ch,
        maybe_week_goal=maybe_week_goal,
        goal=(goal or "").strip(),
        brief=(brief or "").strip(),
    )


# -------------------------------------------------------
# Загрузка строк из writer_samples (посты)
# -------------------------------------------------------

async def fetch_writer_samples(
    conn: asyncpg.Connection,
    sample_type: Optional[str],
    channel: Optional[str],
    limit: Optional[int],
) -> List[asyncpg.Record]:
    """
    Читает строки из writer_samples, которые готовы для обучения:
    - final_post не пустой,
    - можно отфильтровать по sample_type ('post' / 'challenge' / и т.д.),
      по каналу и по лимиту.
    """
    where_clauses = ["final_post IS NOT NULL", "trim(final_post) <> ''"]
    params: List[Any] = []
    idx = 1

    if sample_type:
        where_clauses.append(f"sample_type = ${idx}")
        params.append(sample_type)
        idx += 1

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
            sample_type,
            source_post_id,
            channel_username,
            goal,
            topic_brief,
            final_post,
            created_at
        FROM writer_samples
        WHERE {where_sql}
        ORDER BY id
        {limit_sql};
    """

    rows = await conn.fetch(sql, *params)
    return rows


# -------------------------------------------------------
# Загрузка строк из writer_challenges (челленджи)
# -------------------------------------------------------

async def fetch_writer_challenges(
    conn: asyncpg.Connection,
    channel: Optional[str],
    limit: Optional[int],
) -> List[asyncpg.Record]:
    """
    Читает строки из writer_challenges, которые готовы для обучения:
    - final_challenge не пустой,
    - можно отфильтровать по каналу и по лимиту.
    Ожидаемые поля: id, source_post_id, channel_username,
    week_goal, goal, topic_brief, final_challenge.
    """
    where_clauses = ["final_challenge IS NOT NULL", "trim(final_challenge) <> ''"]
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
            final_challenge AS final_post
        FROM writer_challenges
        WHERE {where_sql}
        ORDER BY id
        {limit_sql};
    """

    rows = await conn.fetch(sql, *params)
    return rows



# -------------------------------------------------------
# Основная логика экспорта
# -------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(
        description="Export SFT dataset for LoRA-writer from writer_samples and writer_challenges."
    )

    parser.add_argument(
        "--out",
        type=str,
        default=os.path.join(BASE_DIR, "data", "writer_sft_dataset_posts.jsonl"),
        help="Путь к выходному JSONL (по умолчанию: ./data/writer_sft_dataset_posts.jsonl)",
    )
    parser.add_argument(
        "--sample-type",
        type=str,
        default="post",
        choices=["post", "challenge", "all"],
        help=(
            "Тип сэмплов для таблицы writer_samples: 'post', 'challenge' или 'all'. "
            "По умолчанию: post."
        ),
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
        help="Максимальное количество примеров из каждой таблицы (по умолчанию: без ограничения).",
    )
    parser.add_argument(
        "--no-challenges",
        action="store_true",
        help="Не добавлять строки из writer_challenges (по умолчанию: добавляем).",
    )

    args = parser.parse_args()

    out_path = args.out
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    if args.sample_type == "all":
        sample_type = None
    else:
        sample_type = args.sample_type

    include_challenges = not args.no_challenges

    print(f"[{datetime.now().isoformat()}] Подключаемся к БД {DB['database']}...")
    conn = await asyncpg.connect(**DB)

    try:
        # ---------- POSTS (writer_samples) ----------
        print(
            f"[{datetime.now().isoformat()}] Загружаем из writer_samples "
            f"(sample_type={sample_type}, channel={args.channel})..."
        )
        posts_rows = await fetch_writer_samples(
            conn=conn,
            sample_type=sample_type,
            channel=args.channel,
            limit=args.limit,
        )
        total_posts = len(posts_rows)
        print(
            f"[{datetime.now().isoformat()}] Найдено {total_posts} обучающих примеров в writer_samples."
        )

        # ---------- CHALLENGES (writer_challenges) ----------
        challenges_rows: List[asyncpg.Record] = []
        total_challenges = 0

        if include_challenges:
            print(
                f"[{datetime.now().isoformat()}] Загружаем из writer_challenges "
                f"(channel={args.channel})..."
            )
            challenges_rows = await fetch_writer_challenges(
                conn=conn,
                channel=args.channel,
                limit=args.limit,
            )
            total_challenges = len(challenges_rows)
            print(
                f"[{datetime.now().isoformat()}] Найдено {total_challenges} обучающих примеров в writer_challenges."
            )
        else:
            print(
                f"[{datetime.now().isoformat()}] writer_challenges пропущена (--no-challenges)."
            )

        if total_posts == 0 and total_challenges == 0:
            print("⚠️ Нет данных ни в writer_samples, ни в writer_challenges.")
            return

        total_all = total_posts + total_challenges
        written = 0
        i = 0

        with open(out_path, "w", encoding="utf-8") as f:
            # сначала посты
            for r in posts_rows:
                i += 1
                channel = r["channel_username"]
                goal = r["goal"] or ""
                brief = r["topic_brief"] or ""
                final_post = (r["final_post"] or "").strip()

                if not final_post:
                    continue

                user_prompt = build_user_prompt(
                    channel=channel,
                    goal=goal,
                    brief=brief,
                    week_goal=None,  # для постов недели нет
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

                if i % 50 == 0 or i == total_all:
                    print(
                        f"[{datetime.now().isoformat()}] Экспортировано {i}/{total_all} (записано {written})",
                        end="\r",
                        flush=True,
                    )

            # затем челленджи
            for r in challenges_rows:
                i += 1
                channel = r["channel_username"]
                goal = r["goal"] or ""
                week_goal = r["week_goal"] or ""
                brief = r["topic_brief"] or ""
                final_post = (r["final_post"] or "").strip()

                if not final_post:
                    continue

                user_prompt = build_user_prompt(
                    channel=channel,
                    goal=goal,
                    brief=brief,
                    week_goal=week_goal,  # вот тут подмешиваем неделю
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

                if i % 50 == 0 or i == total_all:
                    print(
                        f"[{datetime.now().isoformat()}] Экспортировано {i}/{total_all} (записано {written})",
                        end="\r",
                        flush=True,
                    )

        print()
        print(f"[{datetime.now().isoformat()}] ✅ Экспорт завершён. Файл: {out_path}")
        print(
            f"[{datetime.now().isoformat()}] Всего записей: {written} "
            f"(writer_samples={total_posts}, writer_challenges={total_challenges})"
        )

    finally:
        await conn.close()
        print(f"[{datetime.now().isoformat()}] Соединение с БД закрыто.")


if __name__ == "__main__":
    asyncio.run(main())
