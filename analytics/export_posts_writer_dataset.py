# analytics/export_posts_writer_dataset.py
# Экспорт SFT-датасета для LoRA-writer из таблицы writer_samples.
# v1: работает с постами (sample_type='post')
# v2: без изменений сможет работать и с челленджами (sample_type='challenge'),
#     как только ты начнёшь записывать такие строки в writer_samples.

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

WRITER_USER_TEMPLATE = (
    "Канал: {channel}\n"
    "Цель: {goal}\n\n"
    "Фактура (краткий бриф по теме):\n"
    "\"\"\"\n{brief}\n\"\"\"\n\n"
    "Напиши финальный пост для Telegram-канала."
)


def build_user_prompt(
    channel: Optional[str],
    goal: str,
    brief: str,
) -> str:
    ch = channel or "не указан"
    return WRITER_USER_TEMPLATE.format(
        channel=ch,
        goal=(goal or "").strip(),
        brief=(brief or "").strip(),
    )


# -------------------------------------------------------
# Загрузка строк из writer_samples
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
    - можно отфильтровать по sample_type ('post' / 'challenge'),
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
# Основная логика экспорта
# -------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(
        description="Export SFT dataset for LoRA-writer from writer_samples table."
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
        help="Тип сэмплов: 'post', 'challenge' или 'all' (по умолчанию: post).",
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

    if args.sample_type == "all":
        sample_type = None
    else:
        sample_type = args.sample_type

    print(f"[{datetime.now().isoformat()}] Подключаемся к БД {DB['database']}...")
    conn = await asyncpg.connect(**DB)
    try:
        print(
            f"[{datetime.now().isoformat()}] Загружаем writer_samples "
            f"(sample_type={sample_type}, channel={args.channel})..."
        )
        rows = await fetch_writer_samples(
            conn=conn,
            sample_type=sample_type,
            channel=args.channel,
            limit=args.limit,
        )
        total = len(rows)
        print(f"[{datetime.now().isoformat()}] Найдено {total} обучающих примеров.")

        if total == 0:
            print("⚠️ Нет данных в writer_samples. Сначала заполни таблицу автофилом.")
            return

        written = 0
        with open(out_path, "w", encoding="utf-8") as f:
            for i, r in enumerate(rows, start=1):
                channel = r["channel_username"]
                goal = r["goal"] or ""
                brief = r["topic_brief"] or ""
                final_post = (r["final_post"] or "").strip()

                if not final_post:
                    # На всякий случай, хотя уже фильтруем в SQL
                    continue

                user_prompt = build_user_prompt(
                    channel=channel,
                    goal=goal,
                    brief=brief,
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

                if i % 50 == 0 or i == total:
                    print(
                        f"[{datetime.now().isoformat()}] Экспортировано {i}/{total} (записано {written})",
                        end="\r",
                        flush=True,
                    )

        print()
        print(f"[{datetime.now().isoformat()}] ✅ Экспорт завершён. Файл: {out_path}")
        print(f"[{datetime.now().isoformat()}] Всего записей: {written}")

    finally:
        await conn.close()
        print(f"[{datetime.now().isoformat()}] Соединение с БД закрыто.")


if __name__ == "__main__":
    asyncio.run(main())
