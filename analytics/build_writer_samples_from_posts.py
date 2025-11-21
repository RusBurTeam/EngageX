# analytics/build_writer_samples_from_posts.py
#
# Наполняет таблицу writer_samples из таблиц posts + clean_posts + post_quality.
# Берём только посты, у которых есть оценка качества и is_good = true,
# опционально фильтруем по min_score.
#
# ВАЖНО: сейчас brief и final_text одинаковые (текст поста).
# Перед реальным обучением лучше пройтись по final_text и
# переписать его в более вылизанный вариант, чтобы модель не училась
# просто копировать исходник.

import os
import sys
import asyncio
from datetime import datetime
from typing import Any, List, Optional

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
# SQL-запросы
# -------------------------------------------------------

# Вытащить хорошие посты, которые ещё не лежат в writer_samples
FETCH_GOOD_POSTS_SQL = """
SELECT
    p.id               AS post_id,
    p.channel_username AS channel_username,
    COALESCE(cp.clean_text, p.post_text) AS text,
    pq.quality_score   AS quality_score,
    pq.is_good         AS is_good
FROM posts p
JOIN post_quality pq ON pq.post_id = p.id
LEFT JOIN clean_posts cp ON cp.source_post_id = p.id
LEFT JOIN writer_samples ws ON ws.source_post_id = p.id
WHERE
    pq.is_good = TRUE
    AND COALESCE(cp.clean_text, p.post_text) IS NOT NULL
    AND trim(COALESCE(cp.clean_text, p.post_text)) <> ''
    AND ws.id IS NULL
    {score_filter}
ORDER BY pq.quality_score DESC, p.id
LIMIT $1;
"""

INSERT_WRITER_SAMPLE_SQL = """
INSERT INTO writer_samples (
    source_type,
    source_post_id,
    source_challenge_id,
    channel_username,
    goal,
    brief,
    final_text
) VALUES (
    'post',
    $1,   -- source_post_id
    NULL, -- source_challenge_id
    $2,   -- channel_username
    $3,   -- goal
    $4,   -- brief
    $5    -- final_text
)
ON CONFLICT DO NOTHING;
"""


async def fetch_good_posts(
    conn: asyncpg.Connection,
    min_score: Optional[float],
    limit: int,
):
    score_filter = ""
    params: List[Any] = []

    if min_score is not None:
        score_filter = "AND pq.quality_score >= $2"
    else:
        score_filter = ""

    # Строим SQL с нужным фильтром
    sql = FETCH_GOOD_POSTS_SQL.format(score_filter=score_filter)

    if min_score is not None:
        rows = await conn.fetch(sql, limit, min_score)
    else:
        rows = await conn.fetch(sql, limit)

    return rows


async def build_writer_samples(min_score: Optional[float], limit: int):
    print(f"[{datetime.now().isoformat()}] Подключаемся к БД {DB['database']}...")
    conn = await asyncpg.connect(**DB)
    try:
        print(f"[{datetime.now().isoformat()}] Ищем хорошие посты для добавления в writer_samples...")
        rows = await fetch_good_posts(conn, min_score=min_score, limit=limit)
        total = len(rows)
        print(f"[{datetime.now().isoformat()}] Найдено {total} постов-кандидатов.")

        if total == 0:
            print("⚠️ Нет подходящих постов (проверь post_quality или условия фильтра).")
            return

        inserted = 0

        for r in rows:
            post_id = r["post_id"]
            channel = r["channel_username"]
            text = (r["text"] or "").strip()
            qscore = float(r["quality_score"] or 0)

            if not text:
                continue

            # Пока ставим простую цель. При желании можно потом руками отредактировать.
            goal = f"Объяснить новость / обновление для подписчиков канала {channel or ''}".strip()

            brief = text
            final_text = text  # ВАЖНО: на первом этапе просто дублируем.

            await conn.execute(
                INSERT_WRITER_SAMPLE_SQL,
                post_id,
                channel,
                goal,
                brief,
                final_text,
            )
            inserted += 1

            if inserted % 20 == 0 or inserted == total:
                print(
                    f"[{datetime.now().isoformat()}] Добавлено в writer_samples: {inserted}/{total}",
                    end="\r",
                    flush=True,
                )

        print()
        print(f"[{datetime.now().isoformat()}] ✅ Готово. Добавлено строк в writer_samples: {inserted}")

    finally:
        await conn.close()
        print(f"[{datetime.now().isoformat()}] Соединение с БД закрыто.")


# -------------------------------------------------------
# CLI
# -------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fill writer_samples from good posts (posts + post_quality)."
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=70.0,
        help="Минимальный quality_score для отбора постов (по умолчанию: 70.0).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Максимум постов, которые добавить за раз (по умолчанию: 200).",
    )

    args = parser.parse_args()

    asyncio.run(build_writer_samples(min_score=args.min_score, limit=args.limit))
