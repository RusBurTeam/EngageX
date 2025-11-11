import asyncpg
import asyncio
import os
import pandas as pd
from dotenv import load_dotenv

# Базовая директория проекта
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")

load_dotenv(dotenv_path=ENV_PATH)

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "engagex"),
    "user": os.getenv("POSTGRES_USER", "engagex"),
    "password": os.getenv("POSTGRES_PASSWORD", "engagex"),
}


async def build_dataset():
    print("📦 Сбор обучающего датасета из БД...")

    conn = await asyncpg.connect(**DB_CONFIG)

    rows = await conn.fetch(
        """
        SELECT 
            p.id              AS post_id,
            p.channel_username,
            c.clean_text,
            m.engagement_score
        FROM posts p
        JOIN clean_posts   c ON c.source_post_id = p.id
        JOIN post_metrics  m ON m.post_id       = p.id
        WHERE c.clean_text IS NOT NULL
          AND m.engagement_score IS NOT NULL
        """
    )

    await conn.close()

    if not rows:
        print("⚠️ Нет данных для датасета. Проверь парсер и аналитику.")
        return

    data = []
    for r in rows:
        data.append(
            {
                "post_id": r["post_id"],
                "channel": r["channel_username"],
                "text": r["clean_text"],
                "engagement_score": float(r["engagement_score"]),
                # базовая метка — дальше можно делить на цели недели
                "label": "engagement",
            }
        )

    df = pd.DataFrame(data)
    df.sort_values("engagement_score", ascending=False, inplace=True)

    out_dir = os.path.join(BASE_DIR, "analytics", "dataset")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "challenges_dataset.csv")
    df.to_csv(out_path, index=False, encoding="utf-8")

    print(f"✅ Датасет собран: {out_path}")
    print(f"🔢 Записей: {len(df)}")


if __name__ == "__main__":
    asyncio.run(build_dataset())
