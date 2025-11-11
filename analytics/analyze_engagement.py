import asyncpg
import asyncio
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime

# Загружаем .env
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, '.env')
load_dotenv(dotenv_path=ENV_PATH)

# Конфигурация PostgreSQL
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'engagex'),
    'user': os.getenv('POSTGRES_USER', 'engagex'),
    'password': os.getenv('POSTGRES_PASSWORD', 'engagex')
}


async def analyze_engagement():
    """Аналитика вовлечённости постов"""
    print("📊 Запуск анализа вовлечённости...")

    conn = await asyncpg.connect(**DB_CONFIG)

    # Создаём таблицу метрик, если её нет
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS post_metrics (
            id SERIAL PRIMARY KEY,
            post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
            channel_username VARCHAR(255) NOT NULL,
            engagement_score DOUBLE PRECISION DEFAULT 0,
            comments_count INTEGER DEFAULT 0,
            reactions_count INTEGER DEFAULT 0,
            views INTEGER DEFAULT 0,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(post_id)
        )
    ''')

    # Получаем все посты
    posts = await conn.fetch('SELECT id, channel_username, views FROM posts')

    metrics = []
    for post in posts:
        pid = post['id']
        channel = post['channel_username']
        views = post['views'] or 0

        # Считаем комментарии
        comments_count = await conn.fetchval(
            'SELECT COUNT(*) FROM comments WHERE post_id = $1', pid
        ) or 0

        # Считаем реакции
        reactions_count = await conn.fetchval(
            'SELECT COALESCE(SUM(reaction_count), 0) FROM reactions WHERE post_id = $1', pid
        ) or 0

        # Считаем общий балл вовлечённости
        engagement_score = views * 0.01 + reactions_count + comments_count

        metrics.append({
            'post_id': pid,
            'channel_username': channel,
            'views': views,
            'comments_count': comments_count,
            'reactions_count': reactions_count,
            'engagement_score': engagement_score
        })

    # Сохраняем в таблицу
    for m in metrics:
        await conn.execute('''
            INSERT INTO post_metrics (post_id, channel_username, views, comments_count, reactions_count, engagement_score)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (post_id)
            DO UPDATE SET
                views = EXCLUDED.views,
                comments_count = EXCLUDED.comments_count,
                reactions_count = EXCLUDED.reactions_count,
                engagement_score = EXCLUDED.engagement_score,
                calculated_at = CURRENT_TIMESTAMP
        ''', m['post_id'], m['channel_username'], m['views'],
             m['comments_count'], m['reactions_count'], m['engagement_score'])

    # Сохраняем CSV
    df = pd.DataFrame(metrics)
    df.sort_values('engagement_score', ascending=False, inplace=True)
    output_dir = os.path.join(BASE_DIR, 'analytics', 'output')
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, f"top_posts_{datetime.now().strftime('%Y%m%d_%H%M')}.csv")
    df.to_csv(csv_path, index=False, encoding='utf-8')

    # Выводим топ-10
    print("\n🔥 ТОП-10 ПОСТОВ ПО ВОВЛЕЧЁННОСТИ:")
    for i, row in enumerate(df.head(10).itertuples(), start=1):
        print(f"{i:02d}. Пост {row.post_id} | Views={row.views} | "
              f"Comments={row.comments_count} | Reactions={row.reactions_count} | Score={row.engagement_score:.2f}")

    print(f"\n💾 Результаты сохранены в таблицу post_metrics и в CSV:\n{csv_path}")

    await conn.close()
    print("✅ Аналитика завершена.")


if __name__ == "__main__":
    asyncio.run(analyze_engagement())
