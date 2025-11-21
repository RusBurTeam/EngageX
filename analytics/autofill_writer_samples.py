# analytics/autofill_writer_samples.py
#
# Автоматически заполняет writer_samples из хороших постов:
# 1) Берёт посты с высоким quality_score из post_quality
# 2) Просит локальную Qwen выделить goal / topic_brief / final_post
# 3) Сохраняет в writer_samples
#
# Запуск:
#   python -m analytics.autofill_writer_samples

from __future__ import annotations
import os
import sys
import json
import re
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any, List

import asyncpg
import torch
from dotenv import load_dotenv
import pathlib

# === Базовая настройка проекта ===
BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

load_dotenv(BASE_DIR / ".env")

# Локальный загрузчик модели (как в judge_quality_llm)
from Models.qwen_loader import load_tokenizer_model

DB = {
    "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "engagex"),
    "user": os.getenv("POSTGRES_USER", "engagex"),
    "password": os.getenv("POSTGRES_PASSWORD", "engagex"),
}

# Порог качества поста, выше которого считаем его годным для датасета
MIN_QUALITY_SCORE = float(os.getenv("WRITER_MIN_SCORE", "70"))

# Ограничение на количество постов за один прогон
MAX_POSTS = int(os.getenv("WRITER_MAX_POSTS", "100"))

# === Промпт под разметку ===

SYSTEM_MSG = (
    "Ты — методист и редактор, который готовит обучающие пары для модели-писателя.\n"
    "На вход тебе даётся реальный пост из Telegram-канала про TON / крипту.\n\n"
    "Твоя задача — аккуратно разобрать этот пост и вернуть ОДИН JSON:\n"
    "{\n"
    "  \"goal\": <строка>,\n"
    "  \"topic_brief\": <строка>,\n"
    "  \"final_post\": <строка>\n"
    "}\n\n"
    "Требования:\n"
    "1) goal — одна короткая фраза, описывающая цель поста (что он должен сделать для читателя). "
    "БЕЗ слов \"Цель поста\" и без форматирования.\n"
    "2) topic_brief — 3–7 лаконичных пунктов, каждый с новой строки и с началом через дефис или тире.\n"
    "   Здесь кратко раскрывается тема: что за событие / продукт / обновление, какие ключевые аспекты.\n"
    "   НЕ копируй дословно финальный текст поста и не пиши сюда сам пост.\n"
    "3) final_post — отредактированный финальный текст поста для Telegram-канала.\n"
    "   Пиши в живом, деловом стиле, без воды и кликбейта.\n"
    "   Не используй слова \"Цель\", \"Кратко\" и т.п. — это просто нормальный пост.\n"
    "   Можно перефразировать и структурировать исходник, но смысл должен сохраниться.\n\n"
    "ВЕРНИ ТОЛЬКО ОДИН ВАЛИДНЫЙ JSON. НИЧЕГО БОЛЬШЕ."
)

USER_TEMPLATE = (
    "Канал: {channel}\n\n"
    "Оригинальный пост:\n\"\"\"\n{post}\n\"\"\"\n\n"
    "Сформируй JSON с полями goal, topic_brief, final_post в соответствии с инструкцией выше."
)

# === Вспомогательные функции для работы с моделью ===

_tokenizer = None
_model = None


def ensure_model():
    """
    Лениво загружаем токенайзер и модель один раз на процесс.
    """
    global _tokenizer, _model
    if _tokenizer is not None and _model is not None:
        return

    print(f"[{datetime.now().isoformat()}] Загрузка модели для разметки writer_samples...")
    _tokenizer, _model = load_tokenizer_model()

    try:
        device = _model.device
    except Exception:
        params = list(_model.parameters())
        device = params[0].device if params else torch.device("cpu")

    print(f"[{datetime.now().isoformat()}] Модель загружена на {device}")


def extract_json(text: str) -> Optional[Dict[str, Any]]:
    """
    Более устойчивый JSON-экстрактор:
    - чистим код-блоки ```...```
    - убираем управляющие символы
    - сканируем строку по всем '{'
    - на каждом положении пытаемся сделать raw_decode
    - как только удалось — возвращаем объект
    Если ничего не вышло — None.
    """
    if not text:
        return None

    # убираем код-блоки, чтобы не ломать парсер
    text = re.sub(r"```.*?```", " ", text, flags=re.S)
    # убираем управляющие символы
    text = re.sub(r"[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\x9f]", "", text)
    text = text.strip()

    decoder = json.JSONDecoder()

    for m in re.finditer(r"\{", text):
        start = m.start()
        try:
            obj, _ = decoder.raw_decode(text[start:])
            if isinstance(obj, dict):
                return obj
        except Exception:
            continue

    return None


def build_messages(channel: str, post_text: str) -> List[Dict[str, str]]:
    return [
        {"role": "system", "content": SYSTEM_MSG},
        {
            "role": "user",
            "content": USER_TEMPLATE.format(channel=channel, post=post_text[:16000]),
        },
    ]


def run_inference(channel: str, post_text: str) -> Optional[Dict[str, Any]]:
    """
    Прогон одного поста через Qwen, попытка вытащить JSON.
    """
    ensure_model()

    messages = build_messages(channel, post_text)

    # Qwen chat template
    try:
        inputs = _tokenizer.apply_chat_template(
            messages,
            add_generation_prompt=True,
            return_tensors="pt",
        )
    except TypeError:
        inputs = _tokenizer.apply_chat_template(
            messages,
            return_tensors="pt",
        )

    if isinstance(inputs, torch.Tensor):
        input_ids = inputs
        attention_mask = None
    elif isinstance(inputs, dict):
        input_ids = inputs.get("input_ids")
        attention_mask = inputs.get("attention_mask")
    else:
        input_ids = inputs
        attention_mask = None

    try:
        device = _model.device
    except Exception:
        params = list(_model.parameters())
        device = params[0].device if params else torch.device("cpu")

    input_ids = input_ids.to(device)
    if attention_mask is not None:
        attention_mask = attention_mask.to(device)

    gen_kwargs = dict(
        input_ids=input_ids,
        max_new_tokens=512,
        do_sample=False,
        pad_token_id=getattr(_tokenizer, "eos_token_id", None),
        eos_token_id=getattr(_tokenizer, "eos_token_id", None),
    )
    if attention_mask is not None:
        gen_kwargs["attention_mask"] = attention_mask

    with torch.inference_mode():
        out = _model.generate(**gen_kwargs)

    gen_ids = out[0][input_ids.shape[-1]:]
    gen_text = _tokenizer.decode(gen_ids, skip_special_tokens=True)

    js = extract_json(gen_text)
    return js


# === Работа с БД ===

CREATE_WRITER_SAMPLES_SQL = """
CREATE TABLE IF NOT EXISTS writer_samples (
    id SERIAL PRIMARY KEY,
    sample_type VARCHAR(50) NOT NULL DEFAULT 'post',
    source_post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    channel_username VARCHAR(255) NOT NULL,
    goal TEXT NOT NULL,
    topic_brief TEXT NOT NULL,
    final_post TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (sample_type, source_post_id)
);
"""

SELECT_CANDIDATES_SQL = """
SELECT
    p.id AS post_id,
    p.channel_username,
    COALESCE(cp.clean_text, p.post_text) AS text,
    pq.quality_score,
    p.ingest_status
FROM posts p
JOIN post_quality pq
    ON pq.post_id = p.id
LEFT JOIN clean_posts cp
    ON cp.source_post_id = p.id
WHERE
    pq.is_good = true
    AND pq.quality_score >= $1
    AND p.ingest_status = 'done'
    AND COALESCE(cp.clean_text, p.post_text) IS NOT NULL
    AND TRIM(COALESCE(cp.clean_text, p.post_text)) <> ''
    AND NOT EXISTS (
        SELECT 1 FROM writer_samples ws WHERE ws.source_post_id = p.id
            AND ws.sample_type = 'post'
    )
ORDER BY pq.quality_score DESC, p.id
LIMIT $2;
"""

INSERT_WRITER_SAMPLE_SQL = """
INSERT INTO writer_samples (
    sample_type,
    source_post_id,
    channel_username,
    goal,
    topic_brief,
    final_post
) VALUES ($1, $2, $3, $4, $5, $6);
"""


async def ensure_writer_samples_table(conn: asyncpg.Connection) -> None:
    """
    Гарантируем, что writer_samples существует.
    Это даёт автономность: можно запускать скрипт даже
    если парсер ещё не создаёт эту таблицу.
    """
    await conn.execute(CREATE_WRITER_SAMPLES_SQL)


async def fetch_candidates(conn) -> List[asyncpg.Record]:
    rows = await conn.fetch(SELECT_CANDIDATES_SQL, MIN_QUALITY_SCORE, MAX_POSTS)
    print(f"[{datetime.now().isoformat()}] Найдено кандидатов для разметки: {len(rows)}")
    return rows


async def save_writer_sample(
    conn,
    post_id: int,
    channel: str,
    goal: str,
    topic_brief: str,
    final_post: str,
):
    await conn.execute(
        INSERT_WRITER_SAMPLE_SQL,
        "post",              # sample_type — явно помечаем, что это пост
        post_id,
        channel,
        goal.strip(),
        topic_brief.strip(),
        final_post.strip(),
    )


# === Основной цикл ===

async def main():
    print(f"[{datetime.now().isoformat()}] 🚀 Авторазметка writer_samples стартует...")
    conn = await asyncpg.connect(**DB)
    try:
        # На всякий случай создаём writer_samples, если её ещё нет
        await ensure_writer_samples_table(conn)

        rows = await fetch_candidates(conn)
        if not rows:
            print(f"[{datetime.now().isoformat()}] Нет подходящих постов — выходим.")
            return

        processed = 0
        skipped = 0

        for r in rows:
            post_id = r["post_id"]
            channel = r["channel_username"]
            text = (r["text"] or "").strip()
            ingest_status = r["ingest_status"]

            # Дополнительный safety-check, если вдруг SQL поменяют
            if ingest_status != "done":
                print(
                    f"[{datetime.now().isoformat()}] ⚠️ post_id={post_id} с ingest_status={ingest_status}, пропускаем."
                )
                skipped += 1
                continue

            if not text:
                skipped += 1
                continue

            print(f"[{datetime.now().isoformat()}] → Обработка post_id={post_id} ({channel}), ingest_status={ingest_status}")

            js = run_inference(channel, text)
            if not js:
                print(f"[{datetime.now().isoformat()}] ⚠️ Не удалось вытащить JSON для post_id={post_id}, пропускаем.")
                skipped += 1
                continue

            goal = str(js.get("goal", "") or "").strip()
            topic_brief = str(js.get("topic_brief", "") or "").strip()
            final_post = str(js.get("final_post", "") or "").strip()

            if not goal or not topic_brief or not final_post:
                print(f"[{datetime.now().isoformat()}] ⚠️ Пустые поля в JSON для post_id={post_id}, пропускаем.")
                skipped += 1
                continue

            await save_writer_sample(conn, post_id, channel, goal, topic_brief, final_post)
            processed += 1
            print(f"[{datetime.now().isoformat()}] ✅ post_id={post_id} → записан в writer_samples")

        print(f"[{datetime.now().isoformat()}] Готово. Успешно: {processed}, пропущено: {skipped}")

    finally:
        await conn.close()
        print(f"[{datetime.now().isoformat()}] 🔌 Соединение с БД закрыто.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Остановлено пользователем.")
