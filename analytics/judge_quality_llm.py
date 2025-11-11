# analytics/judge_quality_llm.py
# Готовая LLM-модель (Qwen2.5-7B-Instruct) как "судья качества" постов.
# Требования: transformers>=4.44, accelerate, torch (CUDA), bitsandbytes (для 4/8-бит).
# Запуск: python -m analytics.judge_quality_llm

from __future__ import annotations
import os, json, time, asyncio, re
from typing import List, Dict, Any
import asyncpg
from dotenv import load_dotenv

import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, TextStreamer

# ------------ ENV / DB ------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

DB = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'engagex'),
    'user': os.getenv('POSTGRES_USER', 'engagex'),
    'password': os.getenv('POSTGRES_PASSWORD', 'engagex')
}

# ------------ MODEL ------------
# По умолчанию — Qwen2.5-7B-Instruct (мультиязычная, отлично понимает RU).
MODEL_ID = os.getenv("JUDGE_MODEL", "Qwen/Qwen2.5-7B-Instruct")
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
DTYPE = torch.bfloat16 if torch.cuda.is_available() else torch.float32
USE_8BIT = os.getenv("JUDGE_8BIT", "1") == "1"  # 8-бит квант для VRAM экономии

_tokenizer = None
_model = None

def load_model():
    global _tokenizer, _model
    if _tokenizer is not None:
        return
    kwargs = {}
    if DEVICE == "cuda":
        if USE_8BIT:
            kwargs.update(dict(load_in_8bit=True, device_map="auto"))
        else:
            kwargs.update(dict(torch_dtype=DTYPE, device_map="auto"))
    _tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, use_fast=True)
    _model = AutoModelForCausalLM.from_pretrained(MODEL_ID, **kwargs).eval()

SYSTEM_MSG = (
    "Ты — строгий, беспристрастный модератор и редактор. "
    "Оцени качество поста для онлайн-сообщества (SaaS/крипто/ИТ): ясность, полезность, вовлечение, токсичность/этика, структура.\n"
    "Верни ТОЛЬКО валидный JSON без пояснений в формате:\n"
    "{\"score\": <0..100>, \"is_good\": <true|false>, \"reasons\": [\"...\",\"...\"], \"labels\": {\"clarity\": 0..100, \"usefulness\": 0..100, \"engagement\": 0..100, \"ethics\": 0..100}}\n"
    "Если текста почти нет — score=0, is_good=false."
)

PROMPT_TEMPLATE = (
    "Оцени пост по критериям и верни JSON. Критерии:\n"
    "1) Ясность/структура (clarity)\n"
    "2) Полезность/новизна (usefulness)\n"
    "3) Потенциал вовлечения (engagement)\n"
    "4) Этичность/отсутствие токсичности/спама (ethics)\n\n"
    "Пост:\n\"\"\"\n{post}\n\"\"\"\n"
)

def build_messages(post_text: str):
    return [
        {"role": "system", "content": SYSTEM_MSG},
        {"role": "user", "content": PROMPT_TEMPLATE.format(post=post_text.strip())}
    ]

def infer_batch(texts: List[str]) -> List[Dict[str, Any]]:
    # Генерация по одному (надёжнее для парсинга); при желании можно батчить через vLLM/transformers-gen.
    results = []
    for t in texts:
        messages = build_messages(t)
        input_ids = _tokenizer.apply_chat_template(messages, add_generation_prompt=True, return_tensors="pt").to(_model.device)
        with torch.inference_mode():
            out = _model.generate(
                input_ids=input_ids,
                max_new_tokens=256,
                do_sample=False,
                temperature=0.0,
                top_p=1.0,
                pad_token_id=_tokenizer.eos_token_id
            )
        text = _tokenizer.decode(out[0][input_ids.shape[-1]:], skip_special_tokens=True)
        # Парсим JSON из ответа
        m = re.search(r'\{.*\}', text, re.S)
        if not m:
            results.append({"score": 0, "is_good": False, "reasons": ["no_json"], "labels": {"clarity":0,"usefulness":0,"engagement":0,"ethics":0}})
            continue
        try:
            js = json.loads(m.group(0))
            # Валидация полей
            score = float(js.get("score", 0))
            is_good = bool(js.get("is_good", False))
            reasons = js.get("reasons", [])
            labels = js.get("labels", {})
            results.append({
                "score": max(0.0, min(100.0, score)),
                "is_good": bool(is_good),
                "reasons": reasons[:6],
                "labels": {
                    "clarity": float(labels.get("clarity", 0)),
                    "usefulness": float(labels.get("usefulness", 0)),
                    "engagement": float(labels.get("engagement", 0)),
                    "ethics": float(labels.get("ethics", 0)),
                }
            })
        except Exception:
            results.append({"score": 0, "is_good": False, "reasons": ["bad_json"], "labels": {"clarity":0,"usefulness":0,"engagement":0,"ethics":0}})
    return results

# ------------ SQL ------------
CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS post_quality (
  post_id        INTEGER PRIMARY KEY,
  channel_username VARCHAR(255) NOT NULL,
  quality_score  DOUBLE PRECISION NOT NULL,
  is_good        BOOLEAN NOT NULL,
  signals        JSONB NOT NULL,
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

SELECT_BATCH = """
SELECT p.id AS post_id,
       p.channel_username,
       COALESCE(cp.clean_text, p.post_text) AS text
FROM posts p
LEFT JOIN clean_posts cp ON cp.source_post_id = p.id
WHERE p.id > $1
ORDER BY p.id
LIMIT $2;
"""

UPSERT = """
INSERT INTO post_quality (post_id, channel_username, quality_score, is_good, signals, updated_at)
VALUES ($1, $2, $3, $4, $5::jsonb, now())
ON CONFLICT (post_id) DO UPDATE
SET quality_score = EXCLUDED.quality_score,
    is_good       = EXCLUDED.is_good,
    signals       = EXCLUDED.signals,
    updated_at    = now();
"""

# ------------ RUN ------------
async def main():
    print("🧑‍⚖️ LLM-оценка постов (готовая модель) → post_quality")
    load_model()
    conn = await asyncpg.connect(**DB)
    try:
        await conn.execute(CREATE_TABLE)
        last_id = 0
        batch = int(os.getenv("JUDGE_BATCH", "64"))
        total = 0
        t0 = time.time()

        while True:
            rows = await conn.fetch(SELECT_BATCH, last_id, batch)
            if not rows:
                break

            texts = []
            metas = []
            for r in rows:
                txt = (r["text"] or "").strip()
                texts.append(txt if txt else " ")
                metas.append((int(r["post_id"]), r["channel_username"]))
                last_id = int(r["post_id"])

            judged = infer_batch(texts)
            upserts = []
            for (pid, ch), j in zip(metas, judged):
                signals = {
                    "judge": "llm",
                    "reasons": j["reasons"],
                    "labels": j["labels"]
                }
                upserts.append((pid, ch, float(j["score"]), bool(j["is_good"]), json.dumps(signals, ensure_ascii=False)))

            await conn.executemany(UPSERT, upserts)
            total += len(rows)
            print(f"  ✓ +{len(rows)} (итого {total})")

        dt = time.time() - t0
        print(f"✅ Готово: {total} постов оценены LLM за {dt:.1f}s. Таблица: post_quality")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
