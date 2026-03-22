

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

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

load_dotenv(BASE_DIR / ".env")

from Models.qwen_loader import load_tokenizer_model

DB = {
    "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "engagex"),
    "user": os.getenv("POSTGRES_USER", "engagex"),
    "password": os.getenv("POSTGRES_PASSWORD", "engagex"),
}

MIN_QUALITY_SCORE = float(os.getenv("WRITER_MIN_SCORE", "70"))

MAX_POSTS = int(os.getenv("WRITER_MAX_POSTS", "10000000"))

SYSTEM_MSG = (
    "Ты — методист и редактор, который готовит обучающие пары для модели-писателя.\n"
    "На вход тебе даётся реальный пост из Telegram-канала про TON / крипту.\n\n"
    "Твоя задача — аккуратно разобрать этот пост и вернуть ОДИН JSON-объект:\n"
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
    "   Объём final_post — не более 1000–1200 символов, не простыня текста.\n"
    "   Можно перефразировать и структурировать исходник, но смысл должен сохраниться.\n\n"
    "Формат ответа:\n"
    "- Строго один валидный JSON-объект.\n"
    "- БЕЗ пояснений до или после JSON.\n"
    "- БЕЗ обёртки ```json``` или любых других код-блоков.\n"
    "- БЕЗ комментариев и текста вне фигурных скобок.\n\n"
    "Пример допустимого ответа:\n"
    "{ \"goal\": \"Объяснить обновление документации TON\", "
    "\"topic_brief\": \"- Обновление документации\\n- Новые гайды\", "
    "\"final_post\": \"Текст поста...\" }\n"
    "ВЕРНИ ТОЛЬКО ОДИН ВАЛИДНЫЙ JSON. НИЧЕГО БОЛЬШЕ."
)

USER_TEMPLATE = (
    "Канал: {channel}\n\n"
    "Оригинальный пост:\n\"\"\"\n{post}\n\"\"\"\n\n"
    "Строго следуй инструкции из system-сообщения и верни ТОЛЬКО один JSON-объект "
    "с полями goal, topic_brief, final_post. Никакого текста до или после JSON."
)

_tokenizer: Any = None
_model: Any = None

def ensure_model():
    """Ensure model is available."""
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

def _cut_first_json_block(text: str) -> str:
    """Cut first json block."""
    start = text.find("{")
    if start == -1:
        return text

    depth = 0
    end = None
    for i, ch in enumerate(text[start:], start=start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break

    if end is not None:
        return text[start:end]
    return text[start:]

def _json_unescape_soft(s: str) -> str:
    """Json unescape soft."""
    try:
        wrapped = '"' + s.replace('\\', '\\\\').replace('"', '\\"') + '"'
        return json.loads(wrapped)
    except Exception:
        return s

def _cut_final_post_tail(raw_tail: str) -> str:
    """Cut final post tail."""
    stoppers = [
        "\nuser\n",
        "\nuser",
        "\n```",
        "```",
        "对不起",
        "注册登录",
        "知悉您的要求",
        "幸好，您提供的翻译已经很接近了",
    ]

    end = len(raw_tail)
    for stop in stoppers:
        idx = raw_tail.find(stop)
        if idx != -1 and idx < end:
            end = idx

    s = raw_tail[:end].rstrip()

    while s and s[-1] in '" ,}':
        s = s[:-1]

    return s.strip()

def extract_json(text: str) -> Optional[Dict[str, Any]]:
    """Extract json."""
    if not text:
        return None

    text = re.sub(r"```.*?```", " ", text, flags=re.S)
    text = re.sub(r"[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\x9f]", "", text)
    text = text.strip()

    text = _cut_first_json_block(text)

    decoder = json.JSONDecoder()

    for m in re.finditer(r"\{", text):
        start = m.start()
        try:
            obj, _ = decoder.raw_decode(text[start:])
            if isinstance(obj, dict):
                goal = _json_unescape_soft(str(obj.get("goal", "") or "")).strip()
                topic_brief = _json_unescape_soft(str(obj.get("topic_brief", "") or "")).strip()
                final_post = _json_unescape_soft(str(obj.get("final_post", "") or "")).strip()

                if not goal and not topic_brief and not final_post:
                    break

                return {
                    "goal": goal,
                    "topic_brief": topic_brief,
                    "final_post": final_post,
                }
        except Exception:
            continue

    goal_match = re.search(r'"goal"\s*:\s*"(.*?)"', text, flags=re.S)
    brief_match = re.search(r'"topic_brief"\s*:\s*"(.*?)"', text, flags=re.S)
    final_match = re.search(r'"final_post"\s*:\s*"(.*)', text, flags=re.S)

    if not (goal_match and brief_match and final_match):
        return None

    goal_raw = goal_match.group(1)
    brief_raw = brief_match.group(1)
    final_tail_raw = final_match.group(1)

    goal = _json_unescape_soft(goal_raw).strip()
    topic_brief = _json_unescape_soft(brief_raw).strip()
    final_post = _cut_final_post_tail(final_tail_raw)

    if not goal and not topic_brief and not final_post:
        return None

    return {
        "goal": goal,
        "topic_brief": topic_brief,
        "final_post": final_post,
    }

def build_messages(channel: str, post_text: str) -> List[Dict[str, str]]:
    return [
        {"role": "system", "content": SYSTEM_MSG},
        {
            "role": "user",
            "content": USER_TEMPLATE.format(channel=channel, post=post_text[:16000]),
        },
    ]

def run_inference(channel: str, post_text: str) -> Optional[Dict[str, Any]]:
    """Run inference."""
    ensure_model()

    messages = build_messages(channel, post_text)

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

    if attention_mask is None:
        attention_mask = torch.ones_like(input_ids, dtype=torch.long, device=device)
    else:
        attention_mask = attention_mask.to(device)

    gen_kwargs = dict(
        input_ids=input_ids,
        attention_mask=attention_mask,
        max_new_tokens=768,
        do_sample=False,
        pad_token_id=getattr(_tokenizer, "eos_token_id", None),
        eos_token_id=getattr(_tokenizer, "eos_token_id", None),
    )

    with torch.inference_mode():
        out = _model.generate(**gen_kwargs)

    gen_ids = out[0][input_ids.shape[-1]:]
    gen_text = _tokenizer.decode(gen_ids, skip_special_tokens=True)

    js = extract_json(gen_text)
    if js is None:
        print(
            f"[{datetime.now().isoformat()}] ⚠️ Не удалось вытащить JSON для канала {channel}."
        )
        print("===== RAW gen_text (полный) =====")
        print(gen_text)
        print("========== END RAW gen_text ==========\n")
    return js

def add_emojis(channel: str, text: str) -> str:
    """Add emojis."""
    if not text:
        return text

    text = re.sub(r"\s+", " ", text)

    replacements = {
        "Crypto Pay": "Crypto Pay 💳",
        "CryptoBot": "CryptoBot 🤖",
        "TON ": "TON 💎 ",
        " TON": " TON 💎",
        "TON Blockchain": "TON Blockchain 🔵",
        "TON Network": "TON Network 🔵",
        "Telegram": "Telegram ✈️",
        "USDT": "USDT 💵",
        "TON💎": "TON 💎",
        "BTC": "BTC ₿",
        "ETH": "ETH ♦️",
        "SOL": "SOL 🟡",
        "LTC": "LTC 🌕",
        "TRX": "TRX 🔺",
        "криптовалют": "криптовалют 🪙",
        "криптовалюта": "криптовалюта 🪙",
        "криптой": "криптой 🪙",
        "крипта": "крипта 🪙",
        "кошелёк": "кошелёк 👛",
        "кошелек": "кошелек 👛",
        "wallet": "wallet 👛",
        "счета": "счета 🧾",
        "счет": "счёт 🧾",
        "счёт": "счёт 🧾",
        "invoice": "invoice 🧾",
        "createInvoice": "createInvoice 🧾",
        "оплачивать": "оплачивать 💸",
        "оплата": "оплата 💸",
        "платеж": "платёж 💸",
        "платёж": "платёж 💸",
        "вывода баланса": "вывода баланса 🔄",
        "баланс": "баланс 📊",
        "автоматической конвертацией": "автоматической конвертацией 🔁",
        "конвертацией": "конвертацией 🔁",
        "конвертации": "конвертации 🔁",
        "обмен": "обмен ♻️",
        "swap": "swap ♻️",
        "swap_to": "swap_to ♻️",
        "обновленную документацию": "обновлённую документацию 📘",
        "обновленная документация": "обновлённую документацию 📘",
        "обновленную доку": "обновлённую доку 📘",
        "обновление": "обновление 🚀",
        "новые функции": "новые функции ✨",
        "новая функция": "новая функция ✨",
        "новый релиз": "новый релиз ✨",
        "документация": "документация 📘",
        "гайд": "гайд 📘",
        "руководство": "руководство 📘",
        "разработчики": "разработчики 👨‍💻",
        "разработчик": "разработчик 👨‍💻",
        "ботах": "ботах 🤖",
        "боты": "боты 🤖",
        "Mini App": "Mini App 📱",
        "мини-приложени": "мини-приложени📱",
        "например,": "например, 👉",
        "для этого": "для этого 📌",
        "Помимо этого": "Помимо этого ➕",
        "Кроме того": "Кроме того ➕",
        "можете указать": "можете указать ✍️",
        "можно изучить": "можно изучить 🔍",
        "можно изучать": "можно изучать 🔍",
    }

    for src, dst in replacements.items():
        text = text.replace(src, dst)

    return text

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
    gen_status VARCHAR(32) NOT NULL DEFAULT 'ok',
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
        SELECT 1 FROM writer_samples ws
        WHERE ws.source_post_id = p.id
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
    final_post,
    gen_status
) VALUES ($1, $2, $3, $4, $5, $6, $7);
"""

async def ensure_writer_samples_table(conn: asyncpg.Connection) -> None:
    """Ensure writer samples table is available."""
    await conn.execute(CREATE_WRITER_SAMPLES_SQL)

    await conn.execute(
        "ALTER TABLE writer_samples "
        "ADD COLUMN IF NOT EXISTS gen_status VARCHAR(32) NOT NULL DEFAULT 'ok';"
    )

async def fetch_candidates(conn) -> List[asyncpg.Record]:
    rows = await conn.fetch(SELECT_CANDIDATES_SQL, MIN_QUALITY_SCORE, MAX_POSTS)
    print(f"[{datetime.now().isoformat()}] Найдено кандидатов для разметки: {len(rows)}")
    return rows

async def save_writer_sample(
    conn: asyncpg.Connection,
    post_id: int,
    channel: str,
    goal: str,
    topic_brief: str,
    final_post: str,
    gen_status: str = "ok",
) -> None:
    """Save writer sample."""
    await conn.execute(
        INSERT_WRITER_SAMPLE_SQL,
        "post",
        post_id,
        channel,
        goal.strip(),
        topic_brief.strip(),
        final_post.strip(),
        gen_status,
    )

def print_progress(current: int, total: int) -> None:
    """Print progress."""
    if total <= 0:
        return

    if current != total and current % 50 != 0:
        return

    ratio = current / total
    bar_len = 30
    filled = int(bar_len * ratio)
    bar = "█" * filled + "░" * (bar_len - filled)

    print(
        f"[{datetime.now().isoformat()}] Прогресс разметки: "
        f"|{bar}| {ratio * 100:5.1f}% ({current}/{total})",
        flush=True,
    )

async def main():
    print(f"[{datetime.now().isoformat()}] 🚀 Авторазметка writer_samples стартует...")
    conn = await asyncpg.connect(**DB)
    try:
        await ensure_writer_samples_table(conn)

        rows = await fetch_candidates(conn)
        total = len(rows)
        if not rows:
            print(f"[{datetime.now().isoformat()}] Нет подходящих постов — выходим.")
            return

        processed_ok = 0
        processed_error = 0
        seen = 0

        for r in rows:
            seen += 1
            post_id = r["post_id"]
            channel = r["channel_username"]
            text = (r["text"] or "").strip()
            ingest_status = r["ingest_status"]

            if ingest_status != "done":

                print(
                    f"[{datetime.now().isoformat()}] ⚠️ post_id={post_id} с ingest_status={ingest_status}, пропускаем без записи."
                )
                print_progress(seen, total)
                continue

            if not text:
                print(
                    f"[{datetime.now().isoformat()}] ⚠️ Пустой текст для post_id={post_id}, пропускаем без записи."
                )
                print_progress(seen, total)
                continue

            print(
                f"[{datetime.now().isoformat()}] → Обработка post_id={post_id} ({channel}), ingest_status={ingest_status}"
            )

            js = run_inference(channel, text)
            if not js:

                await save_writer_sample(
                    conn,
                    post_id,
                    channel,
                    goal="[error]",
                    topic_brief="[error]",
                    final_post="[error]",
                    gen_status="error",
                )
                processed_error += 1
                print(
                    f"[{datetime.now().isoformat()}] ⚠️ Не удалось вытащить JSON для post_id={post_id}, пометили gen_status='error'."
                )
                print_progress(seen, total)
                continue

            goal = str(js.get("goal", "") or "").strip()
            topic_brief = str(js.get("topic_brief", "") or "").strip()
            final_post = str(js.get("final_post", "") or "").strip()

            if not goal or not topic_brief or not final_post:
                await save_writer_sample(
                    conn,
                    post_id,
                    channel,
                    goal or "[error]",
                    topic_brief or "[error]",
                    final_post or "[error]",
                    gen_status="error",
                )
                processed_error += 1
                print(
                    f"[{datetime.now().isoformat()}] ⚠️ Пустые поля в JSON для post_id={post_id}, пометили gen_status='error'."
                )
                print_progress(seen, total)
                continue

            final_post = add_emojis(channel, final_post)

            await save_writer_sample(
                conn,
                post_id,
                channel,
                goal,
                topic_brief,
                final_post,
                gen_status="ok",
            )
            processed_ok += 1
            print(
                f"[{datetime.now().isoformat()}] ✅ post_id={post_id} → записан в writer_samples (gen_status='ok')"
            )

            print_progress(seen, total)

        print()

        print(
            f"[{datetime.now().isoformat()}] Готово. Успешно (ok): {processed_ok}, с ошибкой (error): {processed_error}"
        )

    finally:
        await conn.close()
        print(f"[{datetime.now().isoformat()}] 🔌 Соединение с БД закрыто.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Остановлено пользователем.")
