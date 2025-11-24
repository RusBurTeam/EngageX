# analytics/autofill_week1_engagement.py
#
# Специальный автофилл для 1-й недели (Вовлечение):
# 1) Берёт до 500 случайных хороших постов из posts + post_quality (+ clean_posts)
# 2) Форсит тип недели week_goal = "Вовлечение"
# 3) Просит локальную Qwen сгенерировать:
#    - goal
#    - topic_brief
#    - final_post (текст челленджа)
# 4) Сохраняет в writer_challenges
#
# Запуск:
#   python -m analytics.autofill_week1_engagement
#   или
#   python analytics/autofill_week1_engagement.py

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

# Порог качества поста
MIN_QUALITY_SCORE = float(os.getenv("WRITER_MIN_SCORE", "70"))

# Сколько максимум постов брать за прогон (рандом)
MAX_RANDOM_POSTS = 500

WEEK_GOAL_ENGAGEMENT = "Вовлечение"

# === Промпт: генерация челленджа только для 1-й недели (Вовлечение) ===

SYSTEM_GENERATE = (
    "Ты — модератор и геймдизайнер челленджей для онлайн-сообщества про TON / крипту.\n\n"
    "Твой режим: НЕДЕЛЯ ВОВЛЕЧЕНИЯ.\n"
    "Главная цель — запустить живое общение, комментарии и обмен опытом прямо в канале.\n\n"
    "Правила для челленджей на неделе вовлечения:\n"
    "- Тон живой, дружелюбный, мотивирующий.\n"
    "- Действие должно занимать 2–5 минут: написать комментарий, поделиться опытом, проголосовать, показать скрин и т.д.\n"
    "- Чётко объясни, ЧТО именно нужно сделать: одна простая, конкретная задача.\n"
    "- Делай акцент на обмене мнениями и опытом между участниками.\n"
    "- Можно использовать эмодзи, чтобы добавить энергии, но без спама.\n\n"
    "На вход ты получаешь оригинальный пост из канала (тон/тематика), а на выходе должен выдать вовлекающий челлендж.\n\n"
    "Формат ответа СТРОГО такой (один JSON-объект):\n"
    "{\n"
    "  \"week_goal\": \"Вовлечение\",\n"
    "  \"goal\": <строка с формулировкой цели челленджа>,\n"
    "  \"topic_brief\": <краткое текстовое описание сути челленджа>,\n"
    "  \"final_post\": <готовый текст челленджа для Telegram-канала>\n"
    "}\n\n"
    "Требования:\n"
    "- week_goal ВСЕГДА = \"Вовлечение\".\n"
    "- goal: 1–2 предложения, что мы хотим получить от участников (например: \"собрать истории\", \"запустить дискуссию\").\n"
    "- topic_brief: несколько слов или 1–2 коротких предложения.\n"
    "- final_post: полноценный текст челленджа (обращение + объяснение + простое действие).\n"
    "- Пиши на русском языке.\n"
    "- Можно использовать эмодзи, но умеренно.\n\n"
    "Никакого текста до или после JSON.\n"
    "Никаких ```json и других обёрток.\n"
)

USER_GENERATE_TEMPLATE = (
    "Тип недели (week_goal): Вовлечение\n\n"
    "Канал: {channel}\n\n"
    "Оригинальный пост (для контекста тона и темы):\n\"\"\"\n{post}\n\"\"\"\n\n"
    "Сделай на основе этого поста вовлекающий челлендж по правилам из system-сообщения.\n"
    "Верни ТОЛЬКО один JSON-объект с полями week_goal, goal, topic_brief, final_post."
)

# === Модель и вспомогательные функции ===

_tokenizer: Any = None
_model: Any = None


def ensure_model():
    """Лениво загружаем токенайзер и модель один раз на процесс."""
    global _tokenizer, _model
    if _tokenizer is not None and _model is not None:
        return

    print(f"[{datetime.now().isoformat()}] Загрузка модели для Week1 (Вовлечение)...")
    _tokenizer, _model = load_tokenizer_model()

    try:
        device = _model.device
    except Exception:
        params = list(_model.parameters())
        device = params[0].device if params else torch.device("cpu")

    print(f"[{datetime.now().isoformat()}] Модель загружена на {device}")


def _cut_first_json_block(text: str) -> str:
    """
    Вырезаем первый законченный блок JSON по балансу фигурных скобок.
    Если не нашли корректный блок — возвращаем исходный текст.
    """
    start = text.find("{")
    if start == -1:
        return text

    depth = 0
    for i, ch in enumerate(text[start:], start=start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start: i + 1]
    return text


def extract_json(text: str) -> Optional[Dict[str, Any]]:
    """
    Устойчивая попытка вытащить JSON с week_goal/goal/topic_brief/final_post
    из ответа модели.
    """
    if not text:
        return None

    text = re.sub(r"```.*?```", " ", text, flags=re.S)
    text = re.sub(r"[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\x9f]", "", text)
    text = text.strip()

    text = _cut_first_json_block(text)

    decoder = json.JSONDecoder()

    # 1) Пытаемся как обычный JSON
    for m in re.finditer(r"\{", text):
        start = m.start()
        try:
            obj, _ = decoder.raw_decode(text[start:])
            if isinstance(obj, dict):
                week_goal_raw = str(obj.get("week_goal", "") or "")
                goal = str(obj.get("goal", "") or "").strip()
                topic_brief = str(obj.get("topic_brief", "") or "").strip()
                final_post = str(obj.get("final_post", "") or "").strip()

                for stopper in ["```", "对不起", "```json", "```JSON"]:
                    idx = final_post.find(stopper)
                    if idx != -1:
                        final_post = final_post[:idx].strip()

                if not goal or not topic_brief or not final_post:
                    return None

                return {
                    "week_goal": week_goal_raw or WEEK_GOAL_ENGAGEMENT,
                    "goal": goal,
                    "topic_brief": topic_brief,
                    "final_post": final_post,
                }
        except Exception:
            continue

    # 2) Фоллбек: регулярки
    def _unescape(s: str) -> str:
        try:
            return bytes(s, "utf-8").decode("unicode_escape")
        except Exception:
            return s

    week_match = re.search(r'"week_goal"\s*:\s*"(?P<val>.*?)"', text, flags=re.S)
    goal_match = re.search(r'"goal"\s*:\s*"(?P<val>.*?)"', text, flags=re.S)
    brief_match = re.search(r'"topic_brief"\s*:\s*"(?P<val>.*?)"', text, flags=re.S)
    final_match = re.search(r'"final_post"\s*:\s*"(?P<val>.*?)"', text, flags=re.S)

    if not (goal_match and brief_match and final_match):
        return None

    week_raw = week_match.group("val") if week_match else ""
    goal_raw = goal_match.group("val")
    brief_raw = brief_match.group("val")
    final_raw = final_match.group("val")

    goal = _unescape(goal_raw).strip()
    topic_brief = _unescape(brief_raw).strip()
    final_post = _unescape(final_raw).strip()

    for stopper in ["```", "对不起", "```json", "```JSON"]:
        idx = final_post.find(stopper)
        if idx != -1:
            final_post = final_post[:idx].strip()

    if not goal or not topic_brief or not final_post:
        return None

    return {
        "week_goal": week_raw or WEEK_GOAL_ENGAGEMENT,
        "goal": goal,
        "topic_brief": topic_brief,
        "final_post": final_post,
    }


def _generate_raw(messages: List[Dict[str, str]], max_new_tokens: int = 512) -> str:
    """
    Chat messages → сырой текст модели.
    """
    ensure_model()

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
        max_new_tokens=max_new_tokens,
        do_sample=False,
        pad_token_id=getattr(_tokenizer, "eos_token_id", None),
        eos_token_id=getattr(_tokenizer, "eos_token_id", None),
    )

    with torch.inference_mode():
        out = _model.generate(**gen_kwargs)

    gen_ids = out[0][input_ids.shape[-1]:]
    gen_text = _tokenizer.decode(gen_ids, skip_special_tokens=True)
    return gen_text


def generate_engagement_challenge(
    channel: str,
    post_text: str,
) -> Optional[Dict[str, Any]]:
    """
    Генерация одного вовлекающего челленджа для поста.
    """
    messages = [
        {"role": "system", "content": SYSTEM_GENERATE},
        {
            "role": "user",
            "content": USER_GENERATE_TEMPLATE.format(
                channel=channel,
                post=post_text[:16000],
            ),
        },
    ]

    gen_text = _generate_raw(messages, max_new_tokens=768)
    js = extract_json(gen_text)
    if js is None:
        print(f"[{datetime.now().isoformat()}] ⚠️ Не удалось вытащить JSON (Вовлечение) для канала {channel}.")
        print("===== RAW gen_text (полный) =====")
        print(gen_text)
        print("========== END RAW gen_text ==========")
        return None

    js["week_goal"] = WEEK_GOAL_ENGAGEMENT
    return js


# === Украшение челленджей эмодзи (чуть более плотное, как ты просил) ===

def add_emojis_to_challenge(channel: str, text: str) -> str:
    """
    Украшение текста челленджа эмодзи:
    – даём яркий header,
    – добавляем эмодзи к ключевым словам,
    – чуть украшаем списки и тикеры.
    """
    if not text:
        return text

    # 0) Header в первой строке
    lines = text.splitlines()
    if lines:
        first = lines[0]
        if not re.search(r"[🎯🔥🚀✨⭐🤝🙌📊💬🪙💰]", first):
            lines[0] = "🔥🎯 " + first.lstrip()
        text = "\n".join(lines)

    # 1) Эмодзи по словам
    word_emojis = {
        "челлендж": "🎯",
        "задание": "🎯",
        "задача": "🎯",
        "миссия": "🎯",
        "цель": "🎯",

        "поделитесь": "💬",
        "делитесь": "💬",
        "напишите": "💬",
        "расскажите": "💬",
        "ответьте": "💬",
        "комментариях": "💬",
        "комментарии": "💬",

        "опыт": "📌",
        "пример": "📌",
        "история": "📌",
        "истории": "📌",

        "голосование": "📊",
        "опрос": "📊",
        "выберите": "✅",
        "выбор": "✅",

        "присоединяйтесь": "🙌",
        "участвуйте": "🙌",
        "поддержите": "🤝",
        "вместе": "🤝",
        "друзья": "🤝",
        "сообщество": "🤝",

        "прибыль": "💰",
        "доход": "💰",
        "выгода": "💰",
        "бонус": "🎁",
        "приз": "🏆",

        "день": "🕒",
        "дней": "🕒",
        "неделю": "🗓️",
        "неделя": "🗓️",
        "каждый": "🔁",

        "крипта": "🪙",
        "кошелек": "👛",
        "кошелька": "👛",
        "кошельки": "👛",
        "бот": "🤖",
        "бота": "🤖",
        "dapp": "🧩",
        "dapps": "🧩",
    }

    pattern = r"\b(" + "|".join(map(re.escape, word_emojis.keys())) + r")\b"

    def word_repl(match: re.Match) -> str:
        word = match.group(0)
        key = word.lower()
        emoji = word_emojis.get(key)
        if not emoji:
            return word
        after = match.string[match.end():match.end() + 4]
        if after.strip().startswith(emoji):
            return word
        return f"{word} {emoji}"

    text = re.sub(pattern, word_repl, text, flags=re.IGNORECASE)

    # 2) Украшаем списки и “шаги”
    enhanced_lines = []
    for line in text.splitlines():
        stripped = line.lstrip()
        if stripped.startswith(("-", "—", "*")):
            enhanced_lines.append(re.sub(r"^(\s*[-—*])", r"\1 ✨", line))
        elif stripped.lower().startswith(("шаг", "пункт", "день", "задача")):
            enhanced_lines.append("⭐ " + stripped)
        else:
            enhanced_lines.append(line)
    text = "\n".join(enhanced_lines)

    # 3) Тикеры
    substr_replacements = {
        "TON ": "TON 💎 ",
        "Ton ": "Ton 💎 ",
        "TON-": "TON 💎-",
        "USDT": "USDT 💵",
        "BTC": "BTC ₿",
        "ETH": "ETH ⚡",
    }

    for src, dst in substr_replacements.items():
        text = text.replace(src, dst)

    return text


# === Работа с БД ===

CREATE_WRITER_CHALLENGES_SQL = """
CREATE TABLE IF NOT EXISTS writer_challenges (
    id SERIAL PRIMARY KEY,
    source_post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    channel_username VARCHAR(255) NOT NULL,
    week_goal VARCHAR(64) NOT NULL,
    goal TEXT NOT NULL,
    topic_brief TEXT NOT NULL,
    final_challenge TEXT NOT NULL,
    gen_status VARCHAR(32) NOT NULL DEFAULT 'ok',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_post_id)
);
"""

# 500 случайных хороших постов, которые ещё НЕ попадали в writer_challenges вообще
SELECT_RANDOM_CANDIDATES_SQL = """
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
        SELECT 1 FROM writer_challenges wc
        WHERE wc.source_post_id = p.id
    )
ORDER BY random()
LIMIT $2;
"""

INSERT_WRITER_CHALLENGE_SQL = """
INSERT INTO writer_challenges (
    source_post_id,
    channel_username,
    week_goal,
    goal,
    topic_brief,
    final_challenge,
    gen_status
) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (source_post_id) DO NOTHING;
"""


async def ensure_writer_challenges_table(conn: asyncpg.Connection) -> None:
    await conn.execute(CREATE_WRITER_CHALLENGES_SQL)


async def fetch_random_candidates(conn) -> List[asyncpg.Record]:
    rows = await conn.fetch(
        SELECT_RANDOM_CANDIDATES_SQL,
        MIN_QUALITY_SCORE,
        MAX_RANDOM_POSTS,
    )
    print(f"[{datetime.now().isoformat()}] Найдено случайных кандидатов для Week1 (Вовлечение): {len(rows)}")
    return rows


async def save_challenge_sample(
    conn: asyncpg.Connection,
    post_id: int,
    channel: str,
    week_goal: str,
    goal: str,
    topic_brief: str,
    final_challenge: str,
    gen_status: str = "ok",
) -> None:
    await conn.execute(
        INSERT_WRITER_CHALLENGE_SQL,
        post_id,
        channel,
        week_goal,
        goal.strip(),
        topic_brief.strip(),
        final_challenge.strip(),
        gen_status,
    )


def print_progress(current: int, total: int) -> None:
    if total <= 0:
        return
    ratio = current / total
    bar_len = 30
    filled = int(bar_len * ratio)
    bar = "█" * filled + "░" * (bar_len - filled)
    print(
        f"[{datetime.now().isoformat()}] Прогресс Week1 (Вовлечение): |{bar}| {ratio * 100:5.1f}% ({current}/{total})",
        end="\r",
        flush=True,
    )


# === Основной цикл ===

async def main():
    print(f"[{datetime.now().isoformat()}] 🚀 Автогенерация Week1 (Вовлечение) стартует...")
    conn = await asyncpg.connect(**DB)
    try:
        await ensure_writer_challenges_table(conn)

        rows = await fetch_random_candidates(conn)
        total = len(rows)
        if not rows:
            print(f"[{datetime.now().isoformat()}] Нет подходящих постов — выходим.")
            return

        processed = 0
        skipped = 0
        seen = 0

        for r in rows:
            seen += 1
            post_id = r["post_id"]
            channel = r["channel_username"]
            text = (r["text"] or "").strip()
            ingest_status = r["ingest_status"]

            if ingest_status != "done":
                print(f"[{datetime.now().isoformat()}] ⚠️ post_id={post_id} с ingest_status={ingest_status}, пропускаем.")
                skipped += 1
                print_progress(seen, total)
                continue

            if not text:
                skipped += 1
                print_progress(seen, total)
                continue

            print(f"[{datetime.now().isoformat()}] → Week1: обработка post_id={post_id} ({channel})")

            js = generate_engagement_challenge(channel, text)
            if not js:
                print(f"[{datetime.now().isoformat()}] ⚠️ Не удалось сгенерировать Week1-челлендж для post_id={post_id}, пропускаем.")
                skipped += 1
                print_progress(seen, total)
                continue

            goal = str(js.get("goal", "") or "").strip()
            topic_brief = str(js.get("topic_brief", "") or "").strip()
            final_challenge = str(js.get("final_post", "") or "").strip()

            if not goal or not topic_brief or not final_challenge:
                print(f"[{datetime.now().isoformat()}] ⚠️ Пустые поля в JSON для post_id={post_id}, пропускаем.")
                skipped += 1
                print_progress(seen, total)
                continue

            final_challenge = add_emojis_to_challenge(channel, final_challenge)

            await save_challenge_sample(
                conn,
                post_id,
                channel,
                WEEK_GOAL_ENGAGEMENT,
                goal,
                topic_brief,
                final_challenge,
                "ok",
            )
            processed += 1
            print(f"[{datetime.now().isoformat()}] ✅ post_id={post_id} → writer_challenges (week_goal='Вовлечение', gen_status=ok)")

            print_progress(seen, total)

        print()  # перенос после прогресс-бара
        print(f"[{datetime.now().isoformat()}] Готово. Week1: успешно {processed}, пропущено {skipped}")

    finally:
        await conn.close()
        print(f"[{datetime.now().isoformat()}] 🔌 Соединение с БД закрыто.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Остановлено пользователем.")
