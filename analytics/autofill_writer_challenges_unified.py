# analytics/autofill_writer_challenges_unified.py
#
# Единый автофилл для writer_challenges:
# 1) Берёт хорошие посты (posts + post_quality + clean_posts), которых ещё нет в writer_challenges
# 2) Перемешивает
# 3) Назначает week_goal по кругу: Вовлечение → Удержание → Продажи → Реактивация
# 4) Для каждой цели генерирует челлендж специальным промптом
# 5) Сохраняет в writer_challenges, posts.ingest_status НЕ трогает
#
# Запуск:
#   python -m analytics.autofill_writer_challenges_unified
#   или
#   python analytics/autofill_writer_challenges_unified.py

from __future__ import annotations
import os
import sys
import json
import re
import asyncio
import random
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

from Models.qwen_loader import load_tokenizer_model

DB = {
    "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "engagex"),
    "user": os.getenv("POSTGRES_USER", "engagex"),
    "password": os.getenv("POSTGRES_PASSWORD", "engagex"),
}

# Порог качества и лимит
MIN_QUALITY_SCORE = float(os.getenv("WRITER_MIN_SCORE", "70"))
MAX_POSTS = int(os.getenv("WRITER_MAX_POSTS", "1000000"))

WEEK_GOALS = ["Вовлечение", "Удержание", "Продажи", "Реактивация"]

# ============================================
# 1. ПРОМПТЫ ДЛЯ 3 НЕДЕЛЬ (БЕЗ РЕАКТИВАЦИИ)
# ============================================

SYSTEM_GENERATE_3 = (
    "Ты — модератор и геймдизайнер челленджей для онлайн-сообщества про TON / крипту.\n\n"
    "На вход ты получаешь:\n"
    "- тип недели (week_goal): \"Вовлечение\", \"Удержание\" или \"Продажи\";\n"
    "- оригинальный пост из канала;\n"
    "- дополнительные правила для этой недели.\n\n"
    "Твоя задача — на основе поста и типа недели:\n"
    "1) Придумать ОДИН конкретный челлендж для участников под заданный week_goal.\n"
    "2) Вернуть ОДИН JSON-объект строгого формата:\n"
    "{\n"
    "  \"week_goal\": <строка, строго одна из: \"Вовлечение\", \"Удержание\", \"Продажи\">,\n"
    "  \"goal\": <строка с формулировкой цели челленджа>,\n"
    "  \"topic_brief\": <краткое описание челленджа>,\n"
    "  \"final_post\": <готовый текст челленджа для Telegram-канала>\n"
    "}\n\n"
    "Важно:\n"
    "- В поле week_goal скопируй тот тип недели, который тебе передали.\n"
    "- В поле goal НЕ нужно повторять формулировки про неделю, просто сформулируй, что должны сделать люди.\n"
    "- final_post — это именно текст челленджа, а не новость.\n"
    "- Обращайся к читателю на \"вы\" или нейтрально.\n"
    "- Дай понятное действие: что нужно написать / показать / сделать (поделиться опытом, проголосовать, протестировать фичу и т.п.).\n"
    "- Стиль живой, деловой, без кликбейта и токсичности.\n"
    "- Можно использовать эмодзи, но умеренно.\n\n"
    "Формат ответа:\n"
    "- строго один валидный JSON-объект;\n"
    "- БЕЗ пояснений до или после JSON;\n"
    "- БЕЗ обёртки ```json``` или любых других код-блоков.\n"
    "- Отвечай ТОЛЬКО на русском языке.\n"
)

WEEK_GOAL_RULES: Dict[str, str] = {
    "Вовлечение": (
        "Неделя вовлечения.\n"
        "- Цель — побудить людей активно писать в чат, делиться мнениями и опытом.\n"
        "- Задание должно быть простым и выполнимым за 1–5 минут.\n"
        "- Сфокусируйся на вопросах, просьбе поделиться опытом или мнением.\n"
    ),
    "Удержание": (
        "Неделя удержания.\n"
        "- Цель — сформировать привычку участвовать регулярно.\n"
        "- Хороши форматы: мини-дневники, серии коротких заметок, повторяющиеся действия.\n"
        "- Сделай акцент на регулярности (каждый день / несколько дней подряд).\n"
    ),
    "Продажи": (
        "Неделя мягких продаж.\n"
        "- Цель — аккуратно подвести к действию, связанному с продуктом/сервисом.\n"
        "- Покажи пользу и сценарий использования продукта.\n"
        "- Дай один чёткий CTA: зарегистрироваться, протестировать, купить, активировать функцию.\n"
        "- Не дави и не пугай, продавай через пользу.\n"
    ),
}

GENERATE_3_USER_TEMPLATE = (
    "Тип недели (week_goal): {week_goal}\n\n"
    "Специальные правила для этой недели:\n"
    "{rules}\n"
    "Канал: {channel}\n\n"
    "Оригинальный пост:\n\"\"\"\n{post}\n\"\"\"\n\n"
    "На основе этого поста и указанных правил:\n"
    "1) Придумай один челлендж для участников.\n"
    "2) Верни ТОЛЬКО ОДИН JSON-объект с полями week_goal, goal, topic_brief, final_post.\n"
)

# ============================================
# 2. ПРОМПТ ДЛЯ РЕАКТИВАЦИИ
# ============================================

SYSTEM_GENERATE_REACT = (
    "Ты — модератор и геймдизайнер челленджей для онлайн-сообщества про TON / крипту.\n\n"
    "Твой режим: НЕДЕЛЯ РЕАКТИВАЦИИ.\n"
    "Цель — вернуть к жизни участников, которые давно молчат.\n\n"
    "Правила для челленджей на неделе реактивации:\n"
    "- Тон тёплый, поддерживающий.\n"
    "- Признай, что нормально делать паузы и пропадать.\n"
    "- Покажи, что участника здесь ждут.\n"
    "- Дай ОЧЕНЬ простое действие: короткий ответ, одна мысль, плюсик, реакция, голос.\n"
    "- Никакого стыда и давления, никаких формулировок типа \"куда пропал?\".\n"
    "- Не требуй длинных отчётов или больших усилий.\n\n"
    "Формат ответа СТРОГО такой (один JSON-объект):\n"
    "{\n"
    "  \"week_goal\": \"Реактивация\",\n"
    "  \"goal\": <строка с формулировкой цели челленджа>,\n"
    "  \"topic_brief\": <краткое описание сути челленджа>,\n"
    "  \"final_post\": <готовый текст челленджа для Telegram-канала>\n"
    "}\n\n"
    "Никакого текста до или после JSON, никаких ```json.\n"
    "Отвечай ТОЛЬКО на русском языке.\n"
)

REACT_USER_TEMPLATE = (
    "Тип недели (week_goal): Реактивация\n\n"
    "Канал: {channel}\n\n"
    "Оригинальный пост (для контекста тона и темы):\n\"\"\"\n{post}\n\"\"\"\n\n"
    "Сделай на основе этого поста тёплый реактивационный челлендж по правилам из system-сообщения.\n"
    "Верни ТОЛЬКО один JSON-объект с полями week_goal, goal, topic_brief, final_post."
)

# ============================================
# 3. МОДЕЛЬ + УТИЛИТЫ
# ============================================

_tokenizer: Any = None
_model: Any = None


def ensure_model():
    global _tokenizer, _model
    if _tokenizer is not None and _model is not None:
        return
    print(f"[{datetime.now().isoformat()}] Загрузка модели для unified CHALLENGE...")
    _tokenizer, _model = load_tokenizer_model()
    try:
        device = _model.device
    except Exception:
        params = list(_model.parameters())
        device = params[0].device if params else torch.device("cpu")
    print(f"[{datetime.now().isoformat()}] Модель загружена на {device}")


def _cut_first_json_block(text: str) -> str:
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
                return text[start : i + 1]
    return text


def extract_json(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    text = re.sub(r"```.*?```", " ", text, flags=re.S)
    text = re.sub(r"[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\x9f]", "", text)
    text = text.replace('"topic_bир"', '"topic_brief"')
    text = text.strip()
    text = _cut_first_json_block(text)
    decoder = json.JSONDecoder()

    # основная попытка
    for m in re.finditer(r"\{", text):
        start = m.start()
        try:
            obj, _ = decoder.raw_decode(text[start:])
            if isinstance(obj, dict):
                goal = str(obj.get("goal", "") or "").strip()
                brief = str(obj.get("topic_brief", "") or "").strip()
                final_post = str(obj.get("final_post", "") or "").strip()
                for stopper in ["```", "对不起", "```json", "```JSON"]:
                    idx = final_post.find(stopper)
                    if idx != -1:
                        final_post = final_post[:idx].strip()
                if not goal or not brief or not final_post:
                    return None
                return {
                    "week_goal": str(obj.get("week_goal", "") or "").strip(),
                    "goal": goal,
                    "topic_brief": brief,
                    "final_post": final_post,
                }
        except Exception:
            continue

    # фоллбек — регулярки
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
    brief = _unescape(brief_raw).strip()
    final_post = _unescape(final_raw).strip()
    for stopper in ["```", "对不起", "```json", "```JSON"]:
        idx = final_post.find(stopper)
        if idx != -1:
            final_post = final_post[:idx].strip()

    if not goal or not brief or not final_post:
        return None

    return {
        "week_goal": week_raw.strip(),
        "goal": goal,
        "topic_brief": brief,
        "final_post": final_post,
    }


def _generate_raw(messages: List[Dict[str, str]], max_new_tokens: int = 512) -> str:
    ensure_model()
    try:
        inputs = _tokenizer.apply_chat_template(
            messages, add_generation_prompt=True, return_tensors="pt"
        )
    except TypeError:
        inputs = _tokenizer.apply_chat_template(messages, return_tensors="pt")

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

    gen_ids = out[0][input_ids.shape[-1] :]
    gen_text = _tokenizer.decode(gen_ids, skip_special_tokens=True)
    return gen_text


def generate_for_goal(channel: str, post_text: str, week_goal: str) -> Optional[Dict[str, Any]]:
    """
    Генерация челленджа под конкретный week_goal.
    """
    if week_goal == "Реактивация":
        messages = [
            {"role": "system", "content": SYSTEM_GENERATE_REACT},
            {
                "role": "user",
                "content": REACT_USER_TEMPLATE.format(
                    channel=channel,
                    post=post_text[:16000],
                ),
            },
        ]
    else:
        rules = WEEK_GOAL_RULES.get(week_goal, "")
        messages = [
            {"role": "system", "content": SYSTEM_GENERATE_3},
            {
                "role": "user",
                "content": GENERATE_3_USER_TEMPLATE.format(
                    week_goal=week_goal,
                    rules=rules,
                    channel=channel,
                    post=post_text[:16000],
                ),
            },
        ]

    gen_text = _generate_raw(messages, max_new_tokens=768)
    js = extract_json(gen_text)
    if js is None:
        print(f"[{datetime.now().isoformat()}] ⚠️ Не удалось вытащить JSON для {channel} (week_goal={week_goal}).")
        print("===== RAW gen_text =====")
        print(gen_text)
        print("========== END RAW =====")
        return None

    # жёстко фиксируем тип недели по нашему плану
    js["week_goal"] = week_goal
    return js


# ============================================
# 4. ЭМОДЗИ
# ============================================

def add_emojis_to_challenge(channel: str, text: str) -> str:
    if not text:
        return text

    # хедер
    lines = text.splitlines()
    if lines:
        first = lines[0]
        if not re.search(r"[🎯🔥🚀✨⭐🤝🙌📊💬🪙💰]", first):
            lines[0] = "🎯🚀 " + first.lstrip()
        text = "\n".join(lines)

    word_emojis = {
        "челлендж": "🎯",
        "задание": "🎯",
        "задача": "🎯",
        "миссия": "🎯",
        "поделитесь": "💬",
        "делитесь": "💬",
        "напишите": "💬",
        "расскажите": "💬",
        "комментариях": "💬",
        "опыт": "📌",
        "пример": "📌",
        "результат": "📌",
        "отчёт": "📊",
        "отчет": "📊",
        "голосование": "📊",
        "опрос": "📊",
        "присоединяйтесь": "🙌",
        "участвуйте": "🙌",
        "поддержите": "🤝",
        "сообщество": "🤝",
        "прибыль": "💰",
        "доход": "💰",
        "бонус": "🎁",
        "приз": "🏆",
        "награда": "🏆",
        "день": "🕒",
        "дней": "🕒",
        "неделю": "🗓️",
        "неделя": "🗓️",
        "каждый": "🔁",
        "крипта": "🪙",
        "кошелек": "👛",
        "кошелька": "👛",
        "бот": "🤖",
        "бота": "🤖",
    }

    pattern = r"\b(" + "|".join(map(re.escape, word_emojis.keys())) + r")\b"

    def word_repl(m: re.Match) -> str:
        word = m.group(0)
        key = word.lower()
        emoji = word_emojis.get(key)
        if not emoji:
            return word
        after = m.string[m.end() : m.end() + 4]
        if after.strip().startswith(emoji):
            return word
        return f"{word} {emoji}"

    text = re.sub(pattern, word_repl, text, flags=re.IGNORECASE)

    # начало строк-списков
    enhanced = []
    for line in text.splitlines():
        stripped = line.lstrip()
        if stripped.startswith(("-", "—", "*")):
            enhanced.append(re.sub(r"^(\s*[-—*])", r"\1 ✨", line))
        else:
            enhanced.append(line)
    text = "\n".join(enhanced)

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

# ============================================
# 5. БАЗА ДАННЫХ
# ============================================

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
        SELECT 1 FROM writer_challenges wc
        WHERE wc.source_post_id = p.id
    )
ORDER BY pq.quality_score DESC, p.id
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


async def fetch_candidates(conn) -> List[asyncpg.Record]:
    rows = await conn.fetch(SELECT_CANDIDATES_SQL, MIN_QUALITY_SCORE, MAX_POSTS)
    print(f"[{datetime.now().isoformat()}] Найдено кандидатов: {len(rows)}")
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
        f"[{datetime.now().isoformat()}] Прогресс unified: |{bar}| {ratio * 100:5.1f}% ({current}/{total})",
        end="\r",
        flush=True,
    )


# ============================================
# 6. ОСНОВНОЙ ЦИКЛ
# ============================================

async def main():
    print(f"[{datetime.now().isoformat()}] 🚀 Unified авторазметка CHALLENGE стартует...")
    random.seed(42)

    conn = await asyncpg.connect(**DB)
    try:
        await ensure_writer_challenges_table(conn)

        rows = await fetch_candidates(conn)
        total = len(rows)
        if not rows:
            print(f"[{datetime.now().isoformat()}] Нет подходящих постов — выходим.")
            return

        # перемешиваем и назначаем цели по кругу
        random.shuffle(rows)
        assignments: List[tuple[asyncpg.Record, str]] = []
        for idx, r in enumerate(rows):
            week_goal = WEEK_GOALS[idx % len(WEEK_GOALS)]
            assignments.append((r, week_goal))

        processed = 0
        skipped = 0

        for i, (r, week_goal) in enumerate(assignments, start=1):
            post_id = r["post_id"]
            channel = r["channel_username"]
            text = (r["text"] or "").strip()
            ingest_status = r["ingest_status"]

            # сюда пишет только по реально обработанным постам;
            # если ingest_status не done или текста нет – пост ещё можно будет догрузить в будущем,
            # поэтому не создаём строку с error
            if ingest_status != "done" or not text:
                skipped += 1
                print_progress(i, total)
                continue

            print(
                f"[{datetime.now().isoformat()}] → post_id={post_id} ({channel}), week_goal={week_goal}"
            )

            js = generate_for_goal(channel, text, week_goal)

            # 1) Модель вообще не вернула JSON → фиксируем gen_status='error'
            if not js:
                err_stub = "[gen_error]"
                await save_challenge_sample(
                    conn,
                    post_id,
                    channel,
                    week_goal,
                    err_stub,
                    err_stub,
                    f"[gen_error] не удалось сгенерировать челлендж для post_id={post_id}",
                    "error",
                )
                skipped += 1
                print(
                    f"[{datetime.now().isoformat()}] ❌ post_id={post_id} → writer_challenges (week_goal='{week_goal}', gen_status=error)"
                )
                print_progress(i, total)
                continue

            goal = str(js.get("goal", "") or "").strip()
            topic_brief = str(js.get("topic_brief", "") or "").strip()
            final_challenge = str(js.get("final_post", "") or "").strip()

            # 2) JSON есть, но поля пустые → тоже error
            if not goal or not topic_brief or not final_challenge:
                print(
                    f"[{datetime.now().isoformat()}] ⚠️ Пустые поля JSON, ставим gen_status=error для post_id={post_id}"
                )
                err_stub = "[empty_json]"
                await save_challenge_sample(
                    conn,
                    post_id,
                    channel,
                    week_goal,
                    err_stub,
                    err_stub,
                    f"[empty_json] пустые поля JSON для post_id={post_id}",
                    "error",
                )
                skipped += 1
                print_progress(i, total)
                continue

            # 3) Нормальный кейс
            final_challenge = add_emojis_to_challenge(channel, final_challenge)

            await save_challenge_sample(
                conn,
                post_id,
                channel,
                week_goal,
                goal,
                topic_brief,
                final_challenge,
                "ok",
            )
            processed += 1
            print(
                f"[{datetime.now().isoformat()}] ✅ post_id={post_id} → writer_challenges (week_goal='{week_goal}', gen_status=ok)"
            )

            print_progress(i, total)

        print()
        print(
            f"[{datetime.now().isoformat()}] Готово. Успешно: {processed}, пропущено: {skipped}"
        )

    finally:
        await conn.close()
        print(f"[{datetime.now().isoformat()}] 🔌 Соединение с БД закрыто.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Остановлено пользователем.")
