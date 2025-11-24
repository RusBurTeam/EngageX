# analytics/autofill_writer_challenges.py
#
# Автоматически генерит CHALLENGE-записи в writer_challenges:
# 1) Берёт хорошие посты из posts + post_quality (+ clean_posts)
# 2) Шаг 1: модель определяет тип недели (week_goal) ИЗ 3 ВАРИАНТОВ:
#       "Вовлечение", "Удержание", "Продажи"
#    (Реактивацию ты делаешь отдельным скриптом)
# 3) Шаг 2: модель генерирует челлендж под этот week_goal по спец-промпту
# 4) Сохраняет в writer_challenges, НИЧЕГО не меняя в posts.ingest_status
#
# Запуск:
#   python -m analytics.autofill_writer_challenges
#   или
#   python analytics/autofill_writer_challenges.py

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

# Порог качества поста, выше которого считаем его годным для челленджа
MIN_QUALITY_SCORE = float(os.getenv("WRITER_MIN_SCORE", "70"))

# Ограничение на количество постов за один прогон
MAX_POSTS = int(os.getenv("WRITER_MAX_POSTS", "10000000"))

# Допустимые типы недель под челленджи (ТУТ ТОЛЬКО 3, БЕЗ РЕАКТИВАЦИИ)
WEEK_GOAL_CHOICES = [
    "Вовлечение",   # Неделя 1
    "Удержание",    # Неделя 2
    "Продажи",      # Неделя 3
]

# === 1. КЛАССИФИКАЦИЯ week_goal (3 типа) ===

SYSTEM_CLASSIFY = (
    "Ты помогаешь администратору Telegram-сообщества по TON / крипте "
    "классифицировать посты по типу недели контент-цикла.\n\n"
    "Нужно выбрать РОВНО ОДНУ категорию week_goal из списка:\n"
    "1) \"Вовлечение\" — посты, которые побуждают к общению, обсуждениям, дележу опытом.\n"
    "2) \"Удержание\" — посты, которые помогают сформировать привычку участвовать регулярно "
    "(серии, дайджесты, напоминания, длительные активности).\n"
    "3) \"Продажи\" — посты, которые прямо или мягко подталкивают к действию, "
    "связанному с продуктом/сервисом (регистрация, покупка, тест, апгрейд, использование фичи).\n\n"
    "Важно: сейчас НЕ используем тип \"Реактивация\".\n\n"
    "На вход тебе дают ОДИН пост. Твоя задача — определить, к какой из трёх недель он ближе всего.\n\n"
    "Формат ответа:\n"
    "– Либо просто одна строка с названием недели: \"Вовлечение\" / \"Удержание\" / \"Продажи\".\n"
    "– Либо JSON-объект вида { \"week_goal\": \"Вовлечение\" }.\n\n"
    "Отвечай ТОЛЬКО на русском языке."
)

USER_CLASSIFY_TEMPLATE = (
    "Канал: {channel}\n\n"
    "Оригинальный пост:\n\"\"\"\n{post}\n\"\"\"\n\n"
    "Определи, какой неделе он больше всего соответствует (Вовлечение / Удержание / Продажи).\n"
    "Ответь либо одной строкой, либо JSON-объектом с полем week_goal."
)

# === 2. ГЕНЕРАЦИЯ челленджа под уже выбранный week_goal ===

SYSTEM_GENERATE = (
    "Ты — модератор и геймдизайнер челленджей для онлайн-сообщества про TON / крипту.\n\n"
    "На вход ты получаешь:\n"
    "– тип недели (week_goal): \"Вовлечение\", \"Удержание\" или \"Продажи\";\n"
    "– оригинальный пост из канала;\n"
    "– дополнительные правила для этой недели.\n\n"
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
    "– В поле week_goal скопируй тот тип недели, который тебе передали.\n"
    "– В поле goal НЕ нужно повторять формулировки про неделю, просто сформулируй, что должны сделать люди.\n"
    "– final_post — это именно текст челленджа, а не новость.\n"
    "– Обращайся к читателю на \"вы\" или нейтрально.\n"
    "– Дай понятное действие: что нужно написать / показать / сделать (поделиться опытом, проголосовать, протестировать фичу, вернуться к продукту и т.п.).\n"
    "– Стиль живой, деловой, без кликбейта и токсичности.\n"
    "– Можно использовать эмодзи умеренно, но не превращай текст в кашу.\n\n"
    "Формат ответа:\n"
    "– строго один валидный JSON-объект;\n"
    "– БЕЗ пояснений до или после JSON;\n"
    "– БЕЗ обёртки ```json``` или любых других код-блоков;\n"
    "– БЕЗ комментариев и текста вне фигурных скобок.\n\n"
    "ОБЯЗАТЕЛЬНО:\n"
    "– Отвечай ТОЛЬКО на русском языке.\n"
    "– После закрывающей фигурной скобки JSON не добавляй никакого текста.\n"
)

WEEK_GOAL_RULES: Dict[str, str] = {
    "Вовлечение": (
        "Неделя вовлечения.\n"
        "- Цель — побудить людей активно писать в чат, делиться мнениями и опытом.\n"
        "- Сделай задание, которое хочется выполнить прямо сейчас.\n"
        "- Ответ должен быть выполним за 1–5 минут.\n"
    ),
    "Удержание": (
        "Неделя удержания.\n"
        "- Цель — сформировать привычку участвовать регулярно.\n"
        "- Упор на регулярность: каждый день / каждую неделю.\n"
        "- Подойдёт формат мини-дневника, серии заметок, повторяющегося действия.\n"
    ),
    "Продажи": (
        "Неделя мягких продаж.\n"
        "- Цель — аккуратно подвести к действию, связанному с продуктом (покупка, регистрация, тест, апгрейд, использование фичи).\n"
        "- Покажи пользу и сценарий использования продукта.\n"
        "- Дай один чёткий CTA: что сделать прямо сейчас.\n"
    ),
}

GENERATE_USER_TEMPLATE = (
    "Тип недели (week_goal): {week_goal}\n\n"
    "Специальные правила для этой недели:\n"
    "{rules}\n"
    "Канал: {channel}\n\n"
    "Оригинальный пост:\n\"\"\"\n{post}\n\"\"\"\n\n"
    "На основе этого поста и указанных правил:\n"
    "1) Придумай один челлендж для участников.\n"
    "2) Верни ТОЛЬКО ОДИН JSON-объект с полями week_goal, goal, topic_brief, final_post.\n"
    "Следуй инструкциям из system-сообщения."
)

# === Модель и вспомогательные функции ===

_tokenizer: Any = None
_model: Any = None


def ensure_model():
    """Лениво загружаем токенайзер и модель один раз на процесс."""
    global _tokenizer, _model
    if _tokenizer is not None and _model is not None:
        return

    print(f"[{datetime.now().isoformat()}] Загрузка модели для CHALLENGE-разметки...")
    _tokenizer, _model = load_tokenizer_model()

    try:
        device = _model.device
    except Exception:
        params = list(_model.parameters())
        device = params[0].device if params else torch.device("cpu")

    print(f"[{datetime.now().isoformat()}] Модель загружена на {device}")


def normalize_week_goal(raw: str) -> str:
    """
    Нормализация week_goal в один из канонических:
    "Вовлечение", "Удержание", "Продажи".
    (Если вдруг проскочит Реактивация или что-то странное — дефолт "Вовлечение".)
    """
    if not raw:
        return "Вовлечение"

    t = raw.strip().lower()

    if "вовлеч" in t:
        return "Вовлечение"
    if "удерж" in t or "ретен" in t:
        return "Удержание"
    if "продаж" in t or "покупк" in t or "конверс" in t or "сделк" in t:
        return "Продажи"

    # если модель написала что-то левое — считаем это "Вовлечение"
    return "Вовлечение"


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
    Устойчивый JSON-экстрактор для ответа генерации челленджа.
    Ожидает поля week_goal, goal, topic_brief, final_post.
    + фикс под опечатку topic_bир -> topic_brief.
    """
    if not text:
        return None

    # убираем код-блоки и управляющие символы
    text = re.sub(r"```.*?```", " ", text, flags=re.S)
    text = re.sub(r"[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\x9f]", "", text)

    # фикс для кривого ключа topic_bир
    text = text.replace('"topic_bир"', '"topic_brief"')

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
                week_goal = normalize_week_goal(week_goal_raw)

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
                    "week_goal": week_goal,
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

    week_goal = normalize_week_goal(_unescape(week_raw))
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
        "week_goal": week_goal,
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


def classify_week_goal(channel: str, post_text: str) -> str:
    """
    Шаг 1 — классификация типа недели (3 типа).
    Возвращает нормализованный week_goal.
    """
    messages = [
        {"role": "system", "content": SYSTEM_CLASSIFY},
        {
            "role": "user",
            "content": USER_CLASSIFY_TEMPLATE.format(
                channel=channel,
                post=post_text[:4000],
            ),
        },
    ]

    raw = _generate_raw(messages, max_new_tokens=128).strip()
    candidate = raw

    # Если модель вернула JSON
    if "{" in raw and "}" in raw:
        try:
            js_text = _cut_first_json_block(raw)
            obj = json.loads(js_text)
            if isinstance(obj, dict) and "week_goal" in obj:
                candidate = str(obj["week_goal"])
        except Exception:
            pass

    week_goal = normalize_week_goal(candidate)
    if week_goal not in WEEK_GOAL_CHOICES:
        week_goal = "Вовлечение"
    return week_goal


def build_generation_messages(channel: str, post_text: str, week_goal: str) -> List[Dict[str, str]]:
    rules = WEEK_GOAL_RULES.get(week_goal, "")
    return [
        {"role": "system", "content": SYSTEM_GENERATE},
        {
            "role": "user",
            "content": GENERATE_USER_TEMPLATE.format(
                channel=channel,
                post=post_text[:16000],
                week_goal=week_goal,
                rules=rules,
            ),
        },
    ]


def generate_challenge(
    channel: str,
    post_text: str,
    week_goal: str,
) -> Optional[Dict[str, Any]]:
    """
    Шаг 2 — генерация челленджа под заданный week_goal.
    Возвращает dict с week_goal/goal/topic_brief/final_post либо None.
    """
    messages = build_generation_messages(channel, post_text, week_goal)
    gen_text = _generate_raw(messages, max_new_tokens=768)
    js = extract_json(gen_text)
    if js is None:
        print(f"[{datetime.now().isoformat()}] ⚠️ Не удалось вытащить JSON (генерация челленджа) для канала {channel}.")
        print("===== RAW gen_text (полный) =====")
        print(gen_text)
        print("========== END RAW gen_text ==========")
    return js


# === Украшение челленджей эмодзи ===

def add_emojis_to_challenge(channel: str, text: str) -> str:
    """
    Лёгкое, но более насыщенное украшение текста челленджа эмодзи:
    – добавляем 1–2 эмодзи в начало,
    – подмешиваем эмодзи к ключевым словам (по целым словам),
    – чуть украшаем маркеры строк и тикеры.
    """
    if not text:
        return text

    # -----------------------------
    # 0) Лёгкий header в начале
    # -----------------------------
    # Если в тексте ещё нет явных эмодзи в первой строке — добавим.
    lines = text.splitlines()
    if lines:
        first = lines[0]
        if not re.search(r"[🎯🔥🚀✨⭐🤝🙌📊💬🪙💰]", first):
            # базово для всех челленджей даём «челлендж + ракета»
            lines[0] = "🎯🚀 " + first.lstrip()
        text = "\n".join(lines)

    # -----------------------------
    # 1) Эмодзи по целым словам
    # -----------------------------
    word_emojis = {
        # базовые слова про челлендж / активность
        "челлендж": "🎯",
        "задание": "🎯",
        "задача": "🎯",
        "миссия": "🎯",
        "цель": "🎯",

        # про участие / общение
        "поделитесь": "💬",
        "делитесь": "💬",
        "напишите": "💬",
        "расскажите": "💬",
        "ответьте": "💬",
        "комментариях": "💬",
        "комментарии": "💬",

        # прогресс, результаты
        "опыт": "📌",
        "пример": "📌",
        "результат": "📌",
        "отчёт": "📊",
        "отчет": "📊",

        # голосования / выбор
        "голосование": "📊",
        "опрос": "📊",
        "выберите": "✅",
        "выбор": "✅",

        # мотивация / поддержка
        "присоединяйтесь": "🙌",
        "участвуйте": "🙌",
        "поддержите": "🤝",
        "вместе": "🤝",
        "друзья": "🤝",
        "сообщество": "🤝",

        # деньги / выгода / продукт
        "прибыль": "💰",
        "доход": "💰",
        "выгода": "💰",
        "скидка": "💰",
        "бонус": "🎁",
        "приз": "🏆",
        "награда": "🏆",

        # время / регулярность
        "день": "🕒",
        "дней": "🕒",
        "неделю": "🗓️",
        "неделя": "🗓️",
        "каждый": "🔁",

        # крипта / TON
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
        # Если эмодзи уже сразу после слова — не дублируем
        after = match.string[match.end():match.end() + 4]
        if after.strip().startswith(emoji):
            return word
        return f"{word} {emoji}"

    text = re.sub(pattern, word_repl, text, flags=re.IGNORECASE)

    # -----------------------------
    # 2) Украшаем начало строк со списками / важными фразами
    # -----------------------------
    enhanced_lines = []
    for line in text.splitlines():
        stripped = line.lstrip()

        if stripped.startswith(("-", "—", "*")):
            # маркеры списков
            enhanced_lines.append(re.sub(r"^(\s*[-—*])", r"\1 ✨", line))
        elif stripped.lower().startswith(("шаг", "пункт", "день", "задача")):
            enhanced_lines.append("⭐ " + stripped)
        else:
            enhanced_lines.append(line)
    text = "\n".join(enhanced_lines)

    # -----------------------------
    # 3) Подстрочные тикеры/аббревиатуры
    # -----------------------------
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
    print(f"[{datetime.now().isoformat()}] Найдено кандидатов для CHALLENGE-разметки: {len(rows)}")
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
        f"[{datetime.now().isoformat()}] Прогресс CHALLENGE-разметки: |{bar}| {ratio * 100:5.1f}% ({current}/{total})",
        end="\r",
        flush=True,
    )


# === Основной цикл ===

async def main():
    print(f"[{datetime.now().isoformat()}] 🚀 Авторазметка CHALLENGE-samples (3 типа недель) стартует...")
    conn = await asyncpg.connect(**DB)
    try:
        await ensure_writer_challenges_table(conn)

        rows = await fetch_candidates(conn)
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

            print(f"[{datetime.now().isoformat()}] → Обработка post_id={post_id} ({channel}), ingest_status={ingest_status}")

            # Шаг 1: классификация типа недели (3 типа)
            week_goal = classify_week_goal(channel, text)
            print(f"[{datetime.now().isoformat()}]   week_goal (классификация) = '{week_goal}'")

            # Шаг 2: генерация челленджа под выбранный тип недели
            js = generate_challenge(channel, text, week_goal)
            if not js:
                print(f"[{datetime.now().isoformat()}] ⚠️ Не удалось сгенерировать челлендж для post_id={post_id}, пропускаем (gen_status=error).")
                skipped += 1
                print_progress(seen, total)
                continue

            gen_week_goal = normalize_week_goal(str(js.get("week_goal", "") or ""))
            if gen_week_goal != week_goal:
                print(f"[{datetime.now().isoformat()}] ⚠️ Несовпадение week_goal: classify='{week_goal}', generate='{gen_week_goal}'. Берём classify.")
            week_goal_final = week_goal

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
                week_goal_final,
                goal,
                topic_brief,
                final_challenge,
                "ok",
            )
            processed += 1
            print(f"[{datetime.now().isoformat()}] ✅ post_id={post_id} → записан в writer_challenges (week_goal='{week_goal_final}', gen_status=ok)")

            print_progress(seen, total)

        print()  # перенос после прогресс-бара
        print(f"[{datetime.now().isoformat()}] Готово. Успешно: {processed}, пропущено: {skipped}")

    finally:
        await conn.close()
        print(f"[{datetime.now().isoformat()}] 🔌 Соединение с БД закрыто.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Остановлено пользователем.")
