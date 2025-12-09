# train/eval_lora_with_judge.py
# Сравнение качества: черновик vs teacher vs LoRA-выход по метрике judge_quality_llm

import os
import sys
import json
import re
from datetime import datetime
from typing import List, Dict, Any

import torch
from peft import PeftModel
from dotenv import load_dotenv
import pathlib

# --- базовые пути ---
BASE_DIR = str(pathlib.Path(__file__).resolve().parents[1])
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

load_dotenv(os.path.join(BASE_DIR, ".env"))

# Путь к валидному датасету:
# 1) WRITER_VAL_PATH из .env
# 2) дефолт: data/writer_rewrite_val.jsonl (старый формат тоже поддерживаем)
VAL_PATH = (
    os.getenv("WRITER_VAL_PATH")
    or os.path.join(BASE_DIR, "data", "writer_rewrite_val.jsonl")
)

# Лимит примеров на прогон, по умолчанию 10
VAL_LIMIT = int(os.getenv("WRITER_VAL_LIMIT", "10"))

# Фильтр по типу сэмплов: all | post | challenge
SAMPLE_TYPE_FILTER = os.getenv("WRITER_SAMPLE_TYPE", "all").strip().lower()
if SAMPLE_TYPE_FILTER not in ("all", "post", "challenge"):
    SAMPLE_TYPE_FILTER = "all"

# Базовая директория, где лежат чекпоинты LoRA-писателя
LORA_BASE_DIR = os.path.join(BASE_DIR, "checkpoints", "lora_writer_qwen2_5_7b")

# наш loader базовой Qwen
from Models.qwen_loader import load_tokenizer_model

# judge-модель и инференс
from analytics.judge_quality_llm import (
    infer_batch as judge_infer_batch,
    ensure_model as judge_ensure_model,
)


# --------- утилита: вытащить черновик из user-контента ---------
def extract_draft_from_user(user_content: str) -> str:
    """
    Старый формат:
      "Вот черновик поста:\\n\"\"\"\\n...ТЕКСТ...\\n\"\"\"\\n\\nПерепиши..."
    Новый формат (посты/челленджи):
      просто бриф без черновика.
    Логика:
      – если есть блок между \"\"\" ... \"\"\" — считаем это черновиком;
      – иначе возвращаем весь user_content.
    """
    m = re.search(r'"""(.*?)"""', user_content, flags=re.S)
    if m:
        draft = m.group(1).strip()
        if draft:
            return draft
    return user_content.strip()


# --------- загрузка валидации ---------
def load_val_examples(limit: int) -> List[Dict[str, Any]]:
    if not os.path.exists(VAL_PATH):
        raise FileNotFoundError(f"Не найден val-датасет: {VAL_PATH}")

    examples: List[Dict[str, Any]] = []
    total_lines = 0
    taken_lines = 0

    print(f"[{datetime.now().isoformat()}] 📂 Читаем val-датасет: {VAL_PATH}")
    print(f"[{datetime.now().isoformat()}] 🔎 SAMPLE_TYPE_FILTER = {SAMPLE_TYPE_FILTER}")
    print(f"[{datetime.now().isoformat()}] 🔎 VAL_LIMIT = {limit}")

    with open(VAL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            total_lines += 1
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)

            # Если есть поле sample_type и включён фильтр
            if SAMPLE_TYPE_FILTER != "all" and "sample_type" in obj:
                st = str(obj.get("sample_type", "")).lower()
                if st != SAMPLE_TYPE_FILTER:
                    continue

            msgs = obj.get("messages", [])
            if len(msgs) < 3:
                # ожидаем system, user, assistant
                continue

            system_msg = msgs[0].get("content", "")
            user_msg = msgs[1].get("content", "")
            assistant = msgs[2].get("content", "")

            draft = extract_draft_from_user(user_msg)

            examples.append(
                {
                    "system": system_msg,
                    "user": user_msg,
                    "draft": draft,
                    "ref": assistant,
                    "sample_type": obj.get("sample_type", None),
                }
            )
            taken_lines += 1

            if len(examples) >= limit:
                break

    print(
        f"[{datetime.now().isoformat()}] Взято {len(examples)} примеров из val "
        f"(прочитано строк: {total_lines}, прошло фильтр: {taken_lines})."
    )
    return examples


# --------- поиск директории с LoRA-адаптерами ---------
def resolve_lora_dir() -> str:
    """
    Находим директорию, где реально лежит adapter_config.json LoRA-писателя.

    Приоритет:
    1) Переменная окружения LORA_WRITER (если путь существует и там есть adapter_config.json).
    2) Прямо в LORA_BASE_DIR.
    3) Любая поддиректория LORA_BASE_DIR/*, где есть adapter_config.json.
       Берём самую "позднюю" по имени (часто это последний checkpoint).
    """
    # 1) .env / окружение
    env_dir = os.getenv("LORA_WRITER")
    if env_dir:
        env_dir = os.path.abspath(env_dir)
        cfg = os.path.join(env_dir, "adapter_config.json")
        if os.path.exists(cfg):
            print(
                f"[{datetime.now().isoformat()}] 📌 Используем LORA_WRITER из .env: {env_dir}"
            )
            return env_dir
        else:
            print(
                f"[{datetime.now().isoformat()}] ⚠️ В LORA_WRITER нет adapter_config.json: {cfg}"
            )

    # 2) Прямо в LORA_BASE_DIR
    base_cfg = os.path.join(LORA_BASE_DIR, "adapter_config.json")
    if os.path.exists(base_cfg):
        print(
            f"[{datetime.now().isoformat()}] 📌 Найден adapter_config.json в {LORA_BASE_DIR}"
        )
        return LORA_BASE_DIR

    # 3) Поиск в поддиректориях (checkpoint-1, checkpoint-12 и т.п.)
    candidates = []
    if os.path.isdir(LORA_BASE_DIR):
        for name in sorted(os.listdir(LORA_BASE_DIR)):
            subdir = os.path.join(LORA_BASE_DIR, name)
            if not os.path.isdir(subdir):
                continue
            cfg = os.path.join(subdir, "adapter_config.json")
            if os.path.exists(cfg):
                candidates.append(subdir)

    if candidates:
        chosen = candidates[-1]
        print(
            f"[{datetime.now().isoformat()}] 📌 Найдено несколько LoRA-чекпоинтов, используем: {chosen}"
        )
        return chosen

    msg_lines = [
        "Не удалось найти LoRA-адаптер (adapter_config.json).",
        f"Проверены пути:",
        f"  • LORA_WRITER={env_dir or 'не задана'}",
        f"  • {LORA_BASE_DIR} и его поддиректории",
        "Убедись, что после обучения LoRA у тебя есть папка с файлами adapter_config.json и adapter_model.*",
    ]
    raise FileNotFoundError("\n".join(msg_lines))


# --------- генерация с LoRA-писателем ---------
def load_writer_lora():
    print(f"[{datetime.now().isoformat()}] 🔄 Загружаем базовую модель + LoRA-Writer...")
    tokenizer, base_model = load_tokenizer_model()
    base_model.eval()
    device = base_model.device if hasattr(base_model, "device") else torch.device("cpu")

    lora_dir = resolve_lora_dir()
    print(f"[{datetime.now().isoformat()}] 🔗 LORA_DIR = {lora_dir}")

    lora_model = PeftModel.from_pretrained(
        base_model,
        lora_dir,
        torch_dtype=base_model.dtype,
        is_trainable=False,
    )
    lora_model.eval()
    print(f"[{datetime.now().isoformat()}] ✅ LoRA подключена. device = {device}")
    return tokenizer, lora_model, device


def generate_with_lora(
    tokenizer,
    model,
    device,
    system_text: str,
    user_text: str,
    max_new_tokens: int = 512,
) -> str:
    messages = [
        {"role": "system", "content": system_text},
        {"role": "user", "content": user_text},
    ]
    try:
        inb = tokenizer.apply_chat_template(
            messages, add_generation_prompt=True, return_tensors="pt"
        )
    except TypeError:
        inb = tokenizer.apply_chat_template(messages, return_tensors="pt")

    if isinstance(inb, torch.Tensor):
        input_ids = inb.to(device)
        attention_mask = torch.ones_like(input_ids, dtype=torch.long, device=device)
    elif isinstance(inb, dict):
        input_ids = inb["input_ids"].to(device)
        attention_mask = inb.get(
            "attention_mask",
            torch.ones_like(inb["input_ids"], dtype=torch.long),
        ).to(device)
    else:
        raise RuntimeError(f"Неожиданный формат токенизации: {type(inb)}")

    with torch.inference_mode():
        out = model.generate(
            input_ids=input_ids,
            attention_mask=attention_mask,
            max_new_tokens=max_new_tokens,
            do_sample=False,
            pad_token_id=getattr(tokenizer, "eos_token_id", None),
            eos_token_id=getattr(tokenizer, "eos_token_id", None),
        )

    seq_len = input_ids.shape[1]
    gen_ids = out[0][seq_len:]
    text = tokenizer.decode(gen_ids, skip_special_tokens=True)
    return text.strip()


# --------- оценка через judge_quality_llm ---------
def evaluate_with_judge(drafts: List[str], refs: List[str], loras: List[str]):
    """
    Прогоняем все три группы через judge_quality_llm и считаем средние score.
    Плюс выводим несколько примеров (teacher vs LoRA).
    """
    judge_ensure_model()  # грузим модель-судью

    items = []
    kind_idx = []  # чтобы помнить, какой текст какого типа

    # 0 = draft, 1 = ref, 2 = lora
    pid = 1
    for d in drafts:
        items.append(
            {
                "post_id": pid,
                "channel": "eval-draft",
                "text": d,
                "metrics": {
                    "views": 0,
                    "forwards": 0,
                    "reactions_sum": 0,
                    "comments_count": 0,
                    "engagement_rate": 0.0,
                },
            }
        )
        kind_idx.append(0)
        pid += 1
    for r in refs:
        items.append(
            {
                "post_id": pid,
                "channel": "eval-ref",
                "text": r,
                "metrics": {
                    "views": 0,
                    "forwards": 0,
                    "reactions_sum": 0,
                    "comments_count": 0,
                    "engagement_rate": 0.0,
                },
            }
        )
        kind_idx.append(1)
        pid += 1
    for l in loras:
        items.append(
            {
                "post_id": pid,
                "channel": "eval-lora",
                "text": l,
                "metrics": {
                    "views": 0,
                    "forwards": 0,
                    "reactions_sum": 0,
                    "comments_count": 0,
                    "engagement_rate": 0.0,
                },
            }
        )
        kind_idx.append(2)
        pid += 1

    print(
        f"[{datetime.now().isoformat()}] ⚖️ Отправляем {len(items)} текстов в judge_quality_llm..."
    )
    results = judge_infer_batch(items)

    n = len(drafts)
    if len(results) != 3 * n:
        print(
            f"[{datetime.now().isoformat()}] ⚠️ Ожидалось {3*n} результатов от judge, получено {len(results)}."
        )

    # разбиваем результаты на три блока
    res_draft = results[0:n]
    res_ref = results[n : 2 * n]
    res_lora = results[2 * n : 3 * n]

    # собираем по типам (для средних значений)
    sums = {0: 0.0, 1: 0.0, 2: 0.0}
    counts = {0: n, 1: n, 2: n}

    for r in res_draft:
        sums[0] += float(r.get("score", 0.0))
    for r in res_ref:
        sums[1] += float(r.get("score", 0.0))
    for r in res_lora:
        sums[2] += float(r.get("score", 0.0))

    avg = {
        k: (sums[k] / counts[k] if counts[k] > 0 else 0.0)
        for k in sums.keys()
    }

    # кто выиграл
    label_map = {0: "draft", 1: "teacher", 2: "lora"}
    best_k = max(avg, key=avg.get)
    best_label = label_map[best_k]

    print("\n========== 📊 ОЦЕНКА ЧЕРЕЗ JUDGE ==========")
    print(f"Черновики (draft):   n={counts[0]}  avg_score={avg[0]:.2f}")
    print(f"Teacher (референсы): n={counts[1]}  avg_score={avg[1]:.2f}")
    print(f"LoRA-выход:          n={counts[2]}  avg_score={avg[2]:.2f}")
    print("-------------------------------------------")
    print(f"🥇 Лучший по средней оценке: {best_label}")
    print(
        f"Δ(LoRA - teacher) = {avg[2] - avg[1]:+.2f}   |   Δ(LoRA - draft) = {avg[2] - avg[0]:+.2f}"
    )
    print("===========================================\n")

    # ------ Примеры для просмотра (teacher vs LoRA) ------
    max_examples = min(3, n)
    if max_examples > 0:
        print("------ Примеры вывода (teacher vs LoRA) ------")
    for i in range(max_examples):
        s_draft = float(res_draft[i].get("score", 0.0))
        s_ref = float(res_ref[i].get("score", 0.0))
        s_lora = float(res_lora[i].get("score", 0.0))

        print(f"\n=== Пример {i+1} ===")
        print(
            f"Оценки judge: draft={s_draft:.1f} | teacher={s_ref:.1f} | lora={s_lora:.1f}"
        )

        ref_text = refs[i].strip()
        lora_text = loras[i].strip()

        # чтобы не спамить консоль — режем по ~600 символов
        def cut(t: str, max_len: int = 600) -> str:
            return t if len(t) <= max_len else t[:max_len] + "...\n[обрезано]"

        print("\n--- Teacher (референс) ---")
        print(cut(ref_text))

        print("\n--- LoRA OUTPUT ---")
        print(cut(lora_text))

    if max_examples > 0:
        print("\n------ Конец примеров ------\n")


def main():
    print(f"[{datetime.now().isoformat()}] 🔍 Тестируем LoRA-Writer на val + judge_quality_llm")
    print(f"[{datetime.now().isoformat()}] VAL_PATH = {VAL_PATH}")

    examples = load_val_examples(limit=VAL_LIMIT)
    if not examples:
        print("❌ В val-датасете нет примеров. Проверь WRITER_VAL_PATH / формат файла.")
        return

    tokenizer, lora_model, device = load_writer_lora()

    drafts: List[str] = []
    refs: List[str] = []
    loras: List[str] = []

    total = len(examples)
    print(f"[{datetime.now().isoformat()}] 🔄 Генерируем ответы LoRA для {total} примеров...")

    for idx, ex in enumerate(examples, start=1):
        system_text = ex["system"]
        user_text = ex["user"]
        draft = ex["draft"]
        ref = ex["ref"]

        lora_out = generate_with_lora(
            tokenizer,
            lora_model,
            device,
            system_text,
            user_text,
        )

        drafts.append(draft)
        refs.append(ref)
        loras.append(lora_out)

        if idx % 5 == 0 or idx == total:
            print(
                f"[{datetime.now().isoformat()}] Прогресс генерации LoRA: {idx}/{total}"
            )

    evaluate_with_judge(drafts, refs, loras)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Остановлено пользователем.")
