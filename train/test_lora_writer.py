# train/test_lora_writer.py
# Тест локальной LoRA-писателя на Qwen2.5-7B-Instruct

import os
import sys
from datetime import datetime
from typing import List, Dict, Any

import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from peft import PeftModel
from dotenv import load_dotenv
import pathlib

# ================== БАЗОВЫЕ ПУТИ ==================
BASE_DIR = str(pathlib.Path(__file__).resolve().parents[1])
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

load_dotenv(os.path.join(BASE_DIR, ".env"))

# ======== ПУТИ К МОДЕЛИ И ЛОРЕ ========
DEFAULT_LOCAL_QWEN = os.path.join(BASE_DIR, "Models", "qwen2.5-7b-instruct")
BASE_MODEL = os.getenv("QWEN_LOCAL_PATH", DEFAULT_LOCAL_QWEN)

DEFAULT_LORA_DIR = os.path.join(BASE_DIR, "checkpoints", "lora_writer_qwen2_5_7b")
LORA_DIR = os.getenv("LORA_WRITER_OUTPUT", DEFAULT_LORA_DIR)

print(f"[{datetime.now().isoformat()}] 🔧 TEST CONFIG:")
print(f"  BASE_MODEL = {BASE_MODEL}")
print(f"  LORA_DIR   = {LORA_DIR}")
print("=====================================\n")

if not os.path.isdir(BASE_MODEL):
    raise FileNotFoundError(f"Базовая модель не найдена по пути: {BASE_MODEL}")

if not os.path.isdir(LORA_DIR):
    raise FileNotFoundError(f"Папка с LoRA-чекпоинтом не найдена: {LORA_DIR}")

# ================== ПРОМПТЫ, КАК В ДАТАСЕТЕ ==================

WRITER_SYSTEM_MSG = (
    "Ты — автор постов для Telegram-канала по крипте и IT. "
    "Пишешь ясно, по-деловому, без воды и кликбейта. "
    "Стиль: живой, но аккуратный, без токсичности и без фейков. "
    "Опирайся на тему и цель поста, следи за структурой и логикой."
)

WRITER_USER_TEMPLATE = (
    "Канал: {channel}\n"
    "Цель: {goal}\n\n"
    "Фактура (краткий бриф по теме):\n"
    "\"\"\"\n{brief}\n\"\"\"\n\n"
    "Напиши финальный пост для Telegram-канала."
)


def build_messages(channel: str, goal: str, brief: str) -> List[Dict[str, str]]:
    user_content = WRITER_USER_TEMPLATE.format(
        channel=channel or "не указан",
        goal=goal.strip(),
        brief=brief.strip(),
    )
    return [
        {"role": "system", "content": WRITER_SYSTEM_MSG},
        {"role": "user", "content": user_content},
    ]


# ================== ЗАГРУЗКА МОДЕЛИ + LORA ==================

def load_lora_model():
    print(f"[{datetime.now().isoformat()}] 🔄 Загружаем токенайзер...")
    tokenizer = AutoTokenizer.from_pretrained(
        BASE_MODEL,
        trust_remote_code=True,
    )
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    print(f"[{datetime.now().isoformat()}] 🔄 Загружаем базовую модель (4bit)...")
    quant_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_use_double_quant=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16,
    )

    base_model = AutoModelForCausalLM.from_pretrained(
        BASE_MODEL,
        trust_remote_code=True,
        quantization_config=quant_config,
        device_map="auto",
    )

    print(f"[{datetime.now().isoformat()}] 🔄 Навешиваем LoRA из {LORA_DIR}...")
    model = PeftModel.from_pretrained(
        base_model,
        LORA_DIR,
    )
    model.eval()

    try:
        device = model.device
    except Exception:
        params = list(model.parameters())
        device = params[0].device if params else torch.device("cpu")

    print(f"[{datetime.now().isoformat()}] ✅ Модель с LoRA загружена на {device}")
    return tokenizer, model, device


def generate_post(
    tokenizer,
    model,
    device,
    channel: str,
    goal: str,
    brief: str,
    max_new_tokens: int = 256,
) -> str:
    messages = build_messages(channel, goal, brief)

    # Собираем input через chat template Qwen
    prompt_text = tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=True,  # просим модель продолжить ответ ассистента
    )

    inputs = tokenizer(
        prompt_text,
        return_tensors="pt",
    ).to(device)

    with torch.inference_mode():
        output_ids = model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            do_sample=False,
            pad_token_id=tokenizer.eos_token_id,
            eos_token_id=tokenizer.eos_token_id,
        )

    gen_ids = output_ids[0][inputs["input_ids"].shape[-1]:]
    gen_text = tokenizer.decode(gen_ids, skip_special_tokens=True)
    return gen_text.strip()


if __name__ == "__main__":
    print(f"[{datetime.now().isoformat()}] 🧪 Тест LoRA-писателя...")

    tokenizer, model, device = load_lora_model()

    # ==== ТЕСТОВЫЙ ПРИМЕР ====
    test_channel = "toncoin_rus"
    test_goal = "Кратко рассказать о новой интеграции TON и USDT и показать пользу для обычных пользователей."
    test_brief = (
        "- В Африке пользователи получили доступ к USDT через TON-кошелёк.\n"
        "- Теперь можно быстро отправлять стабильные доллары без банков.\n"
        "- Комиссии ниже, переводы проходят за секунды.\n"
        "- Это важно там, где местные валюты нестабильны и есть ограничения по доллару."
    )

    print(f"\n[INPUT] Канал: {test_channel}")
    print(f"[INPUT] Цель: {test_goal}")
    print(f"[INPUT] Бриф:\n{test_brief}\n")

    out = generate_post(
        tokenizer=tokenizer,
        model=model,
        device=device,
        channel=test_channel,
        goal=test_goal,
        brief=test_brief,
        max_new_tokens=256,
    )

    print("\n[OUTPUT] Сгенерированный пост LoRA:\n")
    print(out)
    print("\n✅ Тест завершён.")
