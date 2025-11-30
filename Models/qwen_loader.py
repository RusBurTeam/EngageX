# Models/qwen_loader.py
#
# Универсальный лоадер Qwen2.5-7B-Instruct:
# - сначала пытается взять локальную папку модели в проекте
# - при желании можно переопределить через .env (BASE_MODEL=...)
# - грузит в 4-битном режиме через BitsAndBytes (оптимально под LoRA)
# - возвращает (tokenizer, model), как ждут judge_quality_llm и train_lora_writer

from __future__ import annotations

import os
import pathlib
from typing import Tuple

import torch
from dotenv import load_dotenv
from transformers import (
    AutoTokenizer,
    AutoModelForCausalLM,
)

try:
    from transformers import BitsAndBytesConfig
except Exception:
    BitsAndBytesConfig = None  # на всякий случай, но для 4bit лучше установить bitsandbytes

# ----------------------------------------------------
# Базовая директория проекта и .env
# ----------------------------------------------------

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
if ENV_PATH.exists():
    load_dotenv(str(ENV_PATH))


def _resolve_model_name() -> str:
    """
    Определяем, откуда брать модель:

    1) Если в .env задан BASE_MODEL — используем его как путь/название.
    2) Иначе, если в проекте есть локальная папка qwen2.5-7b-instruct — берём её.
    3) Иначе — используем официальный репозиторий на HF: Qwen/Qwen2.5-7B-Instruct.
    """
    env_name = os.getenv("BASE_MODEL")
    if env_name:
        return env_name

    local_dir = BASE_DIR / "qwen2.5-7b-instruct"
    if local_dir.exists():
        return str(local_dir)

    # fallback в интернетный вариант
    return "Qwen/Qwen2.5-7B-Instruct"


def _build_quant_config():
    """
    Собираем конфиг для 4-битной загрузки.
    На 24 ГБ VRAM этого более чем достаточно, плюс остаётся запас под градиенты LoRA.
    """
    if BitsAndBytesConfig is None:
        # Если bitsandbytes не установлен — грузим фулл-precision (может съесть 18–20 ГБ).
        return None

    compute_dtype = torch.bfloat16 if torch.cuda.is_available() else torch.float32

    return BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_compute_dtype=compute_dtype,
        bnb_4bit_use_double_quant=True,
        bnb_4bit_quant_type="nf4",
    )


def load_tokenizer_model() -> Tuple[AutoTokenizer, AutoModelForCausalLM]:
    """
    Главный хелпер, который вызывают judge_quality_llm и train_lora_writer.

    Возвращает:
        tokenizer, model
    """
    model_name = _resolve_model_name()
    print(f"[qwen_loader] ⚙️  BASE_MODEL = {model_name}")

    quant_config = _build_quant_config()

    # --- токенайзер ---
    tokenizer = AutoTokenizer.from_pretrained(
        model_name,
        trust_remote_code=True,
    )

    # нормализуем пад-токен, чтобы не было предупреждений
    if tokenizer.pad_token_id is None:
        # у Qwen часто eos и есть нормальный вариант пад-токена
        tokenizer.pad_token = tokenizer.eos_token

    tokenizer.padding_side = "left"

    # --- модель ---
    model_kwargs = dict(
        trust_remote_code=True,
        device_map="auto",
    )

    if quant_config is not None:
        # 4-битный режим через BitsAndBytes (рекомендуется для LoRA)
        model_kwargs["quantization_config"] = quant_config
    else:
        # без квантования — лучше сразу в bfloat16/fp16, иначе будет жирный fp32
        if torch.cuda.is_available():
            model_kwargs["torch_dtype"] = torch.bfloat16

    model = AutoModelForCausalLM.from_pretrained(
        model_name,
        **model_kwargs
    )

    return tokenizer, model

if __name__ == "__main__":
    # Простой самотест: грузим модель и выводим инфу
    from datetime import datetime

    print(f"[{datetime.now().isoformat()}] 🔍 Тестовый запуск qwen_loader")
    tokenizer, model = load_tokenizer_model()

    try:
        device = model.device
    except Exception:
        params = list(model.parameters())
        device = params[0].device if params else "cpu"

    print(f"[{datetime.now().isoformat()}] ✅ Модель загружена")
    print(f"  • device: {device}")
    print(f"  • pad_token_id: {tokenizer.pad_token_id}")
    print(f"  • vocab_size: {model.config.vocab_size}")
