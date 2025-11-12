# Models/qwen_loader.py
# Задача: гарантировать наличие модели локально (при необходимости докачать)
# и отдать готовые tokenizer+model с корректной квантовкой (4/8 бит) без deprecated флагов.

from __future__ import annotations
import os
from typing import Tuple
from dotenv import load_dotenv

import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from huggingface_hub import snapshot_download

# ---- ENV / PATHS ----
BASE_DIR = os.path.dirname(os.path.abspath(os.path.join(__file__, "..")))
load_dotenv(os.path.join(BASE_DIR, ".env"))

MODEL_ID = os.getenv("JUDGE_MODEL", "Qwen/Qwen2.5-7B-Instruct")
# Папка, куда кладём веса локально (по умолчанию Models/qwen2.5-7b-instruct)
MODEL_DIR = os.getenv(
    "JUDGE_MODEL_DIR",
    os.path.join(BASE_DIR, "Models", "qwen2.5-7b-instruct")
)

# 1 = всегда грузить из локальной папки (без сети, если уже скачано)
MODEL_LOCAL_ONLY = os.getenv("JUDGE_MODEL_LOCAL_ONLY", "0") == "1"
# 1 = 8-бит (экономия VRAM), 0 = bfloat16/float32
USE_8BIT = os.getenv("JUDGE_8BIT", "1") == "1"

def ensure_model_locally() -> str:
    """
    Гарантирует, что MODEL_ID скачана в MODEL_DIR (resume докачки).
    Возвращает локальный путь к папке модели.
    """
    os.makedirs(MODEL_DIR, exist_ok=True)

    # Если уже есть tokenizer/config или хотя бы один шард — считаем, что модель локально присутствует.
    has_any = any(
        os.path.exists(os.path.join(MODEL_DIR, name))
        for name in ("config.json", "tokenizer.json")
    ) or any(
        f.startswith("model-") and f.endswith(".safetensors")
        for f in os.listdir(MODEL_DIR)
    )

    if has_any and MODEL_LOCAL_ONLY:
        return MODEL_DIR

    if not has_any:
        # Первая загрузка или чистая папка — делаем snapshot_download в MODEL_DIR.
        snapshot_download(
            repo_id=MODEL_ID,
            local_dir=MODEL_DIR,
            local_dir_use_symlinks=False,  # чтобы всё лежало реально в MODEL_DIR
            resume_download=True,
            max_workers=4,                 # умеренный параллелизм
        )
    else:
        # Додокачка на случай прерванных шардов
        snapshot_download(
            repo_id=MODEL_ID,
            local_dir=MODEL_DIR,
            local_dir_use_symlinks=False,
            resume_download=True,
            max_workers=4,
        )

    return MODEL_DIR

def load_tokenizer_model() -> Tuple[AutoTokenizer, AutoModelForCausalLM]:
    """
    Возвращает (tokenizer, model) из локальной папки MODEL_DIR.
    Корректно настраивает 8-бит/16-бит через BitsAndBytesConfig.
    """
    local_path = ensure_model_locally()

    device = "cuda" if torch.cuda.is_available() else "cpu"
    dtype = torch.bfloat16 if torch.cuda.is_available() else torch.float32

    quant_cfg = None
    if device == "cuda" and USE_8BIT:
        quant_cfg = BitsAndBytesConfig(load_in_8bit=True)
    elif device == "cuda":
        quant_cfg = BitsAndBytesConfig(load_in_4bit=False)  # явное отключение 4бит

    tok = AutoTokenizer.from_pretrained(local_path, use_fast=True, local_files_only=True)

    if quant_cfg is not None:
        model = AutoModelForCausalLM.from_pretrained(
            local_path,
            quantization_config=quant_cfg,
            device_map="auto",
            local_files_only=True,
        ).eval()
    else:
        model = AutoModelForCausalLM.from_pretrained(
            local_path,
            torch_dtype=dtype,
            device_map="auto" if device == "cuda" else None,
            local_files_only=True,
        ).eval()

    return tok, model

if __name__ == "__main__":
    # Быстрый самотест загрузки
    tok, mdl = load_tokenizer_model()
    print(f"✅ Модель готова: {MODEL_ID} в {MODEL_DIR}")
