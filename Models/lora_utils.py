# lora_utils.py
from __future__ import annotations
import os
from typing import List, Tuple, Optional
import importlib
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from peft import prepare_model_for_kbit_training, LoraConfig, get_peft_model
import re
import sys

# ---------- helpers for pretty printing ----------
def _ok(msg: str):
    print(f"\033[92m[ok]\033[0m {msg}")  # зелёный

def _info(msg: str):
    print(f"\033[94m[info]\033[0m {msg}")  # синий

def _warn(msg: str):
    print(f"\033[93m[warn]\033[0m {msg}")  # жёлтый

def _err(msg: str):
    print(f"\033[91m[err]\033[0m {msg}", file=sys.stderr)  # красный

# ---------- core utils ----------
def get_quant_config(use_4bit: bool = True, use_8bit: bool = False):
    _info(f"Создаю quant config: use_4bit={use_4bit}, use_8bit={use_8bit}")
    if use_4bit:
        _info("Используется 4-bit (nf4, double-quant, compute_dtype=float16)")
        return BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_use_double_quant=True,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_compute_dtype=torch.float16,
        )
    if use_8bit:
        _info("Используется 8-bit")
        return BitsAndBytesConfig(load_in_8bit=True)
    _info("Без квантовки (fp16/float32)")
    return None

def detect_target_modules(model) -> List[str]:
    """
    Попытаться автоматически подобрать имена модулей для LoRA.
    Возвращает список candidate module names (подходит для большинства трансформеров).
    """
    _info("Авто-детект target modules (поиск проекций внимания)...")
    names = set()
    for n, m in model.named_modules():
        # типичные имена проекций внимания
        if re.search(r"(\bq_proj\b|\bkv_proj\b|\bk_proj\b|\bv_proj\b|\bo_proj\b|\bquery_key_value\b|\battn\.q_proj\b|\battn\.v_proj\b|\battention\.query\b)", n):
            names.add(n.split(".")[-1])
        # также захватим generic linear layers
        if n.endswith("q") or n.endswith("k") or n.endswith("v") or n.endswith("o") or "query" in n or "key" in n or "value" in n:
            names.add(n.split(".")[-1])
    # common fallback
    fallback = ["q_proj", "v_proj", "o_proj", "k_proj", "query_key_value", "query", "value", "key"]
    # intersect available names with fallback ordering
    result = []
    for f in fallback:
        if f in names and f not in result:
            result.append(f)
    # if nothing found — вернуть common defaults (многие реализации их понимают)
    if not result:
        result = ["q_proj", "v_proj"]
        _warn("Не удалось найти явно имена модулей — возвращаю дефолтные: ['q_proj','v_proj']")
    else:
        _ok(f"Найдено target modules: {result}")
    return result

def load_base_model_and_tokenizer(
    model_id_or_path: str,
    model_dir_local: Optional[str] = None,
    use_4bit: bool = True,
    use_8bit: bool = False,
    trust_remote_code: bool = True,
) -> Tuple[AutoTokenizer, AutoModelForCausalLM]:
    """
    Загружает tokenizer и модель в 4/8/FP16 режиме. Возвращает (tokenizer, model).
    model_dir_local: если указано, попытаться загрузить локальную папку (local_files_only=True) сначала.
    """
    _info(f"load_base_model_and_tokenizer: model='{model_id_or_path}' model_dir_local='{model_dir_local}'")
    quant_config = get_quant_config(use_4bit=use_4bit, use_8bit=use_8bit)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    _info(f"Устройство: {device}")

    load_kwargs = {}
    if quant_config is not None and device == "cuda":
        load_kwargs.update({"quantization_config": quant_config, "device_map": "auto", "trust_remote_code": trust_remote_code})
    else:
        dtype = torch.bfloat16 if (device == "cuda" and hasattr(torch, "bfloat16")) else (torch.float16 if device == "cuda" else torch.float32)
        load_kwargs.update({"torch_dtype": dtype, "device_map": "auto" if device == "cuda" else None, "trust_remote_code": trust_remote_code})

    # tokenizer
    try:
        if model_dir_local:
            _info(f"Загружаем токенизатор локально из: {model_dir_local}")
            tokenizer = AutoTokenizer.from_pretrained(model_dir_local, use_fast=True, trust_remote_code=trust_remote_code, local_files_only=True)
        else:
            _info(f"Загружаем токенизатор с HF: {model_id_or_path}")
            tokenizer = AutoTokenizer.from_pretrained(model_id_or_path, use_fast=True, trust_remote_code=trust_remote_code)
        _ok("Tokenizer загружен")
    except Exception as e:
        _err(f"Не удалось загрузить токенизатор напрямую: {e}")
        _info("Пробую fallback загрузку токенизатора с HF...")
        tokenizer = AutoTokenizer.from_pretrained(model_id_or_path, use_fast=True, trust_remote_code=trust_remote_code)
        _ok("Tokenizer загружен (fallback)")

    # model
    try:
        if model_dir_local:
            _info(f"Загружаем модель локально из: {model_dir_local}")
            model = AutoModelForCausalLM.from_pretrained(model_dir_local, local_files_only=True, **load_kwargs)
        else:
            _info(f"Загружаем модель: {model_id_or_path} (это может занять время)")
            model = AutoModelForCausalLM.from_pretrained(model_id_or_path, **load_kwargs)
        _ok("Модель загружена")
    except Exception as e:
        _err(f"Ошибка загрузки модели (попытка fallback): {e}")
        _info("Пробую загрузку без local_files_only...")
        model = AutoModelForCausalLM.from_pretrained(model_id_or_path, **{k: v for k, v in load_kwargs.items() if k != "local_files_only"})
        _ok("Модель загружена (fallback)")

    _info(f"Модель готова на устройстве: {next(model.parameters()).device if any(True for _ in model.parameters()) else 'cpu'}")
    return tokenizer, model

def prepare_model_for_lora_training(model, r: int = 8, lora_alpha: int = 32, target_modules: Optional[List[str]] = None):
    """
    Подготавливает модель (prepare_model_for_kbit_training) и встраивает LoRA через PEFT.
    Возвращает peft-wrapped model и конфиг LoRA.
    """
    _info("Подготовка модели для k-bit обучения (prepare_model_for_kbit_training)...")
    model = prepare_model_for_kbit_training(model)
    _ok("prepare_model_for_kbit_training завершён")

    if target_modules is None:
        target_modules = detect_target_modules(model)
    _info(f"Используем target_modules = {target_modules}")

    lora_config = LoraConfig(
        r=r,
        lora_alpha=lora_alpha,
        target_modules=target_modules,
        bias="none",
        task_type="CAUSAL_LM"
    )

    _info(f"Создаём LoRA (r={r}, alpha={lora_alpha}) и встраиваем в модель...")
    model = get_peft_model(model, lora_config)
    _ok("LoRA интегрирован в модель (PEFT).")

    # краткая статистика по обучаемым параметрам
    try:
        trainable = sum(p.numel() for p in model.parameters() if p.requires_grad)
        total = sum(p.numel() for p in model.parameters())
        _info(f"Trainable params: {trainable:,} / Total params: {total:,}")
    except Exception:
        _warn("Не удалось посчитать количество параметров (возможно, квантованная модель).")

    return model, lora_config

# ---------- CLI / self-check ----------
def _check_packages(packages: List[str]) -> None:
    _info("Проверка установленных пакетов...")
    for p in packages:
        try:
            importlib.import_module(p)
            _ok(f"Пакет: {p}")
        except Exception as e:
            _warn(f"Пакет отсутствует или ошибка при импорте: {p} -> {e}")

def _torch_info():
    try:
        import torch
        _info(f"torch {torch.__version__}")
        _info(f"cuda_available: {torch.cuda.is_available()}")
        if torch.cuda.is_available():
            try:
                _info(f"cuda_device_count: {torch.cuda.device_count()}")
                _info(f"current_device: {torch.cuda.current_device()}")
                _info(f"device_name: {torch.cuda.get_device_name(torch.cuda.current_device())}")
            except Exception as e:
                _warn(f"Не удалось получить детали CUDA: {e}")
    except Exception as e:
        _err(f"Ошибка при импорте torch: {e}")

if __name__ == "__main__":
    print("="*60)
    print("lora_utils self-check".center(60))
    print("="*60)
    pkgs = ["peft", "accelerate", "bitsandbytes", "datasets", "safetensors", "huggingface_hub", "transformers"]
    _check_packages(pkgs)
    _torch_info()
    print("-"*60)
    _info("Если все пакеты отмечены OK/без ошибок и torch видит CUDA (при наличии GPU), среда готова для тренировки LoRA.")
    _info("Чтобы проверить детект target modules и/или загрузку модели выполните отдельно скрипт check_target_modules.py (он загрузит модель и может занять время).")
    print("="*60)
