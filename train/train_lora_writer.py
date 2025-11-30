# train/train_lora_writer.py
# Обучение LoRA для "писателя" на Qwen2.5-7B-Instruct
# по датасету, экспортированному из writer_challenges.
#
# Формат строки в JSONL (writer_train.jsonl):
# {
#   "messages": [
#       {"role": "system", "content": WRITER_SYSTEM_MSG},
#       {"role": "user", "content": "Канал: ...\nЦель недели: ...\nСтиль: ...\n..."},
#       {"role": "assistant", "content": "<финальный текст челленджа>"}
#   ]
# }
#
# Стиль уже зашит в user-промпт строкой "Стиль: ...", поэтому
# отдельно поле style здесь не нужно — мы просто учим модель
# на этих диалогах.

import os
import sys
from typing import Dict, Any
from datetime import datetime

import torch
from datasets import load_dataset
from peft import LoraConfig, get_peft_model, prepare_model_for_kbit_training
from transformers import (
    AutoTokenizer,
    AutoModelForCausalLM,
    TrainingArguments,
    Trainer,
    BitsAndBytesConfig,
)
from dotenv import load_dotenv
import pathlib

# ================== БАЗОВЫЕ ПУТИ ==================
BASE_DIR = str(pathlib.Path(__file__).resolve().parents[1])
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

load_dotenv(os.path.join(BASE_DIR, ".env"))

# ======== РЕЖИМ ОБУЧЕНИЯ: safe / max_vram ========
TRAIN_MODE = os.getenv("WRITER_TRAIN_MODE", "safe").lower()
if TRAIN_MODE not in ("safe", "max_vram"):
    TRAIN_MODE = "safe"

# ======== Путь к датасету ========
# Приоритет:
# 1) WRITER_DATASET_PATH
# 2) WRITER_DATA_PATH (старое имя)
# 3) дефолт: ./data/writer_train.jsonl (совпадает с экспортом)
DATA_PATH = (
    os.getenv("WRITER_DATASET_PATH")
    or os.getenv("WRITER_DATA_PATH")
    or os.path.join(BASE_DIR, "data", "writer_train.jsonl")
)

# ======== Путь к локальной модели Qwen ========
DEFAULT_LOCAL_QWEN = os.path.join(BASE_DIR, "Models", "qwen2.5-7b-instruct")
BASE_MODEL = os.getenv("QWEN_LOCAL_PATH", DEFAULT_LOCAL_QWEN)

# ======== Куда сохраняем LoRA ========
OUTPUT_DIR = os.getenv(
    "LORA_WRITER_OUTPUT",
    os.path.join(BASE_DIR, "checkpoints", "lora_writer_qwen2_5_7b"),
)

os.makedirs(os.path.dirname(OUTPUT_DIR), exist_ok=True)

print(f"[{datetime.now().isoformat()}] 🔧 CONFIG:")
print(f"  DATA_PATH         = {DATA_PATH}")
print(f"  OUTPUT_DIR        = {OUTPUT_DIR}")
print(f"  BASE_MODEL        = {BASE_MODEL}")
print(f"  WRITER_TRAIN_MODE = {TRAIN_MODE}")
print("=====================================\n")

if not os.path.exists(DATA_PATH):
    raise FileNotFoundError(f"Не найден датасет: {DATA_PATH}")

# ================== ЗАГРУЗАЕМ ДАТАСЕТ ==================
print(f"[{datetime.now().isoformat()}] 📂 Загружаем датасет...")
raw_dataset = load_dataset(
    "json",
    data_files={"train": DATA_PATH},
)

train_dataset = raw_dataset["train"]
full_len = len(train_dataset)
print(f"[{datetime.now().isoformat()}] 📊 Всего train-сэмплов: {full_len}\n")

if full_len == 0:
    raise RuntimeError("Датасет пуст. Проверь writer_train.jsonl.")

# ================== ЗАГРУЗКА ТОКЕНАЙЗЕРА И МОДЕЛИ ==================
print(f"[{datetime.now().isoformat()}] 🔄 Загружаем токенайзер и модель...")

tokenizer = AutoTokenizer.from_pretrained(
    BASE_MODEL,
    trust_remote_code=True,
)

if tokenizer.pad_token is None:
    tokenizer.pad_token = tokenizer.eos_token

pad_id = tokenizer.pad_token_id

# 4-bit QLoRA под 3090: считаем в float16
quant_config = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_use_double_quant=True,
    bnb_4bit_quant_type="nf4",
    bnb_4bit_compute_dtype=torch.float16,
)

model = AutoModelForCausalLM.from_pretrained(
    BASE_MODEL,
    trust_remote_code=True,
    quantization_config=quant_config,
    device_map="auto",
)

model = prepare_model_for_kbit_training(model)

# Немного конфигурации
model.config.pad_token_id = pad_id
model.config.use_cache = False  # чтобы Trainer не ругался, нормально для тренировки

# ================== LoRA ==================
lora_config = LoraConfig(
    r=16,
    lora_alpha=32,
    target_modules=[
        "q_proj",
        "k_proj",
        "v_proj",
        "o_proj",
        "gate_proj",
        "up_proj",
        "down_proj",
    ],
    lora_dropout=0.05,
    bias="none",
    task_type="CAUSAL_LM",
)

model = get_peft_model(model, lora_config)
model.print_trainable_parameters()

# ================== ТОКЕНИЗАЦИЯ ==================
MAX_LEN = int(os.getenv("WRITER_MAX_LEN", "1024"))


def tokenize_fn(example: Dict[str, Any]) -> Dict[str, Any]:
    """
    Берём уже собранный список messages (system + user + assistant),
    применяем chat_template и токенизируем.
    Стиль уже присутствует внутри user-сообщения в виде строки "Стиль: ...",
    поэтому здесь дополнительно ничего добавлять не нужно.
    """
    messages = example["messages"]

    text = tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=False,
    )

    enc = tokenizer(
        text,
        max_length=MAX_LEN,
        padding="max_length",
        truncation=True,
        return_attention_mask=True,
    )

    # Для SFT: предсказываем весь текст
    enc["labels"] = enc["input_ids"].copy()
    return enc


print(f"[{datetime.now().isoformat()}] ✂️ Токенизируем датасет...")
tokenized_train = train_dataset.map(
    tokenize_fn,
    batched=False,
    remove_columns=train_dataset.column_names,
)

# ================== НАСТРОЙКИ ПОД РЕЖИМ ==================

if TRAIN_MODE == "safe":
    per_device_bs = 2
    grad_acc_steps = 8          # эффективный batch ≈ 16
    num_epochs = 3
    use_gradient_checkpointing = True
else:  # max_vram
    per_device_bs = 6           # можно поджать до 5, если будет OOM
    grad_acc_steps = 4          # эффективный batch ≈ 24
    num_epochs = 2
    use_gradient_checkpointing = False

# Gradient checkpointing — только в safe-режиме
if use_gradient_checkpointing:
    try:
        model.gradient_checkpointing_enable()
    except Exception:
        pass
    try:
        model.enable_input_require_grads()
    except Exception:
        pass

print(
    f"[{datetime.now().isoformat()}] 🧮 TRAIN MODE = {TRAIN_MODE}, "
    f"per_device_bs={per_device_bs}, grad_acc={grad_acc_steps}, "
    f"epochs={num_epochs}, grad_ckpt={use_gradient_checkpointing}"
)

# ================== ТРЕНИРОВКА ==================
train_args = TrainingArguments(
    output_dir=OUTPUT_DIR,
    per_device_train_batch_size=per_device_bs,
    gradient_accumulation_steps=grad_acc_steps,
    num_train_epochs=num_epochs,
    learning_rate=2e-4,
    logging_steps=10,
    fp16=True,                 # 3090 → fp16 ок
    optim="paged_adamw_8bit",
    lr_scheduler_type="cosine",
    warmup_ratio=0.03,
    report_to="none",
    overwrite_output_dir=True,    # учим LoRA "с нуля" в этой папке
    gradient_checkpointing=use_gradient_checkpointing,
    save_strategy="no",           # ❗ НЕ сохраняем промежуточные checkpoint-XXXX
)

trainer = Trainer(
    model=model,
    args=train_args,
    train_dataset=tokenized_train,
    tokenizer=tokenizer,
)

if __name__ == "__main__":
    print(f"[{datetime.now().isoformat()}] 🚀 Старт обучения LoRA на челленджах (writer_challenges)...")
    trainer.train()
    # Сохраняем только финальный вариант
    trainer.save_model(OUTPUT_DIR)
    tokenizer.save_pretrained(OUTPUT_DIR)
    print(f"[{datetime.now().isoformat()}] ✅ Обучение LoRA завершено, финальный чекпоинт: {OUTPUT_DIR}")
