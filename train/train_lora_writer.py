# train/train_lora_writer.py
# Обучение LoRA для "писателя" на Qwen2.5-7B-Instruct
# по датасету writer_sft_dataset*.jsonl (формат: {"messages": [...]})

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

# ======== Путь к датасету ========
# 1) WRITER_DATASET_PATH (как у тебя в .env)
# 2) WRITER_DATA_PATH (старое имя, на всякий случай)
# 3) дефолт в ./data
DATA_PATH = (
    os.getenv("WRITER_DATASET_PATH")
    or os.getenv("WRITER_DATA_PATH")
    or os.path.join(BASE_DIR, "data", "writer_sft_dataset.jsonl")
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
print(f"  DATA_PATH  = {DATA_PATH}")
print(f"  OUTPUT_DIR = {OUTPUT_DIR}")
print(f"  BASE_MODEL = {BASE_MODEL}")
print("=====================================\n")

if not os.path.exists(DATA_PATH):
    raise FileNotFoundError(f"Не найден датасет: {DATA_PATH}")

# ================== ЗАГРУЗАЕМ ДАТАСЕТ ==================
# Структура:
# {
#   "messages": [
#     {"role": "system", "content": "..."},
#     {"role": "user", "content": "..."},
#     {"role": "assistant", "content": "..."}
#   ]
# }
raw_dataset = load_dataset(
    "json",
    data_files={"train": DATA_PATH},
)

# ================== ЗАГРУЗКА ТОКЕНАЙЗЕРА И МОДЕЛИ ==================
print(f"[{datetime.now().isoformat()}] 🔄 Загружаем токенайзер и модель...")

tokenizer = AutoTokenizer.from_pretrained(
    BASE_MODEL,
    trust_remote_code=True,
)

if tokenizer.pad_token is None:
    tokenizer.pad_token = tokenizer.eos_token

quant_config = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_use_double_quant=True,
    bnb_4bit_quant_type="nf4",
    bnb_4bit_compute_dtype=torch.bfloat16,
)

model = AutoModelForCausalLM.from_pretrained(
    BASE_MODEL,
    trust_remote_code=True,
    quantization_config=quant_config,
    device_map="auto",
)

model = prepare_model_for_kbit_training(model)

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

    enc["labels"] = enc["input_ids"].copy()
    return enc

tokenized = raw_dataset.map(
    tokenize_fn,
    batched=False,
    remove_columns=raw_dataset["train"].column_names,
)

# ================== ТРЕНИРОВКА ==================
train_args = TrainingArguments(
    output_dir=OUTPUT_DIR,
    per_device_train_batch_size=1,
    gradient_accumulation_steps=8,
    num_train_epochs=3,
    learning_rate=2e-4,
    logging_steps=5,
    save_steps=50,
    save_total_limit=3,
    bf16=True,
    optim="paged_adamw_8bit",
    lr_scheduler_type="cosine",
    warmup_ratio=0.03,
    report_to="none",
)

trainer = Trainer(
    model=model,
    args=train_args,
    train_dataset=tokenized["train"],
    tokenizer=tokenizer,
)

if __name__ == "__main__":
    trainer.train()
    trainer.save_model(OUTPUT_DIR)
    tokenizer.save_pretrained(OUTPUT_DIR)
    print(f"[{datetime.now().isoformat()}] ✅ Обучение LoRA завершено, чекпоинт: {OUTPUT_DIR}")
