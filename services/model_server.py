# services/model_server.py
#
# Универсальный сервис для EngageX:
# - mode="base"   → чистый Qwen2.5-7B-Instruct (диалоги, обсуждение постов)
# - mode="writer" → Qwen + LoRA-писатель (генерация постов/челленджей)
#
# Запуск:
#   uvicorn services.model_server:app --host 0.0.0.0 --port 8001
# или:
#   python services/model_server.py

from __future__ import annotations

import os
from typing import List, Literal

import torch
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from peft import PeftModel
from transformers import AutoTokenizer, AutoModelForCausalLM

from dotenv import load_dotenv
import pathlib

# ===== ЗАГРУЗКА .env И НАСТРОЙКИ ПРОЕКТА =====

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")  # .env лежит в корне EngageX

BASE_MODEL_PATH = os.getenv("BASE_MODEL")
LORA_WRITER_PATH = os.getenv("LORA_WRITER_PATH")

if not BASE_MODEL_PATH:
    raise RuntimeError(
        "BASE_MODEL_PATH не задан в .env. "
        "Добавь строку вида:\nBASE_MODEL_PATH=/full/path/to/qwen2.5-7b-instruct"
    )

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
DTYPE = torch.bfloat16 if torch.cuda.is_available() else torch.float32

# ===== ГЛОБАЛЬНЫЕ ОБЪЕКТЫ =====

tokenizer = None
base_model = None       # чистый Qwen
writer_model = None     # Qwen + LoRA-писатель

app = FastAPI(title="EngageX Model Service", version="1.0.0")


# ===== Pydantic-схемы =====

class Message(BaseModel):
    role: Literal["system", "user", "assistant"]
    content: str


class GenerateRequest(BaseModel):
    # base   → обычный ассистент
    # writer → LoRA-писатель
    mode: Literal["base", "writer"] = "base"
    messages: List[Message]
    max_new_tokens: int = 512
    temperature: float = 0.7
    top_p: float = 0.9
    top_k: int = 50
    repetition_penalty: float = 1.05


class GenerateResponse(BaseModel):
    text: str
    mode: str


# ===== ЗАГРУЗКА МОДЕЛЕЙ =====

def load_models():
    global tokenizer, base_model, writer_model

    print(f"[model_server] Loading tokenizer & base model from: {BASE_MODEL_PATH}")
    tokenizer_local = AutoTokenizer.from_pretrained(
        BASE_MODEL_PATH,
        trust_remote_code=True,
    )

    # ВАЖНО: device_map="auto" + НИКАКИХ .to(...) для модели
    base = AutoModelForCausalLM.from_pretrained(
        BASE_MODEL_PATH,
        torch_dtype=DTYPE,
        device_map="auto",
        trust_remote_code=True,
    )
    base.eval()

    writer = None
    if LORA_WRITER_PATH:
        if not os.path.isdir(LORA_WRITER_PATH):
            raise RuntimeError(
                f"LORA_WRITER_PATH задан, но директории не существует: {LORA_WRITER_PATH}"
            )
        print(f"[model_server] Loading writer LoRA from: {LORA_WRITER_PATH}")
        writer = PeftModel.from_pretrained(
            base,
            LORA_WRITER_PATH,
        )
        writer.eval()
    else:
        print("[model_server] LORA_WRITER_PATH не задан, режим writer будет недоступен")

    tokenizer = tokenizer_local
    base_model = base
    writer_model = writer

    print("[model_server] Models are loaded and ready.")


@app.on_event("startup")
def on_startup():
    load_models()


# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ГЕНЕРАЦИИ =====

def _chat_to_prompt(messages: List[Message]) -> str:
    chat = [{"role": m.role, "content": m.content} for m in messages]
    prompt = tokenizer.apply_chat_template(
        chat,
        tokenize=False,
        add_generation_prompt=True,
    )
    return prompt


def _generate_with_model(model, req: GenerateRequest) -> str:
    prompt = _chat_to_prompt(req.messages)

    # Входы можно переносить на DEVICE, это ок
    inputs = tokenizer(
        prompt,
        return_tensors="pt",
    ).to(DEVICE)

    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=req.max_new_tokens,
            do_sample=True,
            temperature=req.temperature,
            top_p=req.top_p,
            top_k=req.top_k,
            repetition_penalty=req.repetition_penalty,
            pad_token_id=tokenizer.eos_token_id,
        )

    generated_ids = outputs[0][inputs["input_ids"].shape[1]:]
    text = tokenizer.decode(
        generated_ids,
        skip_special_tokens=True,
    )
    return text.strip()


# ===== ЭНДПОИНТЫ =====

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "device": DEVICE,
        "has_writer": writer_model is not None,
    }


@app.post("/generate", response_model=GenerateResponse)
async def generate(req: GenerateRequest):
    if req.mode == "base":
        if base_model is None:
            raise HTTPException(status_code=500, detail="Base model not loaded")
        text = _generate_with_model(base_model, req)
        return GenerateResponse(text=text, mode="base")

    if req.mode == "writer":
        if writer_model is None:
            raise HTTPException(status_code=500, detail="Writer LoRA not loaded")
        text = _generate_with_model(writer_model, req)
        return GenerateResponse(text=text, mode="writer")

    raise HTTPException(status_code=400, detail="Invalid mode")


# ===== ЛОКАЛЬНЫЙ ЗАПУСК =====

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8001,
    )

#python -m uvicorn services.model_server:app --host 0.0.0.0 --port 8001
