from __future__ import annotations

import os
import pathlib
from typing import Tuple

import torch
from dotenv import load_dotenv
from transformers import AutoModelForCausalLM, AutoTokenizer

try:
    from transformers import BitsAndBytesConfig
except Exception:
    BitsAndBytesConfig = None

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
if ENV_PATH.exists():
    load_dotenv(str(ENV_PATH))


def _resolve_model_name() -> str:
    """Resolve model name/path from env or local fallback."""
    env_name = os.getenv("BASE_MODEL")
    if env_name:
        return env_name

    local_dir = BASE_DIR / "qwen2.5-7b-instruct"
    if local_dir.exists():
        return str(local_dir)

    return "Qwen/Qwen2.5-7B-Instruct"


def _build_quant_config():
    """Build 4-bit quantization config when bitsandbytes is available."""
    if BitsAndBytesConfig is None:
        return None

    compute_dtype = torch.bfloat16 if torch.cuda.is_available() else torch.float32
    return BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_compute_dtype=compute_dtype,
        bnb_4bit_use_double_quant=True,
        bnb_4bit_quant_type="nf4",
    )


def load_tokenizer_model() -> Tuple[AutoTokenizer, AutoModelForCausalLM]:
    """Load tokenizer and model for inference/training."""
    model_name = _resolve_model_name()
    print(f"[qwen_loader] BASE_MODEL={model_name}")

    quant_config = _build_quant_config()

    tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
    if tokenizer.pad_token_id is None:
        tokenizer.pad_token = tokenizer.eos_token
    tokenizer.padding_side = "left"

    model_kwargs = {
        "trust_remote_code": True,
        "device_map": "auto",
    }

    if quant_config is not None:
        model_kwargs["quantization_config"] = quant_config
    elif torch.cuda.is_available():
        model_kwargs["torch_dtype"] = torch.bfloat16

    model = AutoModelForCausalLM.from_pretrained(model_name, **model_kwargs)
    return tokenizer, model


if __name__ == "__main__":
    from datetime import datetime

    print(f"[{datetime.now().isoformat()}] Running qwen_loader self-check")
    tokenizer, model = load_tokenizer_model()

    try:
        device = model.device
    except Exception:
        params = list(model.parameters())
        device = params[0].device if params else "cpu"

    print(f"[{datetime.now().isoformat()}] Model loaded successfully")
    print(f"  - device: {device}")
    print(f"  - pad_token_id: {tokenizer.pad_token_id}")
    print(f"  - vocab_size: {model.config.vocab_size}")
