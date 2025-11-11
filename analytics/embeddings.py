# embeddings.py
"""
Мини-модуль для смысловых эмбеддингов текста.
Модель: BAAI/bge-m3 (мультиязычная, нормализованные эмбеддинги).
GPU: автоматически, если доступен.
"""

from __future__ import annotations
from typing import List
import os
import torch
import numpy as np
from sentence_transformers import SentenceTransformer

# --- конфиг ---
MODEL_NAME = os.getenv("EMB_MODEL", "BAAI/bge-m3")
BATCH_SIZE = int(os.getenv("EMB_BATCH", "64"))
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

# ленивый синглтон модели
_model: SentenceTransformer | None = None

def _get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        _model = SentenceTransformer(MODEL_NAME, device=DEVICE)
    return _model

@torch.inference_mode()
def embed_texts(texts: List[str]) -> np.ndarray:
    """
    Возвращает массив (N, D) c unit-нормированными эмбеддингами.
    Пустые/очень короткие строки фильтруются.
    """
    if not texts:
        return np.zeros((0, 0), dtype=np.float32)

    clean = [(t or "").strip() for t in texts]
    mask = [len(t) >= 3 for t in clean]
    if not any(mask):
        return np.zeros((len(texts), 0), dtype=np.float32)

    model = _get_model()
    vecs = model.encode(
        [t for t, m in zip(clean, mask) if m],
        batch_size=BATCH_SIZE,
        normalize_embeddings=True,
        convert_to_numpy=True,
        show_progress_bar=False,
    )

    # Вставляем пустые эмбеддинги на места отфильтрованных строк
    dim = vecs.shape[1]
    out = np.zeros((len(texts), dim), dtype=np.float32)
    j = 0
    for i, m in enumerate(mask):
        if m:
            out[i] = vecs[j]
            j += 1
        else:
            # пустая/короткая строка → нулевой вектор
            out[i] = 0.0
    return out

if __name__ == "__main__":
    demo = ["Поделитесь лайфхаком продуктивности", "ок", "", "Как вы пользуетесь тайм-блокингом?"]
    V = embed_texts(demo)
    print("shape:", V.shape)
    print("norms:", np.linalg.norm(V, axis=1))
