

import os
import sys
import json
import random
from datetime import datetime
import pathlib

BASE_DIR = str(pathlib.Path(__file__).resolve().parents[1])
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

DATA_PATH = os.path.join(BASE_DIR, "data", "writer_train.jsonl")
TRAIN_PATH = os.path.join(BASE_DIR, "data", "writer_rewrite_train.jsonl")
VAL_PATH   = os.path.join(BASE_DIR, "data", "writer_rewrite_val.jsonl")

SPLIT_RATIO = 0.8
SEED = 42

def main():
    print(f"[{datetime.now().isoformat()}] 📂 Читаем датасет: {DATA_PATH}")
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"Файл не найден: {DATA_PATH}")

    samples = []
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            samples.append(json.loads(line))

    n = len(samples)
    print(f"[{datetime.now().isoformat()}] Найдено {n} примеров.")

    random.seed(SEED)
    random.shuffle(samples)

    train_size = int(n * SPLIT_RATIO)
    train_samples = samples[:train_size]
    val_samples   = samples[train_size:]

    print(f"[{datetime.now().isoformat()}] Train: {len(train_samples)}, Val: {len(val_samples)}")

    with open(TRAIN_PATH, "w", encoding="utf-8") as f:
        for s in train_samples:
            f.write(json.dumps(s, ensure_ascii=False) + "\n")

    with open(VAL_PATH, "w", encoding="utf-8") as f:
        for s in val_samples:
            f.write(json.dumps(s, ensure_ascii=False) + "\n")

    print(f"[{datetime.now().isoformat()}] ✅ Записаны:\n  • {TRAIN_PATH}\n  • {VAL_PATH}")

if __name__ == "__main__":
    main()
