#!/usr/bin/env python3
"""
Единый пайплайн:

1) Парсер Telegram  → пишет посты в БД
2) Оценка постов    → judge_quality_llm
3) Автофил          → формирование writer_samples
4) Экспорт          → JSONL для обучения LoRA-writer:
   - отдельный файл для постов
   - отдельный файл для челленджей
"""

import sys
import subprocess
from pathlib import Path

from dotenv import load_dotenv

# ============================================
# КОНФИГ: пути к скриптам
# ============================================

# run_pipeline.py лежит в: .../EngageX/automatika/run_pipeline.py
# => корень проекта: .../EngageX
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# парсер Telegram
PARSER_SCRIPT = "parser/parser.py"

# LLM-джадж
JUDGE_SCRIPT = "analytics/judge_quality_llm.py"

# автофил writer_samples
AUTOFILL_SCRIPT = "analytics/autofill_writer_samples.py"

# экспорт датасета (универсальный: post / challenge)
EXPORT_SCRIPT = "analytics/export_posts_writer_dataset.py"

# ============================================

ENV_PATH = PROJECT_ROOT / ".env"
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)


def run_step(name: str, cmd: list[str]) -> None:
    """Запускает один шаг пайплайна и падает при ошибке."""
    print("\n" + "=" * 80)
    print(f"▶ {name}")
    print("   CMD:", " ".join(cmd))
    print("=" * 80)

    # ВАЖНО: запускаем из корня проекта (EngageX)
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)

    if result.returncode != 0:
        print(f"\n❌ Шаг '{name}' завершился с ошибкой (код {result.returncode}). Пайплайн остановлен.")
        sys.exit(result.returncode)

    print(f"✅ Шаг '{name}' выполнен успешно.")


def main() -> None:
    # 1. Парсер Telegram
    run_step(
        "Парсер Telegram",
        [sys.executable, PARSER_SCRIPT]
    )

    # 2. Оценка постов (LLM-judge)
    run_step(
        "Оценка постов (LLM-judge)",
        [sys.executable, JUDGE_SCRIPT]
    )

    # 3. Автофил: заполнение writer_samples
    run_step(
        "Автозаполнение writer_samples (autofill)",
        [sys.executable, AUTOFILL_SCRIPT]
    )

    # 4. Экспорт датасетов для LoRA-writer
    out_dir = PROJECT_ROOT / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    posts_out_path = out_dir / "writer_sft_posts_manual.jsonl"
    challenges_out_path = out_dir / "writer_sft_challenges_manual.jsonl"

    # 4.1 Посты
    run_step(
        "Экспорт SFT-датасета для LoRA-writer (посты)",
        [
            sys.executable,
            EXPORT_SCRIPT,
            "--out",
            str(posts_out_path),
            "--sample-type",
            "post",
            # "--limit", "1000",
        ]
    )

    # 4.2 Челленджи
    run_step(
        "Экспорт SFT-датасета для LoRA-writer (челленджи)",
        [
            sys.executable,
            EXPORT_SCRIPT,
            "--out",
            str(challenges_out_path),
            "--sample-type",
            "challenge",
            # "--limit", "1000",
        ]
    )

    print("\n🎯 Пайплайн полностью завершён.")
    print(f"   Датасет постов:       {posts_out_path}")
    print(f"   Датасет челленджей:   {challenges_out_path}")


if __name__ == "__main__":
    main()
