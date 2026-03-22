#!/usr/bin/env python3
"""Run the full EngageX data pipeline."""

import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

PARSER_SCRIPT = "parser/parser.py"
JUDGE_SCRIPT = "analytics/judge_quality_llm.py"
AUTOFILL_SCRIPT = "analytics/autofill_writer_samples.py"
EXPORT_SCRIPT = "analytics/export_posts_writer_dataset.py"

ENV_PATH = PROJECT_ROOT / ".env"
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)


def run_step(name: str, cmd: list[str]) -> None:
    """Run one pipeline step and stop on failure."""
    print("\n" + "=" * 80)
    print(f"[step] {name}")
    print("[cmd]", " ".join(cmd))
    print("=" * 80)

    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    if result.returncode != 0:
        print(f"\n[error] Step '{name}' failed with code {result.returncode}. Pipeline stopped.")
        sys.exit(result.returncode)

    print(f"[ok] Step '{name}' completed")


def main() -> None:
    run_step("Telegram parser", [sys.executable, PARSER_SCRIPT])
    run_step("Post quality scoring (LLM judge)", [sys.executable, JUDGE_SCRIPT])
    run_step("Auto-fill writer_samples", [sys.executable, AUTOFILL_SCRIPT])

    out_dir = PROJECT_ROOT / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    posts_out_path = out_dir / "writer_sft_posts_manual.jsonl"
    challenges_out_path = out_dir / "writer_sft_challenges_manual.jsonl"

    run_step(
        "Export SFT dataset for LoRA writer (posts)",
        [
            sys.executable,
            EXPORT_SCRIPT,
            "--out",
            str(posts_out_path),
            "--sample-type",
            "post",
        ],
    )

    run_step(
        "Export SFT dataset for LoRA writer (challenges)",
        [
            sys.executable,
            EXPORT_SCRIPT,
            "--out",
            str(challenges_out_path),
            "--sample-type",
            "challenge",
        ],
    )

    print("\n[done] Pipeline completed successfully")
    print(f"  Posts dataset:      {posts_out_path}")
    print(f"  Challenges dataset: {challenges_out_path}")


if __name__ == "__main__":
    main()
