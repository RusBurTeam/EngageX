import os
import subprocess
import time
from datetime import datetime

import schedule

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(BASE_DIR)

LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)


def log(message: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_path = os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{now}] {message}\n")
    print(message)


def run_parser() -> None:
    log("[start] Running Telegram parser...")
    try:
        subprocess.run(["python", "parser/parser.py"], check=True)
        log("[ok] Telegram parser completed")
    except subprocess.CalledProcessError as exc:
        log(f"[error] Telegram parser failed: {exc}")


def run_analytics() -> None:
    log("[start] Running engagement analytics...")
    try:
        subprocess.run(["python", "analytics/analyze_engagement.py"], check=True)
        log("[ok] Analytics completed")
    except subprocess.CalledProcessError as exc:
        log(f"[error] Analytics failed: {exc}")


def run_dataset_builder() -> None:
    log("[start] Building dataset...")
    try:
        subprocess.run(["python", "analytics/dataset_builder.py"], check=True)
        log("[ok] Dataset build completed")
    except subprocess.CalledProcessError as exc:
        log(f"[error] Dataset build failed: {exc}")


schedule.every().day.at("03:00").do(run_parser)
schedule.every().day.at("03:30").do(run_analytics)
schedule.every().day.at("04:00").do(run_dataset_builder)

log("[start] EngageX scheduler started")
log("  03:00 - parser")
log("  03:30 - analytics")
log("  04:00 - dataset builder")

while True:
    schedule.run_pending()
    time.sleep(60)
