import schedule
import subprocess
import time
import os
from datetime import datetime
import sys

# === Базовая директория проекта ===
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(BASE_DIR)  # чтобы все пути были из корня

# === Логирование ===
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

def log(message: str):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_path = os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{now}] {message}\n")
    print(message)

# === Задачи ===
def run_parser():
    log("🚀 Запуск парсера Telegram...")
    try:
        subprocess.run(["python", "parser/parser.py"], check=True)
        log("✅ Парсер успешно завершён.")
    except subprocess.CalledProcessError as e:
        log(f"❌ Ошибка парсера: {e}")

def run_analytics():
    log("📊 Запуск аналитики вовлечённости...")
    try:
        subprocess.run(["python", "analytics/analyze_engagement.py"], check=True)
        log("✅ Аналитика успешно завершена.")
    except subprocess.CalledProcessError as e:
        log(f"❌ Ошибка аналитики: {e}")

def run_dataset_builder():
    log("📦 Запуск сборки датасета...")
    try:
        subprocess.run(["python", "analytics/dataset_builder.py"], check=True)
        log("✅ Датасет успешно собран.")
    except subprocess.CalledProcessError as e:
        log(f"❌ Ошибка сборки датасета: {e}")

# === Расписание ===
schedule.every().day.at("03:00").do(run_parser)
schedule.every().day.at("03:30").do(run_analytics)
schedule.every().day.at("04:00").do(run_dataset_builder)

log("🕒 Автоматический планировщик EngageX запущен.")
log("   03:00 — парсер")
log("   03:30 — аналитика")
log("   04:00 — сборка датасета")

# === Основной цикл ===
while True:
    schedule.run_pending()
    time.sleep(60)
