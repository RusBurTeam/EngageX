
import os
import pathlib
from typing import List, Optional

from dotenv import load_dotenv

# Ищем .env, поднимаясь вверх от файла config.py
CURRENT_FILE = pathlib.Path(__file__).resolve()
for parent in [CURRENT_FILE.parent] + list(CURRENT_FILE.parents):
    env_path = parent / ".env"
    if env_path.is_file():
        load_dotenv(env_path)
        break


def _clean(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    # убираем пробелы и кавычки
    return value.strip().strip('"').strip("'")


BOT_TOKEN = _clean(os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN"))
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN / TELEGRAM_BOT_TOKEN не задан в .env")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL не задан в .env")

MODEL_SERVER_URL = os.getenv("MODEL_SERVER_URL", "http://127.0.0.1:8001/generate")

# Админы бота (через пробел/запятую)
ADMIN_IDS: List[int] = []
_raw_admin_ids = os.getenv("ADMIN_IDS", "")
for part in _raw_admin_ids.replace(",", " ").split():
    try:
        ADMIN_IDS.append(int(part))
    except ValueError:
        continue

# Канал, куда бот постит челленджи
_raw_channel_id = (os.getenv("CHANNEL_ID") or "").strip()
_raw_channel_username = (os.getenv("CHANNEL_USERNAME") or "").strip()

CHANNEL_CHAT: Optional[int | str] = None

if _raw_channel_id:
    try:
        CHANNEL_CHAT = int(_raw_channel_id)
    except ValueError:
        CHANNEL_CHAT = _raw_channel_id
elif _raw_channel_username:
    if not _raw_channel_username.startswith("@"):
        _raw_channel_username = "@" + _raw_channel_username
    CHANNEL_CHAT = _raw_channel_username

# Username бота (для deep-link'ов в кнопках)
BOT_USERNAME = (os.getenv("BOT_USERNAME") or "").strip() or None
if BOT_USERNAME and BOT_USERNAME.startswith("@"):
    BOT_USERNAME = BOT_USERNAME[1:]
