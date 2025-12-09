
from __future__ import annotations

from typing import List, Dict, Any

import aiohttp

from ..config import MODEL_SERVER_URL


async def call_model(messages: List[Dict[str, str]]) -> str:
    """
    Универсальный клиент к MODEL_SERVER_URL.

    Поддерживает:
    1) OpenAI-стиль:
       {"choices": [{"message": {"content": "..."}}]}
    2) Простой формат:
       {"text": "...", "mode": "..."}
    """
    async with aiohttp.ClientSession() as session:
        async with session.post(
            MODEL_SERVER_URL,
            json={"messages": messages, "max_tokens": 800, "temperature": 0.9},
            timeout=120,
        ) as resp:
            resp.raise_for_status()
            data: Any = await resp.json()

    if isinstance(data, dict):
        if "choices" in data:
            try:
                return data["choices"][0]["message"]["content"]
            except Exception as e:
                raise RuntimeError(f"Bad model response (choices): {data}") from e

        if "text" in data and isinstance(data["text"], str):
            return data["text"]

    raise RuntimeError(f"Bad model response: {data}")
