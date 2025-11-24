# analytics/judge_quality_llm.py
# Production-ready runner for post quality judging:
# - atomic batch fetch (safe for concurrent workers)
# - robust tokenization normalization and device placement
# - JSON repair + secondary model-based extraction
# - retries, attempts counting, fallback heuristic
# - progress printing and simple GPU memory info
# - records signals with raw_output, metrics, inference_time

from __future__ import annotations
import os
import sys
import json
import time
import re
import asyncio
import inspect
import warnings
from typing import List, Dict, Any
from datetime import datetime

import asyncpg
import torch
from dotenv import load_dotenv

# Project base
import pathlib
BASE_DIR = str(pathlib.Path(__file__).resolve().parents[1])
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# Local model loader (assumed present)
from Models.qwen_loader import load_tokenizer_model

# optional transformers GenerationConfig
try:
    from transformers import GenerationConfig, logging as transformers_logging
except Exception:
    GenerationConfig = None
    transformers_logging = None

# load env
load_dotenv(os.path.join(BASE_DIR, ".env"))

# configuration
DB = {
    "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "engagex"),
    "user": os.getenv("POSTGRES_USER", "engagex"),
    "password": os.getenv("POSTGRES_PASSWORD", "engagex"),
}

JUDGE_BATCH = int(os.getenv("JUDGE_BATCH", "32"))
PROCESSING_TIMEOUT_MINUTES = int(os.getenv("PROCESSING_TIMEOUT_MINUTES", "15"))
MODEL_VERSION = os.getenv("MODEL_VERSION", "qwen-local-v1")
SAMPLE_MODE = os.getenv("SAMPLE_MODE", "0")
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))
MAX_NEW_TOKENS = int(os.getenv("MAX_NEW_TOKENS", "512"))

# globals for model
_tokenizer = None
_model = None


def ensure_model():
    global _tokenizer, _model
    if _tokenizer is None or _model is None:
        print(f"[{datetime.now().isoformat()}] Loading tokenizer+model...")
        _tokenizer, _model = load_tokenizer_model()
        try:
            device = _model.device
        except Exception:
            params = list(_model.parameters())
            device = params[0].device if params else torch.device("cpu")
        print(f"[{datetime.now().isoformat()}] Model loaded on device {device}")

        # ensure pad token so tokenizer can build attention_mask if needed
        if getattr(_tokenizer, "pad_token_id", None) is None:
            try:
                _tokenizer.add_special_tokens({"pad_token": "[PAD]"})
                _model.resize_token_embeddings(len(_tokenizer))
            except Exception as e:
                warnings.warn(f"Could not add pad_token to tokenizer: {e}")


# prompts
SYSTEM_MSG = (
    "Ты — строгий, беспристрастный модератор и редактор. "
    "Оцени качество поста для онлайн-сообщества (крипто): ясность, полезность, вовлечение, токсичность/этика, структура.\n"
    "ВЕРНИ ТОЛЬКО ОДИН ВАЛИДНЫЙ JSON. НИЧЕГО БОЛЬШЕ.\n"
    "Формат JSON: {\"score\": <0..100>, \"is_good\": <true|false>, \"reasons\": [..], \"labels\": {\"clarity\":..,\"usefulness\":..,\"engagement\":..,\"ethics\":..}}\n"
)

PROMPT_USER_TEMPLATE = (
    "POST_ID: {post_id}\nCHANNEL: {channel}\n"
    "METRICS: views={views}, forwards={forwards}, reactions={reactions}, comments={comments}, engagement_rate={engagement_rate}\n"
    "POST:\n\"\"\"\n{post}\n\"\"\"\n\n"
    "Верни ТОЛЬКО JSON в указанном формате."
)


def build_messages(text: str, post_id: int, channel: str, metrics: Dict[str, Any]):
    m = PROMPT_USER_TEMPLATE.format(
        post_id=post_id,
        channel=channel,
        views=metrics.get("views", 0),
        forwards=metrics.get("forwards", 0),
        reactions=metrics.get("reactions_sum", 0),
        comments=metrics.get("comments_count", 0),
        engagement_rate=metrics.get("engagement_rate", 0.0),
        post=text[:16000],
    )
    return [
        {"role": "system", "content": SYSTEM_MSG},
        {"role": "user", "content": m},
    ]


# ------------------- tokenization normalization -------------------
def _normalize_input_bundle(input_bundle):
    if isinstance(input_bundle, torch.Tensor):
        return {"input_ids": input_bundle}
    if isinstance(input_bundle, dict):
        return input_bundle
    if isinstance(input_bundle, (list, tuple)):
        tensors = [x for x in input_bundle if isinstance(x, torch.Tensor)]
        if len(tensors) == 1:
            return {"input_ids": tensors[0]}
        if len(tensors) >= 2:
            return {"input_ids": tensors[0], "attention_mask": tensors[1]}
        for x in input_bundle:
            if isinstance(x, dict):
                return x
        raise RuntimeError(f"Unsupported tokenizer return tuple shape: {type(input_bundle)}")
    try:
        attrs = vars(input_bundle)
        d = {k: v for k, v in attrs.items() if isinstance(v, torch.Tensor)}
        if d:
            return d
    except Exception:
        pass
    raise RuntimeError(f"Unsupported tokenizer return type: {type(input_bundle)}")


def _to_device_and_prepare(input_dict, device):
    new = {}
    for k, v in input_dict.items():
        if isinstance(v, torch.Tensor):
            new[k] = v.to(device)
        else:
            try:
                new[k] = torch.tensor(v, device=device)
            except Exception:
                pass
    if "input_ids" not in new:
        raise RuntimeError("Tokenized output has no 'input_ids'")
    if "attention_mask" not in new:
        new["attention_mask"] = torch.ones_like(new["input_ids"], dtype=torch.long, device=device)
    return new


def _supports_generation_config():
    try:
        sig = inspect.signature(_model.generate)
        return "generation_config" in sig.parameters
    except Exception:
        return False


# ------------------- JSON extract & repair -------------------
def try_find_json_with_decoder(text: str):
    decoder = json.JSONDecoder()
    for m in re.finditer(r"\{", text):
        start = m.start()
        try:
            obj, idx = decoder.raw_decode(text[start:])
            return obj
        except Exception:
            continue
    return None


def repair_json_text(gen_text: str):
    s = gen_text
    # remove fenced code blocks
    s = re.sub(r"```.*?```", " ", s, flags=re.S)
    s = s.replace("`", " ")
    # smart quotes -> normal
    s = (
        s.replace("“", '"')
        .replace("”", '"')
        .replace("«", '"')
        .replace("»", '"')
        .replace("’", "'")
    )
    # remove control chars
    s = re.sub(r"[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\x9f]", "", s)
    # try to find JSON-like chunks
    for m in re.finditer(r"\{", s):
        start = m.start()
        chunk = s[start:]
        # attempt to close at last brace
        last = chunk.rfind("}")
        if last != -1:
            candidate = chunk[: last + 1]
        else:
            candidate = chunk
        candidate = candidate.replace("\n", " ")
        candidate = re.sub(r",\s*}", "}", candidate)
        candidate = re.sub(r",\s*\]", "]", candidate)
        candidate = re.sub(r"\s+", " ", candidate).strip()
        try:
            return json.loads(candidate)
        except Exception:
            continue
    return None


def extract_or_recover_json(gen_text: str):
    parsed = try_find_json_with_decoder(gen_text)
    if parsed is not None:
        return parsed
    repaired = repair_json_text(gen_text)
    if repaired is not None:
        return repaired
    return None


# ------------------- fallback heuristic -------------------
def heuristic_fallback_score(metrics: Dict[str, Any]) -> int:
    er = metrics.get("engagement_rate", 0.0)
    views = metrics.get("views", 0)
    if views >= 500 or er > 0.05:
        return 80
    if views >= 100 or er > 0.02:
        return 55
    if views < 10:
        return 10
    return 35


# ------------------- secondary extraction using model -------------------
def extract_with_model(raw_output: str):
    # secondary prompt: ask the model to return a JSON only
    prompt = [
        {
            "role": "system",
            "content": "Ты — помощник. Извлеки один корректный JSON-объект из данного текста. Ничего кроме JSON.",
        },
        {
            "role": "user",
            "content": "Текст:\n\"\"\"\n"
            + raw_output[:16000]
            + "\n\"\"\"\n\nВерни ОДИН JSON.",
        },
    ]
    try:
        inb = _tokenizer.apply_chat_template(
            prompt, add_generation_prompt=False, return_tensors="pt"
        )
    except TypeError:
        inb = _tokenizer.apply_chat_template(prompt, return_tensors="pt")
    normalized = _normalize_input_bundle(inb)
    device = _model.device if hasattr(_model, "device") else torch.device("cpu")
    input_dict = _to_device_and_prepare(normalized, device)
    try:
        with torch.inference_mode():
            out = _model.generate(
                input_ids=input_dict["input_ids"],
                attention_mask=input_dict["attention_mask"],
                max_new_tokens=200,
                do_sample=False,
                pad_token_id=getattr(_tokenizer, "eos_token_id", None),
                eos_token_id=getattr(_tokenizer, "eos_token_id", None),
            )
        start = input_dict["input_ids"].shape[-1]
        gen_ids = out[0][start:]
        gen_text = _tokenizer.decode(gen_ids, skip_special_tokens=True)
        return extract_or_recover_json(gen_text)
    except Exception:
        return None


# ------------------- inference over items -------------------
def infer_batch(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    items: [{'post_id':int,'channel':str,'text':str,'metrics':{...}}...]
    """
    ensure_model()
    results = []
    try:
        device = _model.device
    except Exception:
        params = list(_model.parameters())
        device = params[0].device if params else torch.device("cpu")

    use_generation_config = _supports_generation_config() and (GenerationConfig is not None)

    total = len(items)
    last_time = None
    for i, it in enumerate(items, start=1):
        post_id = it["post_id"]
        text = it["text"]
        channel = it["channel"]
        metrics = it.get("metrics", {})
        # progress print
        now = time.time()
        if last_time:
            avg = now - last_time
        else:
            avg = 0.0
        last_time = now
        print(
            f"[{datetime.now().isoformat()}] LLM infer: {i}/{total} post_id={post_id} avg_last={avg:.2f}s",
            end="\r",
            flush=True,
        )

        messages = build_messages(text, post_id, channel, metrics)
        # tokenization
        try:
            inb = _tokenizer.apply_chat_template(
                messages, add_generation_prompt=True, return_tensors="pt"
            )
        except TypeError:
            inb = _tokenizer.apply_chat_template(messages, return_tensors="pt")

        try:
            normalized = _normalize_input_bundle(inb)
            input_dict = _to_device_and_prepare(normalized, device)
        except Exception as e:
            warnings.warn(f"Tokenization failed for post {post_id}: {e}")
            # fallback heuristic
            fb = heuristic_fallback_score(metrics)
            results.append(
                {
                    "score": fb,
                    "is_good": fb >= 50,
                    "reasons": ["tokenization_failed_fallback"],
                    "labels": {
                        "clarity": 0,
                        "usefulness": 0,
                        "engagement": 0,
                        "ethics": 0,
                    },
                    "raw_output": "",
                    "inference_time_s": 0.0,
                }
            )
            continue

        gen_kwargs = dict(
            input_ids=input_dict["input_ids"],
            attention_mask=input_dict["attention_mask"],
            max_new_tokens=MAX_NEW_TOKENS,
            pad_token_id=getattr(_tokenizer, "eos_token_id", None),
            eos_token_id=getattr(_tokenizer, "eos_token_id", None),
            do_sample=False,
        )

        if use_generation_config and SAMPLE_MODE == "1":
            temp = float(os.getenv("SAMPLE_TEMPERATURE", 0.7))
            top_p = float(os.getenv("SAMPLE_TOP_P", 0.9))
            top_k = int(os.getenv("SAMPLE_TOP_K", 50))
            gen_cfg = GenerationConfig(
                max_new_tokens=MAX_NEW_TOKENS,
                do_sample=True,
                temperature=temp,
                top_p=top_p,
                top_k=top_k,
            )
            gen_kwargs = {
                "input_ids": input_dict["input_ids"],
                "attention_mask": input_dict["attention_mask"],
                "generation_config": gen_cfg,
                "pad_token_id": gen_cfg.pad_token_id or gen_kwargs["pad_token_id"],
                "eos_token_id": gen_kwargs["eos_token_id"],
            }

        # generation
        t0 = time.time()
        try:
            with torch.inference_mode():
                out = _model.generate(**gen_kwargs)
        except TypeError as e:
            warnings.warn(f"generate TypeError for post {post_id}: {e}; retrying minimal")
            try:
                out = _model.generate(
                    input_ids=input_dict["input_ids"],
                    attention_mask=input_dict["attention_mask"],
                    max_new_tokens=MAX_NEW_TOKENS,
                    do_sample=False,
                    pad_token_id=getattr(_tokenizer, "eos_token_id", None),
                    eos_token_id=getattr(_tokenizer, "eos_token_id", None),
                )
            except Exception as e2:
                warnings.warn(f"generate failed for post {post_id}: {e2}")
                fb = heuristic_fallback_score(metrics)
                results.append(
                    {
                        "score": fb,
                        "is_good": fb >= 50,
                        "reasons": ["generation_failed_fallback"],
                        "labels": {
                            "clarity": 0,
                            "usefulness": 0,
                            "engagement": 0,
                            "ethics": 0,
                        },
                        "raw_output": "",
                        "inference_time_s": 0.0,
                    }
                )
                continue
        except Exception as e:
            warnings.warn(f"Generation exception for post {post_id}: {e}")
            fb = heuristic_fallback_score(metrics)
            results.append(
                {
                    "score": fb,
                    "is_good": fb >= 50,
                    "reasons": ["generation_exception_fallback"],
                    "labels": {
                        "clarity": 0,
                        "usefulness": 0,
                        "engagement": 0,
                        "ethics": 0,
                    },
                    "raw_output": "",
                    "inference_time_s": 0.0,
                }
            )
            continue
        t1 = time.time()
        inference_time = t1 - t0

        # decode generated part
        try:
            out_ids = out[0] if not isinstance(out, list) else out[0]
            start = input_dict["input_ids"].shape[-1]
            gen_ids = out_ids[start:]
            gen_text = _tokenizer.decode(gen_ids, skip_special_tokens=True)
        except Exception as e:
            warnings.warn(f"Decoding failed for post {post_id}: {e}")
            gen_text = ""

        # extract JSON
        js = extract_or_recover_json(gen_text)
        raw_out = gen_text
        if js is None:
            # try secondary extraction with model itself
            js = extract_with_model(gen_text)
            if js is not None:
                reason_tag = "recovered_by_model"
            else:
                reason_tag = "bad_json"
        else:
            reason_tag = None

        if js is None:
            # fallback heuristic
            fb = heuristic_fallback_score(metrics)
            results.append(
                {
                    "score": fb,
                    "is_good": fb >= 50,
                    "reasons": ["bad_json_fallback"],
                    "labels": {
                        "clarity": 0,
                        "usefulness": 0,
                        "engagement": 0,
                        "ethics": 0,
                    },
                    "raw_output": (raw_out[:2000] if raw_out else ""),
                    "inference_time_s": inference_time,
                }
            )
            continue

        # parse js fields
        try:
            score = float(js.get("score", 0))
            is_good = bool(js.get("is_good", False))
            reasons = js.get("reasons", [])
            labels = js.get("labels", {})
            entry = {
                "score": max(0.0, min(100.0, score)),
                "is_good": bool(is_good),
                "reasons": reasons[:6] if isinstance(reasons, list) else [str(reasons)],
                "labels": {
                    "clarity": float(labels.get("clarity", 0)),
                    "usefulness": float(labels.get("usefulness", 0)),
                    "engagement": float(labels.get("engagement", 0)),
                    "ethics": float(labels.get("ethics", 0)),
                },
                "raw_output": (raw_out[:2000] if raw_out else ""),
                "inference_time_s": inference_time,
            }
            if reason_tag:
                entry["reasons"].append(reason_tag)
            results.append(entry)
        except Exception as e:
            warnings.warn(f"Failed to parse js for post {post_id}: {e}")
            fb = heuristic_fallback_score(metrics)
            results.append(
                {
                    "score": fb,
                    "is_good": fb >= 50,
                    "reasons": ["bad_json_parse_fallback"],
                    "labels": {
                        "clarity": 0,
                        "usefulness": 0,
                        "engagement": 0,
                        "ethics": 0,
                    },
                    "raw_output": (raw_out[:2000] if raw_out else ""),
                    "inference_time_s": inference_time,
                }
            )
            continue

    # end loop
    print()  # newline after progress line
    # show GPU mem if available
    try:
        if torch.cuda.is_available():
            d = _model.device
            used = torch.cuda.memory_allocated(d) / 1024**2
            reserved = torch.cuda.memory_reserved(d) / 1024**2
            print(
                f"[{datetime.now().isoformat()}] GPU: {d} used={used:.0f}MiB reserved={reserved:.0f}MiB"
            )
    except Exception:
        pass

    return results


# ------------------- DB helpers for atomic batches -------------------
async def atomic_fetch_and_mark(conn: asyncpg.Connection, batch: int, pid: int):
    """
    1) SELECT ids FOR UPDATE SKIP LOCKED on posts only
    2) UPDATE posts to processing for these ids
    3) SELECT detailed rows (with joins) for those ids
    returns list of rows
    """
    # Step 1: get ids (locked)
    rows = await conn.fetch(
        "SELECT id FROM posts p "
        "WHERE p.processing_status = 'new' "
        "  AND NOT EXISTS (SELECT 1 FROM post_quality pq WHERE pq.post_id = p.id) "
        "ORDER BY p.id "
        "LIMIT $1 FOR UPDATE SKIP LOCKED",
        batch,
    )
    if not rows:
        return []

    ids = [r["id"] for r in rows]

    # Step 2: mark as processing
    await conn.execute(
        "UPDATE posts SET processing_status='processing', processor_pid=$1, processing_started_at=now() WHERE id = ANY($2::int[])",
        pid,
        ids,
    )

    # Step 3: collect details for those ids
    fetch_sql = """
    SELECT p.id,
           p.channel_username,
           COALESCE(cp.clean_text, p.post_text) AS text,
           p.views, p.forwards
    FROM posts p
    LEFT JOIN clean_posts cp ON cp.source_post_id = p.id
    WHERE p.id = ANY($1::int[])
    ORDER BY p.id
    """
    rows2 = await conn.fetch(fetch_sql, ids)

    # aggregate reactions and comments for these ids
    reactions = {}
    rows_r = await conn.fetch(
        "SELECT post_id, SUM(reaction_count) AS reactions_sum FROM reactions WHERE post_id = ANY($1::int[]) GROUP BY post_id",
        ids,
    )
    for r in rows_r:
        reactions[r["post_id"]] = int(r["reactions_sum"] or 0)

    comments = {}
    rows_c = await conn.fetch(
        "SELECT post_id, COUNT(*) AS comments_count FROM comments WHERE post_id = ANY($1::int[]) GROUP BY post_id",
        ids,
    )
    for r in rows_c:
        comments[r["post_id"]] = int(r["comments_count"] or 0)

    # build result rows with metrics
    result = []
    for r in rows2:
        pid_row = int(r["id"])
        views = int(r["views"] or 0)
        forwards = int(r["forwards"] or 0)
        reactions_sum = reactions.get(pid_row, 0)
        comments_count = comments.get(pid_row, 0)
        engagement_rate = (
            (reactions_sum + comments_count) / max(1, views) if views > 0 else 0.0
        )
        result.append(
            {
                "id": pid_row,
                "channel_username": r["channel_username"],
                "text": (r["text"] or "").strip() or " ",
                "views": views,
                "forwards": forwards,
                "reactions_sum": reactions_sum,
                "comments_count": comments_count,
                "engagement_rate": round(engagement_rate, 6),
            }
        )
    return result


# UPSERT for results
# ВАЖНО: есть колонка gen_status (VARCHAR(32)), которую код явно проставляет.
UPSERT_SQL = """
INSERT INTO post_quality (
    post_id,
    channel_username,
    quality_score,
    is_good,
    signals,
    gen_status,
    updated_at
)
VALUES ($1, $2, $3, $4, $5::jsonb, $6, now())
ON CONFLICT (post_id) DO UPDATE
SET quality_score = EXCLUDED.quality_score,
    is_good       = EXCLUDED.is_good,
    signals       = EXCLUDED.signals,
    gen_status    = EXCLUDED.gen_status,
    updated_at    = now();
"""

# helper to reset stuck records older than timeout (minutes)
RESET_STUCK_SQL = """
UPDATE posts
SET processing_status = 'new', processor_pid = NULL, processing_started_at = NULL
WHERE processing_status = 'processing'
  AND processing_started_at < now() - ($1 * INTERVAL '1 minute')
RETURNING id;
"""


# ------------------- main loop -------------------
async def main():
    print(f"[{datetime.now().isoformat()}] 🧑‍⚖️ LLM-оценка постов → post_quality")
    ensure_model()

    conn = await asyncpg.connect(**DB)
    try:
        # 🔹 Считаем, сколько новых постов ждут оценки
        total_planned = await conn.fetchval(
            """
            SELECT COUNT(*) 
            FROM posts p
            WHERE p.processing_status = 'new'
              AND NOT EXISTS (
                  SELECT 1 
                  FROM post_quality pq 
                  WHERE pq.post_id = p.id
              )
            """
        )
        print(
            f"[{datetime.now().isoformat()}] 📊 Найдено {total_planned} постов в статусе 'new' без оценки. "
            f"Будем обрабатывать батчами по {JUDGE_BATCH}."
        )

        pid = os.getpid()
        total = 0
        last_reset = datetime.now()

        while True:
            # periodic reset of stuck records
            if (datetime.now() - last_reset).total_seconds() > 600:
                rows = await conn.fetch(RESET_STUCK_SQL, PROCESSING_TIMEOUT_MINUTES)
                if rows:
                    print(
                        f"[{datetime.now().isoformat()}] Reset {len(rows)} stuck posts -> 'new'"
                    )
                last_reset = datetime.now()

            # atomic fetch + mark
            async with conn.transaction():
                items = await atomic_fetch_and_mark(conn, JUDGE_BATCH, pid)

            if not items:
                print(f"[{datetime.now().isoformat()}] Нет новых постов для обработки. Выход.")
                break

            print(
                f"[{datetime.now().isoformat()}] -> fetched rows: {len(items)}; GPU status check..."
            )

            # prepare inputs for infer
            inputs = []
            metas = []
            for row in items:
                metrics = {
                    "views": row["views"],
                    "forwards": row["forwards"],
                    "reactions_sum": row["reactions_sum"],
                    "comments_count": row["comments_count"],
                    "engagement_rate": row["engagement_rate"],
                }
                inputs.append(
                    {
                        "post_id": row["id"],
                        "channel": row["channel_username"],
                        "text": row["text"],
                        "metrics": metrics,
                    }
                )
                metas.append((row["id"], row["channel_username"]))

            # call inference
            print(
                f"[{datetime.now().isoformat()}] Calling infer_batch for {len(inputs)} items ..."
            )
            judged = infer_batch(inputs)

            # build upserts and status changes
            upserts = []
            done_ids = []
            bump_attempts = []

            for meta, res, it in zip(metas, judged, inputs):
                pid_item, ch = meta
                signals = {
                    "judge": "llm",
                    "model_version": MODEL_VERSION,
                    "score": res.get("score", 0),
                    "is_good": res.get("is_good", False),
                    "reasons": res.get("reasons", []),
                    "labels": res.get("labels", {}),
                    "metrics": it["metrics"],
                    "raw_output": res.get("raw_output", "")[:2000],
                    "inference_time_s": res.get("inference_time_s", None),
                }

                # Пока по договорённости: всегда ставим gen_status = 'ok'
                gen_status = "ok"

                upserts.append(
                    (
                        pid_item,
                        ch,
                        float(signals["score"]),
                        bool(signals["is_good"]),
                        json.dumps(signals, ensure_ascii=False),
                        gen_status,
                    )
                )

                reasons = signals["reasons"]
                if reasons and any(
                    r in ("bad_json", "bad_json_fallback", "tokenization_failed_fallback")
                    for r in reasons
                ):
                    bump_attempts.append(pid_item)
                else:
                    done_ids.append(pid_item)

            # write results to post_quality
            await conn.executemany(UPSERT_SQL, upserts)

            # mark done
            if done_ids:
                await conn.execute(
                    """
                    UPDATE posts 
                    SET processing_status='done',
                        processing_started_at=NULL,
                        processor_pid=NULL
                    WHERE id = ANY($1::int[])
                    """,
                    done_ids,
                )

            # handle bumped attempts
            for bid in bump_attempts:
                await conn.execute(
                    "UPDATE posts SET attempts = COALESCE(attempts,0) + 1 WHERE id = $1",
                    bid,
                )
                attempts_now = await conn.fetchval(
                    "SELECT attempts FROM posts WHERE id = $1",
                    bid,
                )
                if attempts_now >= MAX_ATTEMPTS:
                    await conn.execute(
                        """
                        UPDATE posts 
                        SET processing_status='error',
                            processing_started_at=NULL,
                            processor_pid=NULL
                        WHERE id = $1
                        """,
                        bid,
                    )
                    print(
                        f"[{datetime.now().isoformat()}] Post {bid} -> marked error after {attempts_now} attempts"
                    )
                else:
                    await conn.execute(
                        """
                        UPDATE posts 
                        SET processing_status='new',
                            processing_started_at=NULL,
                            processor_pid=NULL
                        WHERE id = $1
                        """,
                        bid,
                    )
                    print(
                        f"[{datetime.now().isoformat()}] Post {bid} -> scheduled for retry (attempt {attempts_now})"
                    )

            total += len(items)
            print(f"[{datetime.now().isoformat()}]  ✓ +{len(items)} (итого {total})")

    finally:
        await conn.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted by user; exiting.")
