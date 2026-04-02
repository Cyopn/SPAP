from __future__ import annotations
from core.dedupe_utils import signature_for_item, log_duplicate
from core import storage, classifier

import asyncio
import json
import os
import time
from typing import Any
from core.logger import log, log_exc

import importlib
from core import telegram as core_telegram
from core.timezone_mx import now_mx_iso

_redis: Any = None
try:
    _redis = importlib.import_module("redis")
except Exception:
    _redis = None

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
QUEUE_KEY = os.environ.get("PA_REALTIME_QUEUE_KEY", "pa:realtime:queue")


def _to_text(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, (bytes, bytearray)):
        try:
            return val.decode("utf-8")
        except Exception:
            return str(val)
    try:
        return str(val)
    except Exception:
        return ""


def _resolve_awaitable(value: Any):
    if not hasattr(value, "__await__"):
        return value
    try:
        return asyncio.run(value)
    except RuntimeError:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            fut = asyncio.ensure_future(value)
            return value
        raise


def run_worker():
    client = None
    if _redis is not None:
        try:
            client = _redis.Redis.from_url(REDIS_URL)
        except Exception:
            client = None
    else:
        log("worker: redis library not installed; worker will idle until redis available", "WARNING")

    if client is None:
        while True:
            time.sleep(5)
        return

    while True:
        try:
            res = client.blpop(QUEUE_KEY, timeout=5)
            res = _resolve_awaitable(res)
            if not res:
                time.sleep(0.2)
                continue

            payload = res[1] if isinstance(
                res, (list, tuple)) and len(res) >= 2 else res

            if isinstance(payload, (bytes, bytearray)):
                try:
                    payload = payload.decode("utf-8")
                except Exception:
                    pass

            if isinstance(payload, (str, bytes, bytearray)):
                try:
                    obj = json.loads(payload)
                except Exception:
                    obj = {"raw": payload}
            else:
                obj = {"raw": payload}

            title = obj.get("title") or obj.get("text") or "(sin titulo)"
            url = obj.get("url") or obj.get("link") or ""
            summary = obj.get("summary") or obj.get("text") or ""
            published_at = obj.get("published_at") or ""
            keyword = obj.get("keyword") or ""

            title_s = _to_text(title) or "(sin titulo)"
            summary_s = _to_text(summary)

            cls_result = classifier.classify_text(title_s + " " + summary_s)

            level = "medio"
            emoji = ""
            color = ""
            if isinstance(cls_result, dict):
                impacto = str(cls_result.get("impacto")
                              or "medio").strip().lower()
                if impacto == "alto":
                    level = "alto"
                    emoji = "🔴"
                    color = "rojo"
                elif impacto == "medio":
                    level = "medio"
                    emoji = "🟠"
                    color = "naranja"
                else:
                    level = "bajo"
                    emoji = "🟢"
                    color = "verde"
                meta = cls_result
            elif isinstance(cls_result, tuple) and len(cls_result) == 3:
                lvl0 = (cls_result[0] or "").lower()
                if lvl0 == "alto":
                    level = "alto"
                    emoji = "🔴"
                    color = "rojo"
                elif lvl0 == "medio":
                    level = "medio"
                    emoji = "🟠"
                    color = "naranja"
                else:
                    level = "bajo"
                    emoji = "🟢"
                    color = "verde"
                meta = {"legacy": True, "value": cls_result}
            elif isinstance(cls_result, str):
                imp = str(cls_result or "medio").strip().lower()
                if imp == "alto":
                    level = "alto"
                    emoji = "🔴"
                    color = "rojo"
                elif imp == "medio":
                    level = "medio"
                    emoji = "🟠"
                    color = "naranja"
                else:
                    level = "bajo"
                    emoji = "🟢"
                    color = "verde"
                meta = {"legacy_str": cls_result}
            else:
                meta = {"unknown": True}

            src_val = obj.get("source", "realtime")
            if not isinstance(src_val, (str, dict)):
                src_val = _to_text(src_val)

            title_val = _to_text(title) or "(sin titulo)"
            url_val = _to_text(url)
            summary_val = _to_text(summary)
            published_at_val = _to_text(published_at)
            keyword_val = _to_text(keyword)

            enriched_item = {
                "source": src_val,
                "title": title_val,
                "url": url_val,
                "summary": summary_val,
                "published_at": published_at_val,
                "keyword": keyword_val,
                "extracted_at": now_mx_iso(),
                "level": level,
                "emoji": emoji,
                "color": color,
                "origin": "realtime",
                "meta": meta,
            }

            item_id = storage.append_item(enriched_item)

            sig = signature_for_item(title_val, url_val)
            try:
                log_duplicate(sig, item_id)
            except Exception:
                pass

            try:
                cfg = storage.get_config("monitor_config") or {}
            except Exception:
                cfg = {}

            try:
                send_results = core_telegram.send_item_notification_to_targets(
                    enriched_item,
                    cfg=cfg,
                    item_id=item_id,
                )
                first_msg_id = None
                for r in send_results:
                    if r.get("ok") and r.get("message_id"):
                        first_msg_id = r.get("message_id")
                        break

                if first_msg_id is not None:
                    try:
                        storage.set_tg_message_id(item_id, first_msg_id)
                    except Exception:
                        pass
            except Exception as e:
                try:
                    log_exc(
                        f"worker: exception sending alert for item_id={item_id}", e)
                except Exception:
                    pass

        except Exception:
            time.sleep(1)
            continue


if __name__ == "__main__":
    run_worker()
