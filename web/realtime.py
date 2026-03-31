from __future__ import annotations
from core import storage
from core import classifier
from datetime import datetime, timezone
from flask import Flask, request, jsonify

import json
import os

import importlib
from typing import Any

_redis: Any = None
try:
    _redis = importlib.import_module("redis")
except Exception:
    _redis = None

_HAVE_REDIS = _redis is not None


REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
QUEUE_KEY = os.environ.get("PA_REALTIME_QUEUE_KEY", "pa:realtime:queue")

app = Flask(__name__)
client = None
if _redis is not None:
    try:
        client = _redis.Redis.from_url(REDIS_URL)
    except Exception:
        client = None


def publish_item(item: dict) -> None:
    payload = json.dumps(item, ensure_ascii=False)
    if client is not None:
        try:
            client.rpush(QUEUE_KEY, payload)
            return
        except Exception:
            pass

    try:
        text = item.get("summary", "") or ""
        title = item.get("title", "") or ""
        kw = item.get("keyword", "") or ""
        cls = classifier.classify_text(text, title=title, keyword=kw)

        if isinstance(cls, dict):
            impacto = (cls.get("impacto") or "MEDIO").upper()
        elif isinstance(cls, tuple) and len(cls) == 3:
            lvl = (cls[0] or "").lower()
            if lvl == "high":
                impacto = "CRITICO"
            elif lvl == "medium":
                impacto = "MEDIO"
            elif lvl == "low":
                impacto = "BAJO"
            else:
                impacto = "MEDIO"
        elif isinstance(cls, str):
            impacto = (cls or "MEDIO").upper()
        else:
            impacto = "MEDIO"

        level = impacto
        if impacto == "CRITICO":
            emoji = "🔴"
            color = "rojo"
        elif impacto == "MEDIO":
            emoji = "🟠"
            color = "naranja"
        else:
            emoji = "🟢"
            color = "verde"

        extracted_at = datetime.now(timezone.utc).isoformat()

        enriched = {
            "source": item.get("source", "realtime_fallback"),
            "title": item.get("title", "(sin titulo)"),
            "url": item.get("url", ""),
            "summary": item.get("summary", ""),
            "published_at": item.get("published_at", ""),
            "keyword": item.get("keyword", ""),
            "extracted_at": extracted_at,
            "level": level,
            "emoji": emoji,
            "color": color,
            "origin": item.get("origin") or "realtime",
            "meta": cls,
        }
        storage.append_item(enriched)
    except Exception:
        pass


@app.route("/webhook", methods=["POST"])
def webhook():
    item = request.get_json(force=True)
    publish_item(item)
    return jsonify({"ok": True})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5001")))
