from __future__ import annotations

import os
import json
import requests
import html
from typing import Optional
from core.logger import log, log_exc

BOT_TOKEN = os.environ.get("BOT_TOKEN")
API = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else None


def _api_post(method: str, data: dict) -> Optional[dict]:
    if not API:
        log("core.telegram: BOT_TOKEN not configured", "ERROR")
        return None
    url = f"{API}/{method}"
    resp = None
    try:
        resp = requests.post(url, data=data, timeout=15)
        resp.raise_for_status()
        try:
            return resp.json()
        except Exception:
            return {"ok": True}
    except Exception as e:
        try:
            if resp is not None:
                try:
                    body = resp.text
                except Exception:
                    body = None
                log(
                    f"core.telegram: HTTP error {getattr(resp, 'status_code', '??')} for {method} -> {body}", "WARNING")
            log_exc(f"core.telegram: api_post {method} failed", e)
        except Exception:
            pass
        return None


def send_alert_text(chat_id: str, text: str, parse_mode: str = "HTML") -> bool:
    try:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
        res = _api_post("sendMessage", payload)
        if res and res.get("ok"):
            try:
                log(f"core.telegram: message sent to {chat_id}")
            except Exception:
                pass
            return res
        else:
            try:
                log(
                    f"core.telegram: sendMessage returned not ok for {chat_id}: {res}", "WARNING")
            except Exception:
                pass
            return res
    except Exception as e:
        try:
            log_exc("core.telegram: send_alert_text failed", e)
        except Exception:
            pass
        return None


def _format_item_message(item: dict) -> str:
    parts = []
    emoji = item.get("emoji") or ""
    level = item.get("level") or ""
    title = item.get("title") or "(sin título)"
    url = item.get("url") or ""
    summary = item.get("summary") or ""
    source = item.get("source") or ""

    try:
        title = html.escape(str(title))
    except Exception:
        title = str(title)
    try:
        url = html.escape(str(url))
    except Exception:
        url = str(url)
    try:
        summary = html.escape(str(summary))
    except Exception:
        summary = str(summary)
    try:
        source = html.escape(str(source))
    except Exception:
        source = str(source)

    parts.append(f"{emoji} <b>{(level or '').upper()}</b> - {title}")
    if url:
        parts.append(f"Fuente: {url}")
    if summary:
        s = summary.strip()
        if len(s) > 300:
            s = s[:300].rsplit(" ", 1)[0] + "..."
        parts.append(f"Resumen: {s}")
    if source:
        parts.append(f"Fuente técnica: {source}")

    return "\n".join(parts)


def send_item_notification(item: dict, chat_id: str) -> bool:
    try:
        text = _format_item_message(item)
        try:
            chat_id = normalize_chat_id(str(chat_id))
        except Exception:
            pass
        res = send_alert_text(chat_id, text, parse_mode="HTML")
        return res
    except Exception as e:
        try:
            log_exc("core.telegram: send_item_notification failed", e)
        except Exception:
            pass
        return False


def normalize_chat_id(raw: str) -> str:
    if not raw:
        return raw
    s = raw.strip()
    if s.startswith("http://") or s.startswith("https://"):
        try:
            parts = s.rstrip("/").split("/")
            s = parts[-1]
        except Exception:
            pass
    if s.startswith("t.me/"):
        s = s.split("/", 1)[1]
    if s and not s.startswith("@"):
        try:
            int(s)
            return s
        except Exception:
            return "@" + s
    return s
