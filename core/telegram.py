from __future__ import annotations

import os
import json
import requests
from typing import Optional, Any
from core import storage
from core.logger import log, log_exc

BOT_TOKEN = os.environ.get("BOT_TOKEN")
API = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else None


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if s in ("1", "true", "t", "yes", "y", "on", "si", "sí"):
        return True
    if s in ("0", "false", "f", "no", "n", "off"):
        return False
    return default


def normalize_item_alert_level(value: Any) -> str:
    v = str(value or "").strip().lower()
    if v == "alto":
        return "alto"
    if v == "medio":
        return "medio"
    if v == "bajo":
        return "bajo"
    return "medio"


def _normalize_target_entry(raw: dict[str, Any]) -> dict[str, Any] | None:
    chat_id = str(raw.get("chat_id") or "").strip()
    if not chat_id:
        return None

    send_alto = _to_bool(raw.get("send_alto"), True)
    send_medio = _to_bool(raw.get("send_medio"), False)
    send_bajo = _to_bool(raw.get("send_bajo"), False)

    return {
        "chat_id": chat_id,
        "label": str(raw.get("label") or "").strip(),
        "enabled": _to_bool(raw.get("enabled"), True),
        "send_alerts": _to_bool(raw.get("send_alerts"), True),
        "send_alto": send_alto,
        "send_medio": send_medio,
        "send_bajo": send_bajo,
    }


def get_telegram_targets(cfg: dict[str, Any] | None = None, include_disabled: bool = True) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    cfg_local = cfg if isinstance(cfg, dict) else {}
    cfg_targets_provided = isinstance(cfg_local.get("telegram_targets"), list)

    if cfg_targets_provided:
        raw_cfg_targets = cfg_local.get("telegram_targets")
        if isinstance(raw_cfg_targets, list):
            for t in raw_cfg_targets:
                if isinstance(t, dict):
                    nt = _normalize_target_entry(t)
                    if nt is not None:
                        targets.append(nt)

    if not targets and not cfg_targets_provided:
        try:
            db_targets = storage.list_telegram_targets(include_disabled=True)
            if isinstance(db_targets, list):
                for t in db_targets:
                    if isinstance(t, dict):
                        nt = _normalize_target_entry(t)
                        if nt is not None:
                            targets.append(nt)
        except Exception:
            pass

    dedup: list[dict[str, Any]] = []
    seen: set[str] = set()
    for t in targets:
        key = str(t.get("chat_id") or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        dedup.append(t)

    if include_disabled:
        return dedup
    return [t for t in dedup if _to_bool(t.get("enabled"), True)]


def should_send_to_target(level: str, target: dict[str, Any]) -> bool:
    if not _to_bool(target.get("enabled"), True):
        return False
    if not _to_bool(target.get("send_alerts"), True):
        return False

    normalized_level = normalize_item_alert_level(level)
    if normalized_level == "alto":
        return _to_bool(target.get("send_alto"), True)
    if normalized_level == "medio":
        return _to_bool(target.get("send_medio"), False)
    return _to_bool(target.get("send_bajo"), False)


def get_target_chats_for_item(item: dict[str, Any], cfg: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    level = normalize_item_alert_level(
        item.get("classification") or item.get("level"))
    out: list[dict[str, Any]] = []
    for target in get_telegram_targets(cfg=cfg, include_disabled=False):
        try:
            if should_send_to_target(level, target):
                out.append(target)
        except Exception:
            continue
    return out


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


def send_alert_text(
    chat_id: str,
    text: str,
    parse_mode: str = "Markdown",
    reply_markup: dict[str, Any] | None = None,
) -> bool:
    try:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
        if isinstance(reply_markup, dict) and reply_markup:
            payload["reply_markup"] = json.dumps(
                reply_markup, ensure_ascii=False)
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


def _escape_markdown(text: str) -> str:
    s = str(text or "")
    for ch in ("\\", "_", "*", "`", "[", "]", "(", ")"):
        s = s.replace(ch, f"\\{ch}")
    return s


def _sanitize_markdown_url(url: str) -> str:
    u = str(url or "").strip()
    return u.replace(" ", "%20").replace("(", "%28").replace(")", "%29")


def _normalize_classification(item: dict) -> str:
    val = str(item.get("classification") or item.get(
        "level") or "").strip().lower()
    if val == "alto":
        return "alto"
    if val == "medio":
        return "medio"
    if val == "bajo":
        return "bajo"
    return "medio"


def format_item_message(item: dict, prefix: str = "Seleccionado") -> str:
    title = str(item.get("title") or "(sin título)").strip()
    source = str(item.get("source") or "Fuente").strip()
    url = _sanitize_markdown_url(item.get("url") or "")

    classification = _normalize_classification(item)
    emoji = str(item.get("emoji") or "")
    if not emoji:
        emoji = "🔴" if classification == "alto" else (
            "🟠" if classification == "medio" else "🟢")

    parts: list[str] = [
        f"{emoji} {_escape_markdown(prefix)}: {_escape_markdown(title)}"]

    if url:
        parts.append(f"Fuente: [{_escape_markdown(source)}]({url})")
    else:
        parts.append(f"Fuente: {_escape_markdown(source)}")

    parts.append(f"Clasificación: {emoji} {classification}")

    return "\n".join(parts)


def send_item_notification(item: dict, chat_id: str, item_id: int | None = None) -> bool:
    try:
        origin = str(item.get("origin") or "").strip().lower()
        prefix = "Seleccionado" if origin.startswith("telegram") else "Alerta"
        text = format_item_message(item, prefix=prefix)
        reply_markup = None
        try:
            item_id_i = int(item_id or 0)
        except Exception:
            item_id_i = 0

        if item_id_i > 0:
            text = f"{text}"
            reply_markup = {
                "inline_keyboard": [[
                    {
                        "text": "✅ Marcar leído",
                        "callback_data": f"read:{item_id_i}",
                    }
                ]]
            }

        try:
            chat_id = normalize_chat_id(str(chat_id))
        except Exception:
            pass
        res = send_alert_text(
            chat_id,
            text,
            parse_mode="Markdown",
            reply_markup=reply_markup,
        )
        return res
    except Exception as e:
        try:
            log_exc("core.telegram: send_item_notification failed", e)
        except Exception:
            pass
        return False


def send_item_notification_to_targets(
    item: dict,
    *,
    cfg: dict[str, Any] | None = None,
    item_id: int | None = None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    targets = get_target_chats_for_item(item, cfg=cfg)

    for target in targets:
        raw_chat_id = str(target.get("chat_id") or "").strip()
        if not raw_chat_id:
            continue

        try:
            chat_id = normalize_chat_id(raw_chat_id)
        except Exception:
            chat_id = raw_chat_id

        resp = send_item_notification(item, chat_id, item_id=item_id)
        ok = bool(resp and isinstance(resp, dict) and resp.get("ok"))
        message_id = None
        if ok:
            try:
                message_id = resp.get("result", {}).get("message_id")
            except Exception:
                message_id = None

            if item_id and message_id is not None:
                try:
                    storage.record_item_telegram_message(
                        item_id, chat_id, message_id)
                except Exception:
                    pass

        results.append(
            {
                "chat_id": chat_id,
                "ok": ok,
                "message_id": message_id,
                "response": resp,
            }
        )

    return results


def send_document(chat_id: str, file_path: str, caption: str | None = None, parse_mode: str = "Markdown") -> Optional[dict]:
    if not API:
        log("core.telegram: BOT_TOKEN not configured", "ERROR")
        return None

    path = str(file_path or "").strip()
    if not path or not os.path.exists(path):
        try:
            log(
                f"core.telegram: send_document missing file: {path}", "WARNING")
        except Exception:
            pass
        return None

    try:
        normalized_chat = normalize_chat_id(str(chat_id or "").strip())
    except Exception:
        normalized_chat = str(chat_id or "").strip()

    data: dict[str, Any] = {"chat_id": normalized_chat}
    if caption:
        data["caption"] = str(caption)
        data["parse_mode"] = parse_mode

    url = f"{API}/sendDocument"
    resp = None
    try:
        with open(path, "rb") as fh:
            resp = requests.post(
                url,
                data=data,
                files={"document": (os.path.basename(path), fh)},
                timeout=60,
            )
        resp.raise_for_status()
        payload = resp.json() if resp is not None else {"ok": False}
        try:
            log(f"core.telegram: document sent to {normalized_chat}: {path}")
        except Exception:
            pass
        return payload
    except Exception as e:
        try:
            body = resp.text if resp is not None else None
            log(
                f"core.telegram: sendDocument failed for {normalized_chat} file={path} body={body}",
                "WARNING",
            )
            log_exc("core.telegram: send_document failed", e)
        except Exception:
            pass
        return None


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
