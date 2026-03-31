from __future__ import annotations

from core.news_finder import (
    search_google_news,
    search_bing_news,
    search_hacker_news,
    search_newsapi,
    search_x,
    search_facebook,
    search_instagram,
    deduplicate,
    _HAS_NEWSMELT_ADAPTERS,
)
from core import classifier
import os
import time
import json
import requests
import base64
import re
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional, Any
from core.dedupe_utils import signature_for_item as make_signature, log_duplicate
from core import storage
from monitors import monitor
from core.logger import log, log_exc
import threading
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
env_root = ROOT / ".env"
if env_root.exists():
    load_dotenv(env_root)
else:
    load_dotenv()


BOT_TOKEN = os.environ.get("BOT_TOKEN")
TARGET_CHAT = os.environ.get("TELEGRAM_TARGET_CHAT_ID")
OFFSET_FILE = "telegram_offset.txt"
API = f"https://api.telegram.org/bot{BOT_TOKEN}"

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN no encontrado en .env")

try:
    log(f"telegram_bot: NEWS_API={'set' if os.environ.get('NEWS_API') else 'unset'}")
except Exception:
    pass


def _clear_telegram_commands():
    try:
        api_post("setMyCommands", {"commands": json.dumps([])})
        try:
            log("telegram_bot: cleared bot commands via setMyCommands()")
        except Exception:
            pass
    except Exception as e:
        try:
            log_exc("telegram_bot: failed to clear bot commands", e)
        except Exception:
            pass


TMP_SEARCH_DIR = Path(__file__).parent / ".tmp_searches"
TMP_SEARCH_DIR.mkdir(exist_ok=True)
TMP_STATE_DIR = Path(__file__).parent / ".tmp_states"
TMP_STATE_DIR.mkdir(exist_ok=True)


def api_post(method: str, data: dict) -> Optional[dict]:
    url = f"{API}/{method}"
    try:
        resp = requests.post(url, data=data, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def init_bot():
    try:
        _clear_telegram_commands()
    except Exception:
        pass


def detect_level(text: str) -> tuple[str, str, str]:
    try:
        res = classifier.classify_text(text)
    except Exception:
        res = None

    impacto = None
    if isinstance(res, dict):
        impacto = (res.get("impacto") or "MEDIO").upper()
    elif isinstance(res, tuple) and len(res) == 3:
        lvl = (res[0] or "").lower()
        if lvl == "high":
            impacto = "CRITICO"
        elif lvl == "medium":
            impacto = "MEDIO"
        elif lvl == "low":
            impacto = "BAJO"
    elif isinstance(res, str):
        impacto = (res or "MEDIO").upper()

    if impacto == "CRITICO":
        return ("high", "🔴", "red")
    if impacto == "MEDIO":
        return ("medium", "🟡", "yellow")
    return ("low", "🟢", "green")


def append_live_item(item: dict) -> None:
    try:
        enriched = monitor.append_live_item(item)
        try:
            level = (enriched.get("level") or "").upper()
            cfg = {}
            try:
                cfg = storage.get_config("monitor_config") or {}
            except Exception:
                cfg = {}

            target_chat = None
            try:
                target_chat = cfg.get("telegram_target_chat") if isinstance(
                    cfg, dict) else None
            except Exception:
                target_chat = None
            if not target_chat:
                target_chat = os.environ.get("TELEGRAM_TARGET_CHAT_ID")

            alerts_enabled = False
            try:
                alerts_enabled = bool(cfg.get("telegram_alerts")) if isinstance(
                    cfg, dict) else False
            except Exception:
                alerts_enabled = False

            if level == "CRITICO" and target_chat and alerts_enabled:
                try:
                    from core import telegram as core_telegram
                    resp = core_telegram.send_item_notification(
                        enriched, str(target_chat))
                    if resp and isinstance(resp, dict) and resp.get("ok"):
                        try:
                            msg_id = None
                            try:
                                msg_id = resp.get(
                                    "result", {}).get("message_id")
                            except Exception:
                                msg_id = None
                            if msg_id and enriched.get("id"):
                                try:
                                    storage.set_tg_message_id(
                                        enriched.get("id"), msg_id)
                                except Exception:
                                    pass
                            log(
                                f"telegram_bot: sent immediate alert to {target_chat} for title={enriched.get('title')} message_id={msg_id}")
                        except Exception:
                            pass
                    else:
                        try:
                            log(
                                f"telegram_bot: failed to send immediate alert to {target_chat} resp={resp}", "ERROR")
                        except Exception:
                            pass
                except Exception as e:
                    try:
                        log_exc(
                            "telegram_bot: exception sending immediate alert", e)
                    except Exception:
                        pass
            else:
                try:
                    try:
                        monitor.publish_items([enriched])
                    except Exception:
                        try:
                            from web import realtime

                            realtime.publish_item(enriched)
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass
    except Exception:
        try:
            storage.append_item(item)
        except Exception:
            pass


def _store_search_results(search_id: str, results: list[dict]):
    storage.store_search_results(search_id, results)


def _load_search_results(search_id: str) -> list[dict[str, Any]]:
    try:
        res = storage.load_search_results(search_id)
        return res if res is not None else []
    except Exception:
        return []


def set_state(chat_id: str, obj: dict) -> None:
    try:
        obj = dict(obj)
        obj["ts"] = datetime.now(timezone.utc).isoformat()
    except Exception:
        pass
    storage.set_state(chat_id, obj)
    try:
        t = _state_timers.get(str(chat_id))
        if t:
            try:
                t.cancel()
            except Exception:
                pass
        timer = threading.Timer(3600, _expire_state, args=(str(chat_id),))
        timer.daemon = True
        timer.start()
        _state_timers[str(chat_id)] = timer
    except Exception:
        pass


def get_state(chat_id: str) -> dict[str, Any]:
    try:
        res = storage.get_state(chat_id)
        if not res:
            return {}
        ts = res.get("ts")
        if ts:
            try:
                dt = datetime.fromisoformat(ts)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                age = (datetime.now(timezone.utc) - dt).total_seconds()
                if age > 3600:
                    try:
                        clear_state(chat_id)
                    except Exception:
                        pass
                    return {}
            except Exception:
                pass
        return res if res is not None else {}
    except Exception:
        return {}


_state_timers: dict[str, threading.Timer] = {}


def _expire_state(chat_id: str) -> None:
    try:
        storage.clear_state(str(chat_id))
    except Exception:
        pass
    try:
        api_post("sendMessage", {
                 "chat_id": chat_id, "text": "Tu acción fue cancelada por inactividad (más de 1 hora). Usa /start para volver al menú."})
    except Exception:
        pass


def clear_state(chat_id: str) -> None:
    storage.clear_state(chat_id)


def perform_search(keyword: str, sources: list[str], limit: int) -> list[dict]:
    collected = []
    per_source_counts = {}
    try:
        cfg_global = storage.get_config("monitor_config") or {}
    except Exception:
        cfg_global = {}
    if os.environ.get("NEWS_API") or _HAS_NEWSMELT_ADAPTERS:
        if "newsapi" not in (sources or []):
            sources = ["newsapi"] + (sources or [])
        else:
            sources = ["newsapi"] + \
                [s for s in (sources or []) if s != "newsapi"]
    tokens = {
        "x": os.environ.get("X_BEARER_TOKEN"),
        "facebook": os.environ.get("FACEBOOK_TOKEN"),
        "instagram": os.environ.get("INSTAGRAM_TOKEN"),
        "instagram_user_id": os.environ.get("INSTAGRAM_USER_ID"),
    }
    try:
        sources = [s for s in (sources or []) if (s or "").lower() != "reddit"]
    except Exception:
        sources = [s for s in (sources or [])]
    try:
        cfg_global = cfg_global or (storage.get_config("monitor_config") or {})
    except Exception:
        cfg_global = cfg_global or {}
    try:
        use_location_filter = bool(cfg_global.get("use_location_filter"))
        loc = cfg_global.get("location") if isinstance(
            cfg_global, dict) else None
        loc_tokens = []
        if isinstance(loc, dict):
            for field in ("state", "municipality", "colony", "country"):
                try:
                    v = loc.get(field)
                except Exception:
                    v = None
                if v and isinstance(v, str):
                    phrase = v.strip()
                    if phrase:
                        loc_tokens.append(phrase)
                        parts = [p.strip() for p in re.split(
                            r"\W+", phrase) if p and len(p) > 2]
                        for p in parts:
                            loc_tokens.append(p)
        loc_tokens = list(dict.fromkeys([t for t in loc_tokens if t]))
        loc_tokens_l = [t.lower() for t in loc_tokens]
    except Exception:
        use_location_filter = False
        loc_tokens = []
        loc_tokens_l = []

    keyword = (keyword or "").strip()
    query_variants = []
    if keyword:
        query_variants.append(keyword)
        for t in loc_tokens:
            query_variants.append(f"{keyword} {t}")
    for t in loc_tokens:
        if t not in query_variants:
            query_variants.append(t)

    for s in sources:
        try:
            if s == "newsapi":
                try:
                    log(
                        f"telegram_bot: invocando NewsAPI para '{keyword}' limit={limit} | NEWS_API={'set' if os.environ.get('NEWS_API') else 'unset'}")
                except Exception:
                    pass
                try:
                    newsapi_opts = (cfg_global.get(
                        "source_options") or {}).get("newsapi", {})
                    try:
                        loc_country = (cfg_global.get(
                            "location") or {}).get("country")
                        if loc_country:
                            newsapi_opts = dict(newsapi_opts)
                            newsapi_opts["country"] = loc_country
                    except Exception:
                        pass
                    items = []
                    for q in query_variants or [keyword]:
                        try:
                            found = search_newsapi(
                                q, limit, newsapi_opts) or []
                            for f in found:
                                if isinstance(f, dict):
                                    f["matched_query"] = q
                                else:
                                    try:
                                        setattr(f, "matched_query", q)
                                    except Exception:
                                        pass
                            items.extend(found)
                        except Exception:
                            continue
                    items = items or []
                    collected.extend(items)
                    per_source_counts["newsapi"] = per_source_counts.get(
                        "newsapi", 0) + len(items)
                except Exception as e:
                    log_exc(f"telegram_bot: search_newsapi error: {e}", e)
                    continue
                continue
            if s == "google":
                items = []
                for q in query_variants or [keyword]:
                    try:
                        found = search_google_news(q, limit) or []
                        for f in found:
                            if isinstance(f, dict):
                                f["matched_query"] = q
                        items.extend(found)
                    except Exception:
                        continue
                items = items or []
                collected.extend(items)
                per_source_counts["google"] = per_source_counts.get(
                    "google", 0) + len(items)
            elif s == "bing":
                items = search_bing_news(keyword, limit) or []
                collected.extend(items)
                per_source_counts["bing"] = per_source_counts.get(
                    "bing", 0) + len(items)
            elif s == "hn":
                items = search_hacker_news(keyword, limit) or []
                collected.extend(items)
                per_source_counts["hn"] = per_source_counts.get(
                    "hn", 0) + len(items)
            elif s == "x":
                if tokens.get("x"):
                    items = search_x(keyword, limit, tokens.get("x")) or []
                    collected.extend(items)
                    per_source_counts["x"] = per_source_counts.get(
                        "x", 0) + len(items)
            elif s == "facebook":
                if tokens.get("facebook"):
                    items = search_facebook(
                        keyword, limit, tokens.get("facebook")) or []
                    collected.extend(items)
                    per_source_counts["facebook"] = per_source_counts.get(
                        "facebook", 0) + len(items)
            elif s == "instagram":
                if tokens.get("instagram") and tokens.get("instagram_user_id"):
                    items = search_instagram(keyword, limit, tokens.get(
                        "instagram"), tokens.get("instagram_user_id")) or []
                    collected.extend(items)
                    per_source_counts["instagram"] = per_source_counts.get(
                        "instagram", 0) + len(items)
        except Exception:
            continue
    norm = []
    for it in deduplicate(collected):
        if isinstance(it, dict):
            norm.append(it)
        else:
            try:
                norm.append({
                    "source": getattr(it, "source", ""),
                    "channel": getattr(it, "channel", ""),
                    "title": getattr(it, "title", ""),
                    "url": getattr(it, "url", ""),
                    "summary": getattr(it, "summary", ""),
                    "published_at": getattr(it, "published_at", ""),
                    "keyword": getattr(it, "keyword", ""),
                })
            except Exception:
                pass

    try:
        norm.sort(key=lambda it: 0 if (it.get("source", "")
                  or "").lower() == "newsapi" else 1)
    except Exception:
        pass

    from datetime import datetime, timezone, timedelta
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    day_cutoff = now - timedelta(days=1)
    week_cutoff = now - timedelta(days=7)

    def _parse_pub(s: str):
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            try:
                return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            except Exception:
                return None

    with_dates = []
    without_dates = []
    for it in norm:
        pub_dt = _parse_pub(it.get("published_at", "") or "")
        if pub_dt:
            with_dates.append((it, pub_dt))
        else:
            without_dates.append(it)

    day_matches = [it for it, dt in with_dates if dt >= day_cutoff]
    if day_matches:
        return day_matches[:limit]

    week_matches = [it for it, dt in with_dates if dt >= week_cutoff]
    if week_matches:
        return week_matches[:limit]

    result = [it for it, dt in sorted(
        with_dates, key=lambda x: x[1], reverse=True)][:limit]
    if len(result) < limit:
        result.extend(without_dates[: (limit - len(result))])
    try:
        desired_total = int(limit)
    except Exception:
        desired_total = limit

    if desired_total >= 10:
        desired_newsapi = min(5, desired_total)
        desired_other = desired_total - desired_newsapi

        all_candidates = result + \
            without_dates[: max(0, desired_total - len(result))]

        def src_name(it):
            return (it.get("source") or "").lower()

        newsapi_items = [
            it for it in all_candidates if src_name(it) == "newsapi"]
        other_items = [
            it for it in all_candidates if src_name(it) != "newsapi"]

        final = []
        for it in newsapi_items:
            if len(final) >= desired_newsapi:
                break
            final.append(it)
        for it in other_items:
            if len(final) >= desired_total:
                break
            final.append(it)
        if len(final) < desired_total:
            for it in newsapi_items:
                if it in final:
                    continue
                final.append(it)
                if len(final) >= desired_total:
                    break

        return final[:desired_total]

    try:
        log(
            f"telegram_bot: fetched total candidates={len(collected)} unique_after_dedupe={len(result)} per_source_counts={per_source_counts}")
    except Exception:
        pass

    return result


def _format_pub_date(pub_str: str) -> str:
    if not pub_str:
        return ""
    from datetime import datetime, timezone
    try:
        dt = datetime.fromisoformat(pub_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%H:%M - %d/%m/%Y")
    except Exception:
        try:
            dt = datetime.strptime(
                pub_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            return dt.strftime("%H:%M - %d/%m/%Y")
        except Exception:
            return ""


def _render_search_page(results: list[dict], page_idx: int, keyword: str, search_id: str, page_size: int = 5):
    start = page_idx * page_size
    end = start + page_size
    page_items = results[start:end]

    keyboard = []
    sel_row = []
    for offset, it in enumerate(page_items):
        abs_idx = start + offset
        sel_row.append(
            {"text": f"{abs_idx+1}", "callback_data": f"select:{search_id}:{abs_idx}"})
    if sel_row:
        keyboard.append(sel_row)

    b64_kw = base64.urlsafe_b64encode(keyword.encode()).decode()
    nav = []
    if page_idx > 0:
        nav.append({"text": "⬅️ Anterior",
                   "callback_data": f"page:{search_id}:{page_idx-1}:{b64_kw}"})
    if end < len(results):
        nav.append({"text": "Siguiente ➡️",
                   "callback_data": f"page:{search_id}:{page_idx+1}:{b64_kw}"})
    nav.append({"text": "Reintentar", "callback_data": f"retry:{b64_kw}"})
    keyboard.append(nav)

    lines = [
        f"Resultados para: {keyword} (mostrando {len(page_items)} de {len(results)})", ""]
    for idx, it in enumerate(page_items, start=start+1):
        title = (it.get("title") or "(sin título)").strip()
        pub = (it.get("published_at") or "")
        pub_f = _format_pub_date(pub)
        emoji = it.get("emoji") or ""
        lvl = it.get("level") or ""
        compact = ""
        src = it.get("source") or ""
        lines.append(f"{idx}) {emoji} {lvl} - {title}")
        if pub_f:
            lines.append(f"   Fecha: {pub_f}")
        if compact:
            lines.append(f"   {compact}")
        if src:
            lines.append(f"   Fuente: {src}")
        url_btn = it.get("url") or ""
        if url_btn:
            safe_title = title.replace("[", " ").replace(
                "]", " ").replace("(", " ").replace(")", " ")
            label = safe_title[:60].rstrip() or "fuente"
            lines.append(f"   [{label}]({url_btn})")
        lines.append("")

    payload = {"inline_keyboard": keyboard}
    return ("\n".join(lines), payload)


def send_inline_search_results(chat_id: str, keyword: str, results: list[dict]):
    PAGE_SIZE = 5
    page = 0
    search_id = str(int(time.time() * 1000))
    enriched_results: list[dict] = []
    for it in results:
        try:
            cls = classifier.classify_text((it.get("summary") or ""), title=(
                it.get("title") or ""), keyword=(it.get("keyword") or ""))
        except Exception:
            cls = None

        impacto = None
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
        elif isinstance(cls, str):
            impacto = (cls or "MEDIO").upper()
        if impacto is None:
            impacto = "MEDIO"

        emoji = "🔴" if impacto == "CRITICO" else (
            "🟠" if impacto == "MEDIO" else "🟢")
        color = "rojo" if impacto == "CRITICO" else (
            "naranja" if impacto == "MEDIO" else "verde")

        enriched = dict(it)
        enriched["level"] = impacto
        enriched["emoji"] = emoji
        enriched["color"] = color
        if isinstance(cls, dict):
            enriched["meta"] = cls

        enriched_results.append(enriched)

    _store_search_results(search_id, enriched_results)

    text_body, payload = _render_search_page(
        enriched_results, page, keyword, search_id, PAGE_SIZE)
    try:
        log(
            f"telegram_bot: sending initial keyboard payload: {json.dumps(payload)}")
    except Exception:
        pass
    api_post("sendMessage", {
        "chat_id": chat_id,
        "text": text_body,
        "reply_markup": json.dumps(payload),
        "parse_mode": "Markdown",
    })


def send_start_menu(chat_id: str):
    keyboard = {
        "inline_keyboard": [
            [
                {"text": "🔎 Buscar", "callback_data": "cmd:search"},
                {"text": "➕ Agregar noticia", "callback_data": "cmd:addnews"},
            ],
            [
                {"text": "ℹ️ Ayuda", "callback_data": "cmd:help"}
            ],
        ]
    }
    api_post("sendMessage", {
             "chat_id": chat_id, "text": "Elige una opción:", "reply_markup": json.dumps(keyboard)})


def _handle_callback_query(cq: dict):
    cq_id = cq.get("id")
    data = cq.get("data", "")
    message = cq.get("message", {})
    chat = message.get("chat", {})
    chat_id = str(chat.get("id"))

    if data == "cancel":
        try:
            clear_state(chat_id)
        except Exception:
            pass
        try:
            api_post("answerCallbackQuery", {
                     "callback_query_id": cq_id, "text": "Acción cancelada.", "show_alert": False})
        except Exception:
            pass
        try:
            api_post("deleteMessage", {
                     "chat_id": chat_id, "message_id": message.get("message_id")})
        except Exception:
            pass
        return
    if data.startswith("select:"):
        try:
            _, search_id, idx = data.split(":")
            idx = int(idx)
            results = _load_search_results(search_id)
            if 0 <= idx < len(results):
                it = results[idx]
                try:
                    if not (it.get("matched_keyword") or it.get("matched_location")):
                        api_post("answerCallbackQuery", {
                            "callback_query_id": cq_id,
                            "text": "Este resultado no cumple con los filtros de palabra clave ni de ubicación; no se añadirá.",
                            "show_alert": False,
                        })
                        return
                except Exception:
                    pass
                level, emoji, color = detect_level(
                    (it.get("title", "") + " " + it.get("summary", "")).strip())
                item = {
                    "source": it.get("source", "telegram_search"),
                    "title": it.get("title", "(sin titulo)"),
                    "url": it.get("url", ""),
                    "summary": it.get("summary", ""),
                    "published_at": it.get("published_at") or time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                    "keyword": it.get("keyword", ""),
                    "level": level,
                    "emoji": emoji,
                    "color": color,
                    "meta": {"method": "user_select"},
                    "origin": "telegram_search",
                }
                append_live_item(item)
                api_post("answerCallbackQuery", {
                         "callback_query_id": cq_id, "text": f"Seleccionado: {item['title']}", "show_alert": False})
                api_post("sendMessage", {
                         "chat_id": chat_id, "text": f"{emoji} Seleccionado: {item['title']}\nFuente: {item.get('source')}"})
                return
        except Exception:
            pass
        api_post("answerCallbackQuery", {
                 "callback_query_id": cq_id, "text": "Selección inválida", "show_alert": False})
        return

    if data.startswith("retry:"):
        try:
            b = data.split(":", 1)[1]
            keyword = base64.urlsafe_b64decode(b.encode()).decode()
            try:
                cfg = storage.get_config("monitor_config") or {}
                sources = cfg.get("sources", ["google", "bing", "hn"]) or [
                    "google", "bing", "hn"]
                limit = int(cfg.get("limit", 10))
            except Exception:
                sources = ["google", "bing", "hn"]
                limit = 10

            results = perform_search(keyword, sources, limit)
            send_inline_search_results(chat_id, keyword, results)
            api_post("answerCallbackQuery", {
                     "callback_query_id": cq_id, "text": "Reintentando búsqueda...", "show_alert": False})
            return
        except Exception:
            api_post("answerCallbackQuery", {
                     "callback_query_id": cq_id, "text": "Error al reintentar", "show_alert": False})
            return

    if data.startswith("cmd:"):
        try:
            cmd = data.split(":", 1)[1]
            if cmd == "addnews":
                api_post("answerCallbackQuery", {
                         "callback_query_id": cq_id, "text": "Envía la noticia (texto o URL) para agregar.", "show_alert": False})
                cancel_kb = {"inline_keyboard": [
                    [{"text": "Cancelar", "callback_data": "cancel"}]]}
                api_post("sendMessage", {
                         "chat_id": chat_id, "text": "Por favor envía la noticia ahora. Después podrás clasificarla con botones.", "reply_markup": json.dumps(cancel_kb)})
                set_state(str(chat_id), {"action": "awaiting_news"})
                return
            if cmd == "search":
                api_post("answerCallbackQuery", {
                    "callback_query_id": cq_id,
                    "text": "Envía la palabra clave para buscar.",
                    "show_alert": False,
                })
                cancel_kb = {"inline_keyboard": [
                    [{"text": "Cancelar", "callback_data": "cancel"}]]}
                api_post("sendMessage", {
                         "chat_id": chat_id, "text": "Escribe la palabra clave que quieres buscar (ej: 'vacunas, elecciones').", "reply_markup": json.dumps(cancel_kb)})
                set_state(str(chat_id), {"action": "awaiting_search"})
                return
            if cmd == "help":
                api_post("answerCallbackQuery", {
                    "callback_query_id": cq_id,
                    "text": "Mostrando ayuda.",
                    "show_alert": False,
                })
                help_text = (
                    "El bot ahora usa exclusivamente el menú de /start.\n"
                    "Pulsa '🔎 Buscar' para buscar o '➕ Agregar noticia' para añadir una noticia."
                )
                api_post("sendMessage", {
                         "chat_id": chat_id, "text": help_text})
                return
        except Exception:
            api_post("answerCallbackQuery", {
                     "callback_query_id": cq_id, "text": "Comando no reconocido", "show_alert": False})
            return

    if data.startswith("classify:"):
        try:
            _, choice = data.split(":", 1)
            st = get_state(chat_id)
            if choice in ("high", "medium", "low"):
                if not st or st.get("action") not in ("confirm_classify",):
                    api_post("answerCallbackQuery", {
                             "callback_query_id": cq_id, "text": "No hay noticia pendiente para clasificar.", "show_alert": False})
                    return
                st["level"] = choice
                set_state(chat_id, st)
                news_text = st.get("text", "").strip()
                emoji = "🔴" if choice == "high" else (
                    "🟡" if choice == "medium" else "🟢")
                preview = f"Vista previa:\n{emoji} <b>{(choice or 'UNKNOWN').upper()}</b> - {news_text[:200]}"
                kb = {"inline_keyboard": [[{"text": "Confirmar", "callback_data": "classify:confirm"}, {"text": "Editar texto", "callback_data": "classify:edit"}], [
                    {"text": "Añadir/Modificar URL (opcional)", "callback_data": "classify:addurl"}, {"text": "Cancelar", "callback_data": "classify:cancel"}]]}
                api_post("answerCallbackQuery", {
                         "callback_query_id": cq_id, "text": "Nivel seleccionado.", "show_alert": False})
                api_post("sendMessage", {"chat_id": chat_id, "text": preview,
                         "parse_mode": "HTML", "reply_markup": json.dumps(kb)})
                return

            if choice == "confirm":
                if not st or "level" not in st:
                    api_post("answerCallbackQuery", {
                             "callback_query_id": cq_id, "text": "Nada que confirmar.", "show_alert": False})
                    return
                level = st.get("level")
                news_text = st.get("text", "").strip()
                url = st.get("url", "")
                emoji = "🔴" if level == "high" else (
                    "🟡" if level == "medium" else "🟢")
                color = "red" if level == "high" else (
                    "yellow" if level == "medium" else "green")
                item = {
                    "source": "telegram",
                    "title": (news_text[:200] or "(sin titulo)"),
                    "url": url or "",
                    "summary": "",
                    "published_at": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                    "keyword": "",
                    "level": level,
                    "emoji": emoji,
                    "color": color,
                    "meta": {"method": "bot_add_manual"},
                    "origin": "telegram_manual",
                }
                append_live_item(item)
                clear_state(chat_id)
                api_post("answerCallbackQuery", {
                         "callback_query_id": cq_id, "text": f"Noticia agregada como {(level or 'UNKNOWN').upper()}.", "show_alert": False})
                api_post("sendMessage", {
                         "chat_id": chat_id, "text": f"{emoji} Noticia agregada: {item['title']}"})
                return

            if choice == "addurl":
                if not st:
                    api_post("answerCallbackQuery", {
                             "callback_query_id": cq_id, "text": "No hay noticia pendiente.", "show_alert": False})
                    return
                set_state(chat_id, {**st, "action": "awaiting_url"})
                api_post("answerCallbackQuery", {
                         "callback_query_id": cq_id, "text": "Envía la URL ahora (o escribe 'omit' para omitir).", "show_alert": False})
                api_post("sendMessage", {
                         "chat_id": chat_id, "text": "Envía la URL que quieres asociar (o escribe 'omit' para omitir)."})
                return

            if choice == "edit":
                if not st:
                    api_post("answerCallbackQuery", {
                             "callback_query_id": cq_id, "text": "No hay noticia para editar.", "show_alert": False})
                    return
                set_state(chat_id, {**st, "action": "awaiting_edit"})
                api_post("answerCallbackQuery", {
                         "callback_query_id": cq_id, "text": "Envía el texto actualizado.", "show_alert": False})
                api_post("sendMessage", {
                         "chat_id": chat_id, "text": "Envía el texto actualizado de la noticia."})
                return

            if choice == "cancel":
                clear_state(chat_id)
                api_post("answerCallbackQuery", {
                         "callback_query_id": cq_id, "text": "Operación cancelada.", "show_alert": False})
                api_post("sendMessage", {
                         "chat_id": chat_id, "text": "Se canceló la adición de la noticia."})
                return
            if choice == "removeurl":
                if not st:
                    api_post("answerCallbackQuery", {
                             "callback_query_id": cq_id, "text": "No hay noticia pendiente.", "show_alert": False})
                    return
                st.pop("url", None)
                st["action"] = "confirm_details"
                set_state(chat_id, st)
                lvl = st.get("level", "low")
                emoji = "🔴" if lvl == "high" else (
                    "🟡" if lvl == "medium" else "🟢")
                preview = f"Vista previa:\n{emoji} <b>{(lvl or 'LOW').upper()}</b> - {st.get('text', '')[:200]}\nURL: (sin URL)"
                kb = {"inline_keyboard": [[{"text": "Confirmar", "callback_data": "classify:confirm"}, {"text": "Editar texto", "callback_data": "classify:edit"}], [
                    {"text": "Añadir URL", "callback_data": "classify:addurl"}, {"text": "Cancelar", "callback_data": "classify:cancel"}]]}
                api_post("answerCallbackQuery", {
                         "callback_query_id": cq_id, "text": "URL eliminada.", "show_alert": False})
                api_post("sendMessage", {"chat_id": chat_id, "text": preview,
                         "parse_mode": "HTML", "reply_markup": json.dumps(kb)})
                return
        except Exception:
            api_post("answerCallbackQuery", {
                     "callback_query_id": cq_id, "text": "Error procesando acción.", "show_alert": False})
            return

    if data.startswith("page:"):
        try:
            parts = data.split(":", 3)
            if len(parts) < 4:
                api_post("answerCallbackQuery", {
                         "callback_query_id": cq_id, "text": "Página inválida", "show_alert": False})
                return
            _, search_id, page_idx, b64_kw = parts
            page_idx = int(page_idx)
            results = _load_search_results(search_id)
            if not results:
                api_post("answerCallbackQuery", {
                    "callback_query_id": cq_id,
                    "text": "Resultados de búsqueda expirados o no válidos.",
                    "show_alert": False,
                })
                return

            try:
                keyword = base64.urlsafe_b64decode(b64_kw.encode()).decode()
            except Exception:
                keyword = ""

            text_body, payload = _render_search_page(
                results, page_idx, keyword, search_id)
            try:
                log(
                    f"telegram_bot: editing message with payload: {json.dumps(payload)}")
            except Exception:
                pass
            api_post("editMessageText", {
                "chat_id": chat_id,
                "message_id": message.get("message_id"),
                "text": text_body,
                "reply_markup": json.dumps(payload),
                "parse_mode": "Markdown",
            })
            api_post("answerCallbackQuery", {
                     "callback_query_id": cq_id, "text": "Página actualizada", "show_alert": False})
            return
        except Exception as e:
            api_post("answerCallbackQuery", {
                     "callback_query_id": cq_id, "text": "No se pudo cambiar de página", "show_alert": False})
            return

    api_post("answerCallbackQuery", {
             "callback_query_id": cq_id, "text": "Acción no reconocida", "show_alert": False})


def handle_message(update: dict) -> None:
    cq = update.get("callback_query")
    if cq:
        _handle_callback_query(cq)
        return

    message = update.get("message") or update.get("edited_message")
    if not message:
        return
    chat = message.get("chat", {})
    chat_id = str(chat.get("id"))
    text = message.get("text") or message.get("caption") or ""
    if not text and message.get("entities"):
        text = "(sin texto)"

    lower = text.strip().lower()

    state = get_state(chat_id)
    if state and state.get("action") == "awaiting_news":
        news_text = text.strip()
        set_state(chat_id, {"action": "confirm_classify", "text": news_text})
        kb = {"inline_keyboard": [[{"text": "🔴 Alta", "callback_data": "classify:high"}, {"text": "🟡 Media", "callback_data": "classify:medium"}, {
            "text": "🟢 Baja", "callback_data": "classify:low"}], [{"text": "Cancelar", "callback_data": "classify:cancel"}]]}
        api_post("sendMessage", {
                 "chat_id": chat_id, "text": "Recibido. Elige nivel para clasificar esta noticia:", "reply_markup": json.dumps(kb)})
        return

    if state and state.get("action") == "awaiting_search":
        keyword = text.strip()
        clear_state(chat_id)
        try:
            cfg = storage.get_config("monitor_config") or {}
            sources = cfg.get("sources", ["google", "bing", "hn"]) or [
                "google", "bing", "hn"]
            limit = int(cfg.get("limit", 10))
        except Exception:
            sources = ["google", "bing", "hn"]
            limit = 10

        results = perform_search(keyword, sources, limit)
        if not results:
            api_post("sendMessage", {"chat_id": chat_id,
                     "text": "No se encontraron resultados."})
            return
        send_inline_search_results(chat_id, keyword, results)
        return

    if state and state.get("action") == "awaiting_edit":
        new_text = text.strip()
        state["text"] = new_text
        state["action"] = "confirm_details"
        set_state(chat_id, state)
        lvl = state.get("level", "low")
        emoji = "🔴" if lvl == "high" else ("🟡" if lvl == "medium" else "🟢")
        preview = f"Vista previa:\n{emoji} <b>{(lvl or 'LOW').upper()}</b> - {new_text[:200]}"
        kb = {"inline_keyboard": [[{"text": "Confirmar", "callback_data": "classify:confirm"}, {"text": "Editar texto", "callback_data": "classify:edit"}], [
            {"text": "Añadir/Modificar URL (opcional)", "callback_data": "classify:addurl"}, {"text": "Cancelar", "callback_data": "classify:cancel"}]]}
        api_post("sendMessage", {"chat_id": chat_id, "text": preview,
                 "parse_mode": "HTML", "reply_markup": json.dumps(kb)})
        return

    if state and state.get("action") == "awaiting_url":
        url_text = text.strip()
        if url_text.lower() == "omit":
            state.pop("url", None)
        else:
            state["url"] = url_text
        state["action"] = "confirm_details"
        set_state(chat_id, state)
        lvl = state.get("level", "low")
        emoji = "🔴" if lvl == "high" else ("🟡" if lvl == "medium" else "🟢")
        preview = f"Vista previa:\n{emoji} <b>{(lvl or 'LOW').upper()}</b> - {state.get('text', '')[:200]}\nURL: {state.get('url', '(sin URL)')}"
        kb = {"inline_keyboard": [[{"text": "Confirmar", "callback_data": "classify:confirm"}, {"text": "Editar texto", "callback_data": "classify:edit"}], [
            {"text": "Quitar URL", "callback_data": "classify:removeurl"}, {"text": "Cancelar", "callback_data": "classify:cancel"}]]}
        api_post("sendMessage", {"chat_id": chat_id, "text": preview,
                 "parse_mode": "HTML", "reply_markup": json.dumps(kb)})
        return

    if lower.startswith("/classify "):
        try:
            api_post("sendMessage", {
                     "chat_id": chat_id, "text": "Los comandos textuales han sido desactivados. Usa /start y el menú para buscar o agregar noticias."})
        except Exception:
            pass
        return

    if lower.startswith("/start"):
        send_start_menu(chat_id)
        return

    if lower.startswith("/level ") or lower.startswith("/setlevel "):
        try:
            api_post("sendMessage", {
                     "chat_id": chat_id, "text": "Los comandos de nivel han sido desactivados. Usa /start y el menú para clasificar o añadir noticias."})
        except Exception:
            pass
        return

    if lower.startswith("/search "):
        try:
            api_post("sendMessage", {
                     "chat_id": chat_id, "text": "El comando /search está desactivado. Usa /start y pulsa '🔎 Buscar' para buscar por palabra clave."})
        except Exception:
            pass
        return

    try:
        if (chat.get("type") or "").lower() == "private":
            api_post("sendMessage", {
                     "chat_id": chat_id, "text": "Para agregar noticias usa /start y pulsa '➕ Agregar noticia', o usa /search <palabra> para buscar."})
    except Exception:
        pass
    return


def get_offset() -> int:
    if os.path.exists(OFFSET_FILE):
        try:
            with open(OFFSET_FILE, "r", encoding="utf-8") as f:
                return int(f.read().strip() or 0)
        except Exception:
            return 0
    return 0


def save_offset(offset: int) -> None:
    with open(OFFSET_FILE, "w", encoding="utf-8") as f:
        f.write(str(offset))


def poll_updates(poll_interval: int = 2) -> None:
    offset = get_offset()
    log("Iniciando poller de Telegram... (CTRL+C para salir)")
    while True:
        try:
            url = f"{API}/getUpdates?timeout=20&offset={offset + 1}"
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            for upd in data.get("result", []):
                offset = max(offset, upd.get("update_id", 0))
                try:
                    handle_message(upd)
                except Exception as exc:
                    log_exc(f"Error procesando mensaje: {exc}", exc)
            save_offset(offset)
        except KeyboardInterrupt:
            log("Interrumpido por usuario")
            break
        except Exception as exc:
            log_exc(f"Error en getUpdates: {exc}", exc)
            time.sleep(poll_interval)


if __name__ == "__main__":
    try:
        init_bot()
    except Exception:
        pass
    poll_updates()
