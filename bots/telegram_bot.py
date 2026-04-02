from __future__ import annotations

from core.news_finder import (
    search_all_sources,
)
from core import classifier
import os
import time
import json
import requests
import base64
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional, Any
from core import storage
from monitors import monitor
from core.logger import log, log_exc
import threading
from datetime import datetime, timedelta
from core.timezone_mx import MX_TZ, now_mx, now_mx_iso

ROOT = Path(__file__).resolve().parents[1]
env_root = ROOT / ".env"
if env_root.exists():
    load_dotenv(env_root)
else:
    load_dotenv()


BOT_TOKEN = os.environ.get("BOT_TOKEN")
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
        impacto = str(res.get("impacto") or "medio").strip().lower()
    elif isinstance(res, tuple) and len(res) == 3:
        lvl = (res[0] or "").lower()
        if lvl == "alto":
            impacto = "alto"
        elif lvl == "medio":
            impacto = "medio"
        elif lvl == "bajo":
            impacto = "bajo"
    elif isinstance(res, str):
        impacto = str(res or "medio").strip().lower()

    if impacto not in ("alto", "medio", "bajo"):
        impacto = "medio"

    if impacto == "alto":
        return ("alto", "🔴", "rojo")
    if impacto == "medio":
        return ("medio", "🟠", "naranja")
    return ("bajo", "🟢", "verde")


def append_live_item(item: dict) -> None:
    try:
        enriched = monitor.append_live_item(item)
        try:
            cfg = {}
            try:
                cfg = storage.get_config("monitor_config") or {}
            except Exception:
                cfg = {}

            item_id = None
            try:
                item_id = int(enriched.get("id") or 0) or None
            except Exception:
                item_id = None

            try:
                from core import telegram as core_telegram

                send_results = core_telegram.send_item_notification_to_targets(
                    enriched,
                    cfg=cfg,
                    item_id=item_id,
                )
                if item_id:
                    for r in send_results:
                        if r.get("ok") and r.get("message_id"):
                            try:
                                storage.set_tg_message_id(
                                    item_id, r.get("message_id"))
                            except Exception:
                                pass
                            break
            except Exception as e:
                try:
                    log_exc("telegram_bot: exception sending alerts", e)
                except Exception:
                    pass

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


def _cancel_keyboard() -> dict:
    return {"inline_keyboard": [[{"text": "Cancelar", "callback_data": "cancel"}]]}


def _send_temp_message(chat_id: str, text: str, seconds: int = 6, parse_mode: str | None = None) -> None:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    api_post("sendMessage", payload)


def set_state(chat_id: str, obj: dict) -> None:
    try:
        obj = dict(obj)
        obj["ts"] = now_mx_iso()
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
                    dt = dt.replace(tzinfo=MX_TZ)
                age = (now_mx() - dt).total_seconds()
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
        clear_state(str(chat_id))
    except Exception:
        pass
    try:
        _send_temp_message(
            str(chat_id),
            "Tu acción fue cancelada por inactividad (más de 1 hora). Usa /start para volver al menú.",
            seconds=8,
        )
    except Exception:
        pass


def clear_state(chat_id: str) -> None:
    try:
        storage.clear_state(chat_id)
    except Exception:
        pass
    try:
        t = _state_timers.pop(str(chat_id), None)
        if t:
            try:
                t.cancel()
            except Exception:
                pass
    except Exception:
        pass


def perform_search(keyword: str, sources: list[str], limit: int) -> list[dict]:
    try:
        cfg_global = storage.get_config("monitor_config") or {}
    except Exception:
        cfg_global = {}

    try:
        cfg_local = dict(cfg_global)
        cfg_local["sources"] = [
            s for s in (sources or cfg_global.get("sources") or [])
            if (s or "").lower() != "reddit"
        ]
    except Exception:
        cfg_local = cfg_global or {}

    try:
        safe_limit = max(1, min(int(limit or 10), 10))
    except Exception:
        safe_limit = 10

    now_local = now_mx()
    start_today_utc = now_local.replace(
        hour=0, minute=0, second=0, microsecond=0)
    week_start_utc = now_local - timedelta(days=7)

    try:
        today_results = search_all_sources(
            limit=safe_limit,
            keyword=keyword,
            cfg=cfg_local,
            window_start=start_today_utc,
            window_end=now_local,
            strict_window=True,
            include_location_only_when_keyword=False,
            prefer_specific_location_first=True,
            keyword_with_location_only=True,
        )
        if today_results:
            return today_results[:safe_limit]

        week_results = search_all_sources(
            limit=safe_limit,
            keyword=keyword,
            cfg=cfg_local,
            window_start=week_start_utc,
            window_end=now_local,
            strict_window=True,
            include_location_only_when_keyword=False,
            prefer_specific_location_first=True,
            keyword_with_location_only=True,
        )
        return week_results[:safe_limit]
    except Exception as e:
        log_exc(f"telegram_bot: search_all_sources failed: {e}", e)
        return []


def _format_pub_date(pub_str: str) -> str:
    if not pub_str:
        return ""
    try:
        dt = datetime.fromisoformat(pub_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MX_TZ)
        return dt.astimezone(MX_TZ).strftime("%H:%M - %d/%m/%Y")
    except Exception:
        try:
            dt = datetime.strptime(
                pub_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=MX_TZ)
            return dt.astimezone(MX_TZ).strftime("%H:%M - %d/%m/%Y")
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
    nav.append({"text": "Cancelar", "callback_data": "cancel"})
    keyboard.append(nav)

    lines = [
        f"Resultados para: {keyword} (mostrando {len(page_items)} de {len(results)})",
        "Pulsa un numero para seleccionar una noticia.",
        "",
    ]
    for idx, it in enumerate(page_items, start=start+1):
        title = (it.get("title") or "(sin título)").strip()
        pub = (it.get("published_at") or "")
        pub_f = _format_pub_date(pub)
        lvl = it.get("level") or ""
        classification = str(it.get("classification")
                             or lvl or "medio").strip().lower()
        if classification not in ("alto", "medio", "bajo"):
            classification = "medio"
        emoji = it.get("emoji") or (
            "🔴" if classification == "alto" else (
                "🟠" if classification == "medio" else "🟢")
        )
        compact = ""
        src = it.get("source") or ""
        lines.append(f"{idx}) {emoji} {title}")
        lines.append(f"   Clasificación: {emoji} {classification}")
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
            impacto = str(cls.get("impacto") or "medio").strip().lower()
        elif isinstance(cls, tuple) and len(cls) == 3:
            lvl = (cls[0] or "").lower()
            if lvl == "alto":
                impacto = "alto"
            elif lvl == "medio":
                impacto = "medio"
            elif lvl == "bajo":
                impacto = "bajo"
        elif isinstance(cls, str):
            impacto = str(cls or "medio").strip().lower()
        if impacto is None:
            impacto = "medio"

        if impacto not in ("alto", "medio", "bajo"):
            impacto = "medio"

        emoji = "🔴" if impacto == "alto" else (
            "🟠" if impacto == "medio" else "🟢")
        color = "rojo" if impacto == "alto" else (
            "naranja" if impacto == "medio" else "verde")

        enriched = dict(it)
        enriched["level"] = impacto
        enriched["classification"] = impacto
        enriched["emoji"] = emoji
        enriched["color"] = color
        if isinstance(cls, dict):
            enriched["meta"] = cls
            try:
                just = (cls.get("justificacion") or "").strip()
            except Exception:
                just = ""
            if just:
                enriched["classification_reason"] = just
            try:
                risk = cls.get("nivel_riesgo")
            except Exception:
                risk = None
            if risk is not None:
                enriched["risk_score"] = risk

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
    message = cq.get("message", {}) or {}
    chat = message.get("chat", {}) or {}
    chat_id = str(chat.get("id"))
    st = get_state(chat_id)

    if data == "cancel":
        clear_state(chat_id)
        api_post("answerCallbackQuery", {
                 "callback_query_id": cq_id, "text": "Operación cancelada.", "show_alert": False})
        return

    if data.startswith("select:"):
        try:
            _, search_id, idx = data.split(":")
            idx = int(idx)
            results = _load_search_results(search_id)
            if 0 <= idx < len(results):
                it = results[idx]
                classification = str(
                    it.get("classification") or it.get("level") or "").strip().lower()
                risk_score = it.get("risk_score")
                if classification not in ("alto", "medio", "bajo"):
                    classification = "medio"

                if classification not in ("alto", "medio", "bajo"):
                    level, emoji, color = detect_level(
                        (it.get("title", "") + " " + it.get("summary", "")).strip())
                    classification = level
                else:
                    level = classification
                    emoji = it.get("emoji") or (
                        "🔴" if classification == "alto" else (
                            "🟠" if classification == "medio" else "🟢")
                    )
                    color = "rojo" if classification == "alto" else (
                        "naranja" if classification == "medio" else "verde")

                meta_payload: dict[str, Any] = {
                    "method": "user_select",
                    "classification": classification,
                }
                if risk_score not in (None, ""):
                    meta_payload["risk_score"] = risk_score
                item = {
                    "source": it.get("source", "telegram_search"),
                    "title": it.get("title", "(sin titulo)"),
                    "url": it.get("url", ""),
                    "summary": it.get("summary", ""),
                    "published_at": it.get("published_at") or now_mx_iso(),
                    "keyword": it.get("keyword", ""),
                    "level": level,
                    "classification": classification,
                    "emoji": emoji,
                    "color": color,
                    "meta": meta_payload,
                    "origin": "telegram_search",
                    "ingested_by": "telegram_search",
                }

                existing_id = None
                try:
                    existing_id = storage.find_existing_item_id(
                        item.get("url"), item.get("title"))
                except Exception:
                    existing_id = None

                append_live_item(item)
                clear_state(chat_id)
                api_post("answerCallbackQuery", {
                         "callback_query_id": cq_id, "text": f"Seleccionado: {item['title']}", "show_alert": False})
                try:
                    from core import telegram as core_telegram
                    selected_text = core_telegram.format_item_message(
                        item, prefix="Seleccionado")
                except Exception:
                    selected_text = (
                        f"{emoji} Seleccionado: {item['title']}\n"
                        f"Fuente: {item.get('source')}\n"
                        f"Clasificación: {emoji} {classification}"
                    )
                    summary = str(item.get("summary") or "").strip()
                    if summary:
                        if len(summary) > 400:
                            summary = summary[:400].rsplit(" ", 1)[0] + "..."
                        selected_text += f"\nResumen: {summary}"

                if existing_id is not None:
                    selected_text += "\n\n⚠️ Esta noticia ya estaba registrada y no se guardó de nuevo."

                api_post("sendMessage", {
                         "chat_id": chat_id, "text": selected_text, "parse_mode": "Markdown"})
                return
        except Exception:
            pass
        api_post("answerCallbackQuery", {
                 "callback_query_id": cq_id, "text": "Selección inválida", "show_alert": False})
        return

    if data.startswith("retry:"):
        try:
            api_post("answerCallbackQuery", {
                     "callback_query_id": cq_id, "text": "Buscando de nuevo...", "show_alert": False})
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
            if not results:
                _send_temp_message(
                    chat_id,
                    "No se encontraron resultados.",
                    seconds=6,
                )
                return
            send_inline_search_results(chat_id, keyword, results)
            clear_state(chat_id)
            return
        except Exception:
            api_post("answerCallbackQuery", {
                     "callback_query_id": cq_id, "text": "Error al reintentar", "show_alert": False})
            return

    if data.startswith("cmd:"):
        try:
            cmd = data.split(":", 1)[1]
            if cmd == "addnews":
                clear_state(chat_id)
                api_post("answerCallbackQuery", {
                         "callback_query_id": cq_id, "text": "Envía la noticia para agregar.", "show_alert": False})
                api_post("sendMessage", {
                    "chat_id": chat_id,
                    "text": "Envía la noticia ahora. Puedes cancelar en cualquier momento.",
                    "reply_markup": json.dumps(_cancel_keyboard()),
                })
                new_state = {"action": "awaiting_news"}
                set_state(str(chat_id), new_state)
                return

            if cmd == "search":
                clear_state(chat_id)
                api_post("answerCallbackQuery", {
                    "callback_query_id": cq_id,
                    "text": "Envía la palabra clave para buscar.",
                    "show_alert": False,
                })
                api_post("sendMessage", {
                    "chat_id": chat_id,
                    "text": "Escribe la palabra clave que quieres buscar (ej: vacunas, elecciones).",
                    "reply_markup": json.dumps(_cancel_keyboard()),
                })
                new_state = {"action": "awaiting_search"}
                set_state(str(chat_id), new_state)
                return

            if cmd == "help":
                api_post("answerCallbackQuery", {
                    "callback_query_id": cq_id,
                    "text": "Mostrando ayuda.",
                    "show_alert": False,
                })
                help_text = (
                    "Usa /start para abrir el menú.\n"
                    "Buscar: abre búsqueda guiada con cancelación.\n"
                    "Agregar noticia: te guía paso a paso y puedes cancelar."
                )
                api_post("sendMessage", {
                         "chat_id": chat_id, "text": help_text})
                return
        except Exception:
            api_post("answerCallbackQuery", {
                     "callback_query_id": cq_id, "text": "Comando no reconocido.", "show_alert": False})
            return

    if data.startswith("classify:"):
        try:
            _, choice = data.split(":", 1)
            st = get_state(chat_id)

            if choice in ("alto", "medio", "bajo"):
                if not st or st.get("action") not in ("confirm_classify", "confirm_details"):
                    api_post("answerCallbackQuery", {
                             "callback_query_id": cq_id, "text": "No hay noticia pendiente para clasificar.", "show_alert": False})
                    return
                st["level"] = choice
                st["action"] = "confirm_details"
                news_text = st.get("text", "").strip()
                emoji = "🔴" if choice == "alto" else (
                    "🟠" if choice == "medio" else "🟢")
                preview = f"Vista previa:\n{emoji} <b>{choice}</b> - {news_text[:200]}"
                kb = {"inline_keyboard": [[{"text": "Confirmar", "callback_data": "classify:confirm"}, {"text": "Editar texto", "callback_data": "classify:edit"}], [
                    {"text": "Añadir/Modificar URL (opcional)", "callback_data": "classify:addurl"}, {"text": "Cancelar", "callback_data": "classify:cancel"}]]}
                api_post("answerCallbackQuery", {
                         "callback_query_id": cq_id, "text": "Nivel seleccionado.", "show_alert": False})
                api_post("sendMessage", {"chat_id": chat_id, "text": preview,
                                         "parse_mode": "HTML", "reply_markup": json.dumps(kb)})
                set_state(chat_id, st)
                return

            if choice == "confirm":
                if not st or "level" not in st:
                    api_post("answerCallbackQuery", {
                             "callback_query_id": cq_id, "text": "Nada que confirmar.", "show_alert": False})
                    return
                level = str(st.get("level") or "medio").strip().lower()
                news_text = st.get("text", "").strip()
                url = st.get("url", "")
                if level not in ("alto", "medio", "bajo"):
                    level = "medio"

                emoji = "🔴" if level == "alto" else (
                    "🟠" if level == "medio" else "🟢")
                color = "rojo" if level == "alto" else (
                    "naranja" if level == "medio" else "verde")
                item = {
                    "source": "telegram",
                    "title": (news_text[:200] or "(sin titulo)"),
                    "url": url or "",
                    "summary": "",
                    "published_at": now_mx_iso(),
                    "keyword": "",
                    "level": level,
                    "emoji": emoji,
                    "color": color,
                    "meta": {"method": "bot_add_manual"},
                    "origin": "telegram_add",
                    "ingested_by": "telegram_add",
                }
                append_live_item(item)
                clear_state(chat_id)
                api_post("answerCallbackQuery", {
                         "callback_query_id": cq_id, "text": f"Noticia agregada como {level}.", "show_alert": False})
                _send_temp_message(
                    chat_id,
                    f"{emoji} Noticia agregada: {item['title']}",
                    seconds=6,
                )
                return

            if choice == "addurl":
                if not st:
                    api_post("answerCallbackQuery", {
                             "callback_query_id": cq_id, "text": "No hay noticia pendiente.", "show_alert": False})
                    return
                api_post("answerCallbackQuery", {
                         "callback_query_id": cq_id, "text": "Envía la URL ahora (o escribe omit).", "show_alert": False})
                st = {**st, "action": "awaiting_url"}
                api_post("sendMessage", {
                    "chat_id": chat_id,
                    "text": "Envía la URL que quieres asociar (o escribe omit).",
                    "reply_markup": json.dumps(_cancel_keyboard()),
                })
                set_state(chat_id, st)
                return

            if choice == "edit":
                if not st:
                    api_post("answerCallbackQuery", {
                             "callback_query_id": cq_id, "text": "No hay noticia para editar.", "show_alert": False})
                    return
                api_post("answerCallbackQuery", {
                         "callback_query_id": cq_id, "text": "Envía el texto actualizado.", "show_alert": False})
                st = {**st, "action": "awaiting_edit"}
                api_post("sendMessage", {
                    "chat_id": chat_id,
                    "text": "Envía el texto actualizado de la noticia.",
                    "reply_markup": json.dumps(_cancel_keyboard()),
                })
                set_state(chat_id, st)
                return

            if choice == "cancel":
                clear_state(chat_id)
                api_post("answerCallbackQuery", {
                         "callback_query_id": cq_id, "text": "Operación cancelada.", "show_alert": False})
                return

            if choice == "removeurl":
                if not st:
                    api_post("answerCallbackQuery", {
                             "callback_query_id": cq_id, "text": "No hay noticia pendiente.", "show_alert": False})
                    return
                st.pop("url", None)
                st["action"] = "confirm_details"
                lvl = str(st.get("level") or "bajo").strip().lower()
                if lvl not in ("alto", "medio", "bajo"):
                    lvl = "medio"
                emoji = "🔴" if lvl == "alto" else (
                    "🟠" if lvl == "medio" else "🟢")
                preview = f"Vista previa:\n{emoji} <b>{lvl}</b> - {st.get('text', '')[:200]}\nURL: (sin URL)"
                kb = {"inline_keyboard": [[{"text": "Confirmar", "callback_data": "classify:confirm"}, {"text": "Editar texto", "callback_data": "classify:edit"}], [
                    {"text": "Añadir URL", "callback_data": "classify:addurl"}, {"text": "Cancelar", "callback_data": "classify:cancel"}]]}
                api_post("answerCallbackQuery", {
                         "callback_query_id": cq_id, "text": "URL eliminada.", "show_alert": False})
                api_post("sendMessage", {"chat_id": chat_id, "text": preview,
                                         "parse_mode": "HTML", "reply_markup": json.dumps(kb)})
                set_state(chat_id, st)
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
                    "text": "Resultados expirados o no válidos.",
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
        except Exception:
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
        if not news_text:
            api_post("sendMessage", {
                "chat_id": chat_id,
                "text": "No recibí texto. Envía la noticia o cancela.",
                "reply_markup": json.dumps(_cancel_keyboard()),
            })
            set_state(chat_id, {"action": "awaiting_news"})
            return

        kb = {"inline_keyboard": [[{"text": "🔴 alto", "callback_data": "classify:alto"}, {"text": "🟠 medio", "callback_data": "classify:medio"}, {
            "text": "🟢 bajo", "callback_data": "classify:bajo"}], [{"text": "Cancelar", "callback_data": "classify:cancel"}]]}
        api_post("sendMessage", {
            "chat_id": chat_id,
            "text": "Recibido. Elige nivel para clasificar esta noticia:",
            "reply_markup": json.dumps(kb),
        })
        set_state(chat_id, {"action": "confirm_classify", "text": news_text})
        return

    if state and state.get("action") == "awaiting_search":
        keyword = text.strip()
        if not keyword:
            api_post("sendMessage", {
                "chat_id": chat_id,
                "text": "Escribe una palabra clave o cancela.",
                "reply_markup": json.dumps(_cancel_keyboard()),
            })
            set_state(chat_id, {"action": "awaiting_search"})
            return

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
            _send_temp_message(
                chat_id,
                "No se encontraron resultados.",
                seconds=6,
            )
            return

        send_inline_search_results(chat_id, keyword, results)
        clear_state(chat_id)
        return

    if state and state.get("action") == "awaiting_edit":
        new_text = text.strip()
        state["text"] = new_text
        state["action"] = "confirm_details"
        lvl = str(state.get("level") or "bajo").strip().lower()
        if lvl not in ("alto", "medio", "bajo"):
            lvl = "medio"
        emoji = "🔴" if lvl == "alto" else (
            "🟠" if lvl == "medio" else "🟢")
        preview = f"Vista previa:\n{emoji} <b>{lvl}</b> - {new_text[:200]}"
        kb = {"inline_keyboard": [[{"text": "Confirmar", "callback_data": "classify:confirm"}, {"text": "Editar texto", "callback_data": "classify:edit"}], [
            {"text": "Añadir/Modificar URL (opcional)", "callback_data": "classify:addurl"}, {"text": "Cancelar", "callback_data": "classify:cancel"}]]}
        api_post("sendMessage", {"chat_id": chat_id, "text": preview,
                                 "parse_mode": "HTML", "reply_markup": json.dumps(kb)})
        set_state(chat_id, state)
        return

    if state and state.get("action") == "awaiting_url":
        url_text = text.strip()
        if url_text.lower() == "omit":
            state.pop("url", None)
        else:
            state["url"] = url_text
        state["action"] = "confirm_details"
        lvl = str(state.get("level") or "bajo").strip().lower()
        if lvl not in ("alto", "medio", "bajo"):
            lvl = "medio"
        emoji = "🔴" if lvl == "alto" else (
            "🟠" if lvl == "medio" else "🟢")
        preview = f"Vista previa:\n{emoji} <b>{lvl}</b> - {state.get('text', '')[:200]}\nURL: {state.get('url', '(sin URL)')}"
        kb = {"inline_keyboard": [[{"text": "Confirmar", "callback_data": "classify:confirm"}, {"text": "Editar texto", "callback_data": "classify:edit"}], [
            {"text": "Quitar URL", "callback_data": "classify:removeurl"}, {"text": "Cancelar", "callback_data": "classify:cancel"}]]}
        api_post("sendMessage", {"chat_id": chat_id, "text": preview,
                                 "parse_mode": "HTML", "reply_markup": json.dumps(kb)})
        set_state(chat_id, state)
        return

    if lower.startswith("/start"):
        clear_state(chat_id)
        send_start_menu(chat_id)
        return

    try:
        if (chat.get("type") or "").lower() == "private":
            api_post("sendMessage", {
                     "chat_id": chat_id, "text": "Para agregar noticias usa /start y pulsa '➕ Agregar noticia', o pulsa '🔎 Buscar' para buscar por palabra clave."})
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
        except requests.HTTPError as exc:
            status_code = getattr(exc.response, "status_code", None)
            if status_code == 409:
                log("telegram_bot: getUpdates devolvio 409 (Conflict). Hay otra instancia activa con el mismo token; se detiene este poller.")
                break
            if status_code == 401:
                log("telegram_bot: getUpdates devolvio 401 (Unauthorized). Verifica BOT_TOKEN; se detiene este poller.")
                break
            log_exc(f"Error HTTP en getUpdates ({status_code}): {exc}", exc)
            time.sleep(poll_interval)
        except requests.RequestException as exc:
            log_exc(f"Error de red en getUpdates: {exc}", exc)
            time.sleep(poll_interval)
        except Exception as exc:
            log_exc(f"Error en getUpdates: {exc}", exc)
            time.sleep(poll_interval)


if __name__ == "__main__":
    try:
        init_bot()
    except Exception:
        pass
    poll_updates()
