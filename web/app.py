from __future__ import annotations

import os
from flask import Flask, jsonify, request, render_template, redirect, url_for, flash, Response, send_file
import json
import time
import requests
from typing import Any
from datetime import datetime, timedelta
from werkzeug.utils import secure_filename

from core import storage, classifier
from core.logger import log, log_exc
from core.timezone_mx import MX_TZ, now_mx, now_mx_iso
import importlib

_BASE_DIR = os.path.dirname(__file__)
_TEMPLATES_DIR = os.path.join(_BASE_DIR, "templates")
_PROJECT_ROOT = os.path.dirname(_BASE_DIR)
_REPORT_LOGO_UPLOAD_DIR = os.path.join(
    _PROJECT_ROOT, "reports", "_logo_uploads")
_ALLOWED_LOGO_EXTENSIONS = {".png", ".jpg",
                            ".jpeg", ".webp", ".gif", ".bmp", ".svg"}

app = Flask(__name__, template_folder=_TEMPLATES_DIR)
app.secret_key = os.environ.get("FLASK_SECRET", "dev_secret")

_GEO_BASE_URL = "https://countriesnow.space/api/v0.1"
_GEO_CACHE_TTL = 60 * 60 * 12
_GEO_CACHE: dict[str, dict] = {
    "countries_states": {"ts": 0, "data": []},
    "cities": {},
}


def _resolve_logo_path_candidate(raw_ref: str) -> str:
    ref = str(raw_ref or "").strip()
    if not ref:
        return ""
    expanded = os.path.expanduser(ref)
    if os.path.isabs(expanded):
        return os.path.abspath(expanded)
    return os.path.abspath(os.path.join(_PROJECT_ROOT, expanded))


def _save_uploaded_report_logo(files) -> str | None:
    if files is None:
        return None

    try:
        logo_file = files.get("report_logo_file")
    except Exception:
        logo_file = None

    if not logo_file:
        return None

    filename = secure_filename(str(getattr(logo_file, "filename", "") or ""))
    if not filename:
        return None

    ext = os.path.splitext(filename)[1].lower()
    if ext not in _ALLOWED_LOGO_EXTENSIONS:
        try:
            log(
                f"web: logo upload ignored, unsupported extension: {ext}", "WARNING")
        except Exception:
            pass
        return None

    try:
        os.makedirs(_REPORT_LOGO_UPLOAD_DIR, exist_ok=True)
        stamp = now_mx().strftime("%Y%m%d_%H%M%S_%f")
        out_name = f"logo_{stamp}{ext}"
        out_abs = os.path.join(_REPORT_LOGO_UPLOAD_DIR, out_name)
        logo_file.save(out_abs)
        return os.path.relpath(out_abs, _PROJECT_ROOT).replace("\\", "/")
    except Exception as e:
        log_exc("web: failed saving uploaded report logo", e)
        return None


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


def _normalize_telegram_target(raw: dict[str, Any]) -> dict[str, Any] | None:
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
        "send_report_manual": _to_bool(raw.get("send_report_manual"), True),
        "send_report_manual_alto": _to_bool(raw.get("send_report_manual_alto"), True),
        "send_report_manual_medio": _to_bool(raw.get("send_report_manual_medio"), True),
        "send_report_manual_bajo": _to_bool(raw.get("send_report_manual_bajo"), True),
        "send_report_auto": _to_bool(raw.get("send_report_auto"), False),
        "send_report_auto_alto": _to_bool(raw.get("send_report_auto_alto"), True),
        "send_report_auto_medio": _to_bool(raw.get("send_report_auto_medio"), True),
        "send_report_auto_bajo": _to_bool(raw.get("send_report_auto_bajo"), True),
    }


def _load_telegram_targets_for_ui(cfg: dict[str, Any] | None) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    try:
        db_targets = storage.list_telegram_targets(include_disabled=True)
    except Exception:
        db_targets = []

    for raw in (db_targets or []):
        if isinstance(raw, dict):
            t = _normalize_telegram_target(raw)
            if t:
                targets.append(t)

    cfg_local = cfg if isinstance(cfg, dict) else {}
    if not targets:
        cfg_targets = cfg_local.get("telegram_targets")
        if isinstance(cfg_targets, list):
            for raw in cfg_targets:
                if isinstance(raw, dict):
                    t = _normalize_telegram_target(raw)
                    if t:
                        targets.append(t)

    return targets


def _parse_telegram_targets_form(form) -> list[dict[str, Any]]:
    row_ids = [str(v).strip()
               for v in form.getlist("tg_row_idx") if str(v).strip()]
    row_ids = list(dict.fromkeys(row_ids))

    targets: list[dict[str, Any]] = []
    seen: set[str] = set()

    for rid in row_ids:
        chat_id = str(form.get(f"tg_chat_{rid}", "") or "").strip()
        if not chat_id:
            continue
        key = chat_id.lower()
        if key in seen:
            continue
        seen.add(key)

        targets.append(
            {
                "chat_id": chat_id,
                "label": str(form.get(f"tg_label_{rid}", "") or "").strip(),
                "enabled": _to_bool(form.get(f"tg_enabled_{rid}"), False),
                "send_alerts": _to_bool(form.get(f"tg_alert_enabled_{rid}"), False),
                "send_alto": _to_bool(form.get(f"tg_alto_{rid}"), False),
                "send_medio": _to_bool(form.get(f"tg_medio_{rid}"), False),
                "send_bajo": _to_bool(form.get(f"tg_bajo_{rid}"), False),
                "send_report_manual": _to_bool(form.get(f"tg_rep_manual_enabled_{rid}"), False),
                "send_report_manual_alto": _to_bool(form.get(f"tg_rep_manual_alto_{rid}"), False),
                "send_report_manual_medio": _to_bool(form.get(f"tg_rep_manual_medio_{rid}"), False),
                "send_report_manual_bajo": _to_bool(form.get(f"tg_rep_manual_bajo_{rid}"), False),
                "send_report_auto": _to_bool(form.get(f"tg_rep_auto_enabled_{rid}"), False),
                "send_report_auto_alto": _to_bool(form.get(f"tg_rep_auto_alto_{rid}"), False),
                "send_report_auto_medio": _to_bool(form.get(f"tg_rep_auto_medio_{rid}"), False),
                "send_report_auto_bajo": _to_bool(form.get(f"tg_rep_auto_bajo_{rid}"), False),
            }
        )

    return targets


def _sync_telegram_fields(cfg: dict[str, Any], targets: list[dict[str, Any]]) -> None:
    cfg["telegram_targets"] = targets
    cfg.pop("telegram_target_chat", None)
    cfg.pop("telegram_alerts", None)


def _load_country_states() -> list[dict]:
    cache = _GEO_CACHE.get("countries_states") or {}
    now = int(time.time())
    if cache.get("data") and (now - int(cache.get("ts") or 0) < _GEO_CACHE_TTL):
        return cache.get("data") or []

    url = f"{_GEO_BASE_URL}/countries/states"
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        payload = resp.json() or {}
        rows = payload.get("data") if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            rows = []
        _GEO_CACHE["countries_states"] = {"ts": now, "data": rows}
        return rows
    except Exception as e:
        try:
            log_exc("web: failed to load country/state catalog", e)
        except Exception:
            pass
        return cache.get("data") or []


def _load_cities(country: str, state: str) -> list[str]:
    country = (country or "").strip()
    state = (state or "").strip()
    if not country or not state:
        return []

    key = f"{country.lower()}|{state.lower()}"
    cities_cache = _GEO_CACHE.setdefault("cities", {})
    cached = cities_cache.get(key) or {}
    now = int(time.time())
    if cached.get("data") and (now - int(cached.get("ts") or 0) < _GEO_CACHE_TTL):
        return cached.get("data") or []

    url = f"{_GEO_BASE_URL}/countries/state/cities"
    try:
        resp = requests.post(
            url, json={"country": country, "state": state}, timeout=20)
        resp.raise_for_status()
        payload = resp.json() or {}
        rows = payload.get("data") if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            rows = []
        cities = sorted({str(v).strip()
                        for v in rows if str(v).strip()}, key=lambda x: x.lower())
        cities_cache[key] = {"ts": now, "data": cities}
        return cities
    except Exception as e:
        try:
            log_exc("web: failed to load city catalog", e)
        except Exception:
            pass
        return cached.get("data") or []


def _parse_iso_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    try:
        dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
    except Exception:
        try:
            dt = datetime.strptime(txt, "%Y-%m-%d")
        except Exception:
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MX_TZ)
    else:
        dt = dt.astimezone(MX_TZ)
    return dt


def _parse_filter_date(value: str | None, end_of_day: bool = False) -> datetime | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        dt = datetime.strptime(txt, "%Y-%m-%d").replace(tzinfo=MX_TZ)
        if end_of_day:
            dt = dt + timedelta(days=1) - timedelta(microseconds=1)
        return dt
    except Exception:
        return None


def _normalize_level(value: Any) -> str:
    v = str(value or "").strip().lower()
    if v in ("alto", "medio", "bajo"):
        return v
    return "medio"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


_WEEKDAY_ES = [
    "lunes",
    "martes",
    "miércoles",
    "miercoles",
    "jueves",
    "viernes",
    "sábado",
    "sabado",
    "domingo",
]

_WEEKDAY_ES_TO_INDEX = {
    "lunes": 0,
    "martes": 1,
    "miércoles": 2,
    "miercoles": 2,
    "jueves": 3,
    "viernes": 4,
    "sábado": 5,
    "sabado": 5,
    "domingo": 6,
}

_REPORT_AUTO_TZ = MX_TZ


def _normalize_report_format(value: Any) -> str:
    v = str(value or "").strip().lower()
    if v in ("pdf", "xlsx"):
        return v
    return "pdf"


def _normalize_report_frequency(value: Any) -> str:
    v = str(value or "").strip().lower()
    if v in ("diario", "semanal", "mensual"):
        return v
    return "diario"


def _normalize_report_weekday(value: Any) -> str:
    v = str(value or "").strip().lower()
    if v in ("miercoles", "miércoles"):
        return "miércoles"
    if v in ("sabado", "sábado"):
        return "sábado"
    if v in ("lunes", "martes", "jueves", "viernes", "domingo"):
        return v
    return "lunes"


def _normalize_report_month_day(value: Any) -> int:
    try:
        day = int(value)
    except Exception:
        return 1
    return max(1, min(31, day))


def _last_day_of_month(year: int, month: int) -> int:
    first = datetime(year, month, 1, tzinfo=_REPORT_AUTO_TZ).date()
    next_first = (first.replace(day=28) + timedelta(days=4)).replace(day=1)
    return (next_first - timedelta(days=1)).day


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    y, m = year, month
    if delta > 0:
        for _ in range(delta):
            if m == 12:
                y += 1
                m = 1
            else:
                m += 1
    elif delta < 0:
        for _ in range(-delta):
            if m == 1:
                y -= 1
                m = 12
            else:
                m -= 1
    return y, m


def _build_month_anchor(year: int, month: int, month_day: int):
    effective_day = min(_normalize_report_month_day(
        month_day), _last_day_of_month(year, month))
    return datetime(year, month, effective_day, tzinfo=_REPORT_AUTO_TZ).date()


def _compute_auto_report_window_dates(
    frequency: str,
    weekday: str,
    month_day: int,
    now_local_date=None,
) -> tuple[str, str]:
    current_date = now_local_date
    if current_date is None:
        current_date = datetime.now(_REPORT_AUTO_TZ).date()

    freq = _normalize_report_frequency(frequency)

    if freq == "diario":
        to_date = current_date
        from_date = to_date - timedelta(days=1)
        return from_date.isoformat(), to_date.isoformat()

    if freq == "semanal":
        weekday_norm = _normalize_report_weekday(weekday)
        target_idx = _WEEKDAY_ES_TO_INDEX.get(weekday_norm, 0)
        delta_days = (target_idx - current_date.weekday()) % 7
        to_date = current_date + timedelta(days=delta_days)
        from_date = to_date - timedelta(days=7)
        return from_date.isoformat(), to_date.isoformat()

    selected_day = _normalize_report_month_day(month_day)
    this_anchor = _build_month_anchor(
        current_date.year, current_date.month, selected_day)
    if current_date <= this_anchor:
        to_date = this_anchor
    else:
        next_year, next_month = _shift_month(
            current_date.year, current_date.month, 1)
        to_date = _build_month_anchor(next_year, next_month, selected_day)

    prev_year, prev_month = _shift_month(to_date.year, to_date.month, -1)
    from_date = _build_month_anchor(prev_year, prev_month, selected_day)
    return from_date.isoformat(), to_date.isoformat()


def _apply_auto_report_window(auto_cfg: dict[str, Any], now_local_date=None) -> dict[str, Any]:
    out = dict(auto_cfg or {})
    out["frequency"] = _normalize_report_frequency(out.get("frequency"))
    out["weekday"] = _normalize_report_weekday(out.get("weekday"))
    out["month_day"] = _normalize_report_month_day(out.get("month_day"))
    from_date, to_date = _compute_auto_report_window_dates(
        out.get("frequency"),
        out.get("weekday"),
        out.get("month_day"),
        now_local_date=now_local_date,
    )
    out["from_date"] = from_date
    out["to_date"] = to_date
    return out


def _normalize_report_levels(values: list[str] | tuple[str, ...] | None) -> list[str]:
    out: list[str] = []
    for raw in (values or []):
        lvl = _normalize_level(raw)
        if lvl not in out:
            out.append(lvl)
    if out:
        return out
    return ["alto", "medio", "bajo"]


def _normalize_manual_report_window(value: Any) -> str:
    v = str(value or "").strip().lower()
    if v in ("todo", "un_mes", "una_semana", "un_dia", "personalizado"):
        return v
    return "una_semana"


def _compute_manual_report_window_dates(window: str) -> tuple[str, str]:
    today = now_mx().date()
    if window == "todo":
        return "", today.isoformat()
    if window == "un_mes":
        return (today - timedelta(days=30)).isoformat(), today.isoformat()
    if window == "una_semana":
        return (today - timedelta(days=7)).isoformat(), today.isoformat()
    if window == "un_dia":
        return (today - timedelta(days=1)).isoformat(), today.isoformat()
    return "", ""


def _apply_manual_report_window(manual_cfg: dict[str, Any]) -> dict[str, Any]:
    out = dict(manual_cfg or {})
    window = _normalize_manual_report_window(out.get("window"))
    out["window"] = window
    if window != "personalizado":
        from_date, to_date = _compute_manual_report_window_dates(window)
        out["from_date"] = from_date
        out["to_date"] = to_date
    return out


def _default_reporting_cfg() -> dict[str, Any]:
    return {
        "branding": {
            "company_name": "SPAP",
            "letterhead": "Monitoreo de noticias y alertas",
            "logo_path": "",
        },
        "manual": {
            "window": "una_semana",
            "format": "pdf",
            "from_date": "",
            "to_date": "",
            "levels": ["alto", "medio", "bajo"],
            "category": "",
            "keyword": "",
        },
        "auto": {
            "enabled": False,
            "format": "pdf",
            "frequency": "diario",
            "weekday": "lunes",
            "month_day": 1,
            "time": "09:00",
            "from_date": "",
            "to_date": "",
            "levels": ["alto", "medio", "bajo"],
            "category": "",
            "keyword": "",
            "targets": [],
        },
    }


def _normalize_report_target(raw: dict[str, Any]) -> dict[str, Any] | None:
    chat_id = str(raw.get("chat_id") or "").strip()
    if not chat_id:
        return None
    return {
        "chat_id": chat_id,
        "label": str(raw.get("label") or "").strip(),
        "enabled": _to_bool(raw.get("enabled"), True),
    }


def _normalize_reporting_cfg(raw_cfg: dict[str, Any] | None) -> dict[str, Any]:
    base = _default_reporting_cfg()
    cfg = raw_cfg if isinstance(raw_cfg, dict) else {}

    branding_raw = cfg.get("branding") if isinstance(
        cfg.get("branding"), dict) else {}
    base["branding"]["company_name"] = str(
        branding_raw.get("company_name") or base["branding"]["company_name"]
    ).strip()
    base["branding"]["letterhead"] = str(
        branding_raw.get("letterhead") or base["branding"]["letterhead"]
    ).strip()
    base["branding"]["logo_path"] = str(
        branding_raw.get("logo_path") or base["branding"]["logo_path"]
    ).strip()

    manual_raw = cfg.get("manual") if isinstance(
        cfg.get("manual"), dict) else {}
    base["manual"]["window"] = _normalize_manual_report_window(
        manual_raw.get("window"))
    base["manual"]["format"] = _normalize_report_format(
        manual_raw.get("format"))
    base["manual"]["from_date"] = str(
        manual_raw.get("from_date") or "").strip()
    base["manual"]["to_date"] = str(manual_raw.get("to_date") or "").strip()
    base["manual"]["levels"] = _normalize_report_levels(
        manual_raw.get("levels"))
    base["manual"]["category"] = str(manual_raw.get("category") or "").strip()
    base["manual"]["keyword"] = str(manual_raw.get("keyword") or "").strip()
    base["manual"] = _apply_manual_report_window(base["manual"])

    auto_raw = cfg.get("auto") if isinstance(cfg.get("auto"), dict) else {}
    base["auto"]["enabled"] = _to_bool(auto_raw.get("enabled"), False)
    base["auto"]["format"] = _normalize_report_format(auto_raw.get("format"))
    base["auto"]["frequency"] = _normalize_report_frequency(
        auto_raw.get("frequency"))
    base["auto"]["weekday"] = _normalize_report_weekday(
        auto_raw.get("weekday"))
    base["auto"]["month_day"] = _normalize_report_month_day(
        auto_raw.get("month_day"))
    base["auto"]["time"] = str(auto_raw.get(
        "time") or "09:00").strip() or "09:00"
    base["auto"]["from_date"] = str(auto_raw.get("from_date") or "").strip()
    base["auto"]["to_date"] = str(auto_raw.get("to_date") or "").strip()
    base["auto"]["levels"] = _normalize_report_levels(auto_raw.get("levels"))
    base["auto"]["category"] = str(auto_raw.get("category") or "").strip()
    base["auto"]["keyword"] = str(auto_raw.get("keyword") or "").strip()
    base["auto"] = _apply_auto_report_window(base["auto"])

    targets_out: list[dict[str, Any]] = []
    raw_targets = auto_raw.get("targets") if isinstance(
        auto_raw.get("targets"), list) else []
    for t in raw_targets:
        if isinstance(t, dict):
            nt = _normalize_report_target(t)
            if nt:
                targets_out.append(nt)
    base["auto"]["targets"] = targets_out

    return base


def _filter_report_targets_for_mode(
    targets: list[dict[str, Any]] | None,
    mode: str,
    levels: list[str] | tuple[str, ...] | None,
) -> list[dict[str, Any]]:
    mode_norm = str(mode or "").strip().lower()
    if mode_norm not in ("manual", "auto"):
        return []

    levels_norm = _normalize_report_levels(list(levels or []))
    enabled_key = "send_report_manual" if mode_norm == "manual" else "send_report_auto"
    prefix = "send_report_manual_" if mode_norm == "manual" else "send_report_auto_"

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in (targets or []):
        if not isinstance(raw, dict):
            continue
        t = _normalize_telegram_target(raw)
        if not t:
            continue
        if not _to_bool(t.get(enabled_key), False):
            continue

        allowed = False
        for lvl in levels_norm:
            if _to_bool(t.get(f"{prefix}{lvl}"), False):
                allowed = True
                break
        if not allowed:
            continue

        key = str(t.get("chat_id") or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "chat_id": str(t.get("chat_id") or "").strip(),
                "label": str(t.get("label") or "").strip(),
            }
        )
    return out


def _merge_reporting_cfg_from_form(
    current: dict[str, Any],
    form,
    telegram_targets: list[dict[str, Any]] | None = None,
    files=None,
) -> dict[str, Any]:
    cfg = _normalize_reporting_cfg(current)

    cfg["branding"]["company_name"] = str(form.get(
        "report_company_name", cfg["branding"].get("company_name", "SPAP")) or "").strip()
    cfg["branding"]["letterhead"] = str(form.get(
        "report_letterhead", cfg["branding"].get("letterhead", "")) or "").strip()
    uploaded_logo = _save_uploaded_report_logo(files)
    if uploaded_logo:
        cfg["branding"]["logo_path"] = uploaded_logo
    else:
        cfg["branding"]["logo_path"] = str(form.get(
            "report_logo_path", cfg["branding"].get("logo_path", "")) or "").strip()

    cfg["manual"]["window"] = _normalize_manual_report_window(
        form.get("manual_report_window",
                 cfg["manual"].get("window", "una_semana"))
    )
    cfg["manual"]["format"] = _normalize_report_format(
        form.get("manual_report_format", cfg["manual"].get("format", "pdf")))
    cfg["manual"]["from_date"] = str(
        form.get("manual_from_date", cfg["manual"].get("from_date", "")) or "").strip()
    cfg["manual"]["to_date"] = str(
        form.get("manual_to_date", cfg["manual"].get("to_date", "")) or "").strip()
    cfg["manual"]["levels"] = ["alto", "medio", "bajo"]
    cfg["manual"]["category"] = str(
        form.get("manual_category", cfg["manual"].get("category", "")) or "").strip()
    cfg["manual"]["keyword"] = str(
        form.get("manual_keyword", cfg["manual"].get("keyword", "")) or "").strip()
    cfg["manual"] = _apply_manual_report_window(cfg["manual"])

    cfg["auto"]["format"] = _normalize_report_format(
        form.get("auto_report_format", cfg["auto"].get("format", "pdf")))
    cfg["auto"]["frequency"] = _normalize_report_frequency(
        form.get("auto_report_frequency", cfg["auto"].get("frequency", "diario")))
    cfg["auto"]["weekday"] = _normalize_report_weekday(
        form.get("auto_report_weekday", cfg["auto"].get("weekday", "lunes")))
    cfg["auto"]["month_day"] = _normalize_report_month_day(
        form.get("auto_report_month_day", cfg["auto"].get("month_day", 1))
    )
    cfg["auto"]["time"] = str(form.get("auto_report_time", cfg["auto"].get(
        "time", "09:00")) or "09:00").strip() or "09:00"
    cfg["auto"]["from_date"] = str(
        form.get("auto_from_date", cfg["auto"].get("from_date", "")) or "").strip()
    cfg["auto"]["to_date"] = str(
        form.get("auto_to_date", cfg["auto"].get("to_date", "")) or "").strip()
    cfg["auto"]["levels"] = ["alto", "medio", "bajo"]
    cfg["auto"]["category"] = str(
        form.get("auto_category", cfg["auto"].get("category", "")) or "").strip()
    cfg["auto"]["keyword"] = str(
        form.get("auto_keyword", cfg["auto"].get("keyword", "")) or "").strip()
    tg_targets = telegram_targets if isinstance(
        telegram_targets, list) else _parse_telegram_targets_form(form)
    cfg["auto"]["targets"] = _filter_report_targets_for_mode(
        tg_targets,
        mode="auto",
        levels=cfg["auto"].get("levels") or ["alto", "medio", "bajo"],
    )
    cfg["auto"]["enabled"] = bool(cfg["auto"].get("targets"))
    cfg["auto"] = _apply_auto_report_window(cfg["auto"])

    return cfg


@app.route("/api/items")
def api_items():
    items = storage.read_items(200)
    return jsonify(items)


@app.before_request
def _log_request_info():
    try:
        log(f"web: incoming request: {request.method} {request.path}", "DEBUG")
    except Exception:
        pass


@app.after_request
def _log_after_response(response):
    try:
        log(f"web: response: {request.method} {request.path} -> {response.status}", "DEBUG")
    except Exception:
        pass
    return response


@app.route("/api/config", methods=["GET", "POST"])
def api_config():
    if request.method == "GET":
        cfg = storage.get_config("monitor_config") or {}
        return jsonify(cfg)
    else:
        body = request.get_json(force=True)
        storage.set_config("monitor_config", body)
        return jsonify({"ok": True})


@app.route("/api/geo/countries")
def api_geo_countries():
    rows = _load_country_states()
    countries = sorted(
        {
            str(r.get("name", "")).strip()
            for r in rows
            if isinstance(r, dict) and str(r.get("name", "")).strip()
        },
        key=lambda x: x.lower(),
    )
    items = [{"value": "", "label": "Mundo"}] + [
        {"value": c, "label": c} for c in countries
    ]
    return jsonify({"ok": True, "items": items})


@app.route("/api/geo/states")
def api_geo_states():
    country = (request.args.get("country") or "").strip()
    if not country:
        return jsonify({"ok": True, "items": []})

    rows = _load_country_states()
    states_raw = []
    country_l = country.lower()
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        if name.lower() == country_l:
            states_raw = row.get("states") or []
            break

    states = sorted(
        {
            str(s.get("name", "")).strip()
            for s in states_raw
            if isinstance(s, dict) and str(s.get("name", "")).strip()
        },
        key=lambda x: x.lower(),
    )
    items = [{"value": s, "label": s} for s in states]
    return jsonify({"ok": True, "items": items})


@app.route("/api/geo/municipalities")
def api_geo_municipalities():
    country = (request.args.get("country") or "").strip()
    state = (request.args.get("state") or "").strip()
    if not country or not state:
        return jsonify({"ok": True, "items": []})

    cities = _load_cities(country, state)
    items = [{"value": c, "label": c} for c in cities]
    return jsonify({"ok": True, "items": items})


@app.route("/api/report/logo/check")
def api_report_logo_check():
    ref = str(request.args.get("ref") or "").strip()
    if not ref:
        return jsonify({"ok": True, "exists": False, "detail": "Debes ingresar una URL o ruta."})

    ref_l = ref.lower()
    if ref_l.startswith("http://") or ref_l.startswith("https://"):
        try:
            status = None
            content_type = ""
            try:
                head_resp = requests.head(
                    ref, allow_redirects=True, timeout=10)
                status = int(head_resp.status_code)
                content_type = str(head_resp.headers.get("Content-Type") or "")
            except Exception:
                status = None

            if status is None or status == 405 or status >= 400:
                get_resp = requests.get(
                    ref, stream=True, allow_redirects=True, timeout=10)
                status = int(get_resp.status_code)
                content_type = str(get_resp.headers.get("Content-Type") or "")
                try:
                    get_resp.close()
                except Exception:
                    pass

            exists = 200 <= int(status or 0) < 400
            if exists and content_type:
                ctype_l = content_type.lower()
                exists = ctype_l.startswith(
                    "image/") or ctype_l.startswith("application/octet-stream")

            detail = f"HTTP {status}" + \
                (f" | {content_type}" if content_type else "")
            return jsonify({"ok": True, "exists": bool(exists), "detail": detail, "kind": "url"})
        except Exception as e:
            return jsonify({"ok": True, "exists": False, "detail": f"No accesible: {e}", "kind": "url"})

    abs_path = _resolve_logo_path_candidate(ref)
    if not abs_path:
        return jsonify({"ok": True, "exists": False, "detail": "Ruta vacía."})

    exists = os.path.isfile(abs_path)
    if not exists:
        return jsonify({"ok": True, "exists": False, "detail": f"No existe: {abs_path}", "kind": "path"})

    ext = os.path.splitext(abs_path)[1].lower()
    if ext and ext not in _ALLOWED_LOGO_EXTENSIONS:
        return jsonify({"ok": True, "exists": False, "detail": "El archivo existe, pero no parece una imagen soportada.", "kind": "path"})

    return jsonify({"ok": True, "exists": True, "detail": f"Existe: {abs_path}", "kind": "path"})


@app.route("/api/classifier", methods=["GET", "POST"])
def api_classifier():
    if request.method == "GET":
        return jsonify(classifier.load_config())
    body = request.get_json(force=True)
    classifier.set_config(body)
    return jsonify({"ok": True})


@app.route("/")
def index():
    log("web: index requested; rendering template index.html", "INFO")
    try:
        return render_template("index.html")
    except Exception as exc:
        log_exc("web: render_template failed for index.html", exc)
        try:
            idx = os.path.join(_TEMPLATES_DIR, "index.html")
            with open(idx, "r", encoding="utf-8") as f:
                content = f.read()
            return Response(content, mimetype="text/html")
        except Exception as exc2:
            log_exc("web: error serving index fallback", exc2)
            return "Not Found", 404


@app.route("/dashboard")
def dashboard_page():
    return render_template("dashboard.html")


@app.route("/api/items/<int:item_id>/engagement", methods=["POST"])
def api_item_engagement(item_id: int):
    body = request.get_json(silent=True) or {}
    action = str(body.get("action") or "").strip().lower()
    if action not in ("view", "share"):
        return jsonify({"ok": False, "error": "invalid_action"}), 400

    try:
        storage.increment_item_engagement(item_id, action)
        return jsonify({"ok": True})
    except Exception as e:
        log_exc("web: error incrementing item engagement", e)
        return jsonify({"ok": False, "error": "internal_error"}), 500


@app.route("/api/dashboard")
def api_dashboard():
    try:
        items = storage.read_items(5000)
    except Exception as e:
        log_exc("web: error reading items for dashboard", e)
        return jsonify({"ok": False, "error": "read_failed"}), 500

    source_filter = str(request.args.get("source") or "").strip().lower()
    category_filter = str(request.args.get("category") or "").strip().lower()
    levels_raw = str(request.args.get("levels") or "").strip().lower()
    granularity = str(request.args.get("granularity") or "dia").strip().lower()
    if granularity not in ("dia", "semana"):
        granularity = "dia"

    from_dt = _parse_filter_date(
        request.args.get("from_date"), end_of_day=False)
    to_dt = _parse_filter_date(request.args.get("to_date"), end_of_day=True)

    levels = {x.strip() for x in levels_raw.split(",") if x.strip()}
    levels = {x for x in levels if x in ("alto", "medio", "bajo")}

    filtered: list[dict[str, Any]] = []
    for it in items:
        lvl = _normalize_level(it.get("level"))
        if levels and lvl not in levels:
            continue

        source_raw = str(it.get("source") or "").strip()
        source_key = source_raw.lower()
        if source_filter and source_key != source_filter:
            continue

        category_raw = str(it.get("keyword") or "").strip()
        category_key = category_raw.lower()
        if category_filter and category_key != category_filter:
            continue

        dt = (
            _parse_iso_datetime(it.get("published_at"))
            or _parse_iso_datetime(it.get("extracted_at"))
            or _parse_iso_datetime(it.get("created_at"))
        )
        if from_dt and (dt is None or dt < from_dt):
            continue
        if to_dt and (dt is None or dt > to_dt):
            continue

        row = dict(it)
        row["_level"] = lvl
        row["_dt"] = dt
        filtered.append(row)

    distribution = {"alto": 0, "medio": 0, "bajo": 0}
    for it in filtered:
        distribution[it["_level"]] = distribution.get(it["_level"], 0) + 1

    buckets: dict[str, dict[str, int]] = {}
    for it in filtered:
        dt = it.get("_dt")
        if not isinstance(dt, datetime):
            continue
        if granularity == "semana":
            anchor = (dt - timedelta(days=dt.weekday())).date().isoformat()
        else:
            anchor = dt.date().isoformat()

        if anchor not in buckets:
            buckets[anchor] = {"alto": 0, "medio": 0, "bajo": 0}
        lvl = it.get("_level", "medio")
        buckets[anchor][lvl] = buckets[anchor].get(lvl, 0) + 1

    timeline_labels = sorted(buckets.keys())
    timeline_series = {
        "alto": [buckets[k].get("alto", 0) for k in timeline_labels],
        "medio": [buckets[k].get("medio", 0) for k in timeline_labels],
        "bajo": [buckets[k].get("bajo", 0) for k in timeline_labels],
    }

    ranked = sorted(
        filtered,
        key=lambda x: (
            _safe_int(x.get("shares_count"), 0),
            _safe_int(x.get("views_count"), 0),
            _safe_int(x.get("id"), 0),
        ),
        reverse=True,
    )
    top_items = [
        {
            "id": _safe_int(it.get("id"), 0),
            "title": str(it.get("title") or "(sin título)"),
            "source": str(it.get("source") or ""),
            "level": str(it.get("_level") or "medio"),
            "views_count": _safe_int(it.get("views_count"), 0),
            "shares_count": _safe_int(it.get("shares_count"), 0),
            "url": str(it.get("url") or ""),
        }
        for it in ranked[:10]
    ]

    source_options = sorted(
        {str(it.get("source") or "").strip()
         for it in items if str(it.get("source") or "").strip()},
        key=lambda x: x.lower(),
    )
    category_options = sorted(
        {str(it.get("keyword") or "").strip()
         for it in items if str(it.get("keyword") or "").strip()},
        key=lambda x: x.lower(),
    )

    totals = {
        "items": len(filtered),
        "views": sum(_safe_int(it.get("views_count"), 0) for it in filtered),
        "shares": sum(_safe_int(it.get("shares_count"), 0) for it in filtered),
    }

    return jsonify(
        {
            "ok": True,
            "distribution": distribution,
            "timeline": {
                "granularity": granularity,
                "labels": timeline_labels,
                "series": timeline_series,
            },
            "top": top_items,
            "options": {
                "sources": source_options,
                "categories": category_options,
            },
            "totals": totals,
        }
    )


@app.route("/config/report/download", methods=["POST"])
def config_report_download():
    cfg = storage.get_config("monitor_config") or {}
    reporting_cfg = _normalize_reporting_cfg((cfg or {}).get("reporting"))

    try:
        reporting_cfg = _merge_reporting_cfg_from_form(
            reporting_cfg,
            request.form,
            files=request.files,
        )
    except Exception:
        pass

    manual = _apply_manual_report_window(reporting_cfg.get("manual") or {})
    branding = reporting_cfg.get("branding") or {}

    try:
        report_generator = importlib.import_module("tools.report_generator")
        file_path, _meta = report_generator.generate_report(
            report_format=manual.get("format"),
            from_date=manual.get("from_date"),
            to_date=manual.get("to_date"),
            levels=manual.get("levels") or ["alto", "medio", "bajo"],
            category=manual.get("category"),
            keyword=manual.get("keyword"),
            branding=branding,
            file_prefix="reporte_manual",
            limit=5000,
        )
    except RuntimeError as e:
        flash(str(e), "danger")
        return redirect(url_for("config_page"))
    except Exception as e:
        log_exc("web: failed to generate manual report", e)
        flash("No se pudo generar el reporte. Revisa logs.", "danger")
        return redirect(url_for("config_page"))

    ext = str(manual.get("format") or "pdf").strip().lower()
    if ext == "xlsx":
        mimetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        mimetype = "application/pdf"

    abs_file_path = os.path.abspath(file_path)

    return send_file(
        abs_file_path,
        as_attachment=True,
        download_name=os.path.basename(abs_file_path),
        mimetype=mimetype,
    )


@app.route("/config/report/send_now", methods=["POST"])
def config_report_send_now():
    cfg = storage.get_config("monitor_config") or {}
    reporting_cfg = _normalize_reporting_cfg((cfg or {}).get("reporting"))

    telegram_targets = _parse_telegram_targets_form(request.form)
    if not telegram_targets:
        telegram_targets = _load_telegram_targets_for_ui(cfg)

    try:
        reporting_cfg = _merge_reporting_cfg_from_form(
            reporting_cfg,
            request.form,
            telegram_targets=telegram_targets,
            files=request.files,
        )
    except Exception:
        pass

    manual_cfg = _apply_manual_report_window(reporting_cfg.get("manual") or {})
    branding = reporting_cfg.get("branding") or {}

    targets = _filter_report_targets_for_mode(
        telegram_targets,
        mode="manual",
        levels=manual_cfg.get("levels") or ["alto", "medio", "bajo"],
    )
    if not targets:
        flash("No hay chats configurados en Telegram para reporte de 1 clic.", "warning")
        return redirect(url_for("config_page"))

    try:
        report_generator = importlib.import_module("tools.report_generator")
        file_path, meta = report_generator.generate_report(
            report_format=manual_cfg.get("format"),
            from_date=manual_cfg.get("from_date"),
            to_date=manual_cfg.get("to_date"),
            levels=manual_cfg.get("levels") or ["alto", "medio", "bajo"],
            category=manual_cfg.get("category"),
            keyword=manual_cfg.get("keyword"),
            branding=branding,
            file_prefix="reporte_un_clic_envio",
            limit=5000,
        )
    except RuntimeError as e:
        flash(str(e), "danger")
        return redirect(url_for("config_page"))
    except Exception as e:
        log_exc("web: failed to generate report for send_now", e)
        flash("No se pudo generar el reporte para envío. Revisa logs.", "danger")
        return redirect(url_for("config_page"))

    try:
        core_telegram = importlib.import_module("core.telegram")
    except Exception as e:
        log_exc("web: failed to import core.telegram for report send", e)
        flash("No se pudo cargar Telegram para enviar el reporte.", "danger")
        return redirect(url_for("config_page"))

    summary = meta.get("summary") if isinstance(meta, dict) else {}
    abs_file_path = os.path.abspath(file_path)
    caption = (
        "Reporte 1 clic generado desde configuracion\n"
        f"Noticias: {summary.get('items', 0)} | "
        f"Niveles: {', '.join((summary.get('levels') or []))}"
    )

    ok_targets: list[str] = []
    fail_targets: list[str] = []
    for t in targets:
        chat_id = str(t.get("chat_id") or "").strip()
        if not chat_id:
            continue
        try:
            resp = core_telegram.send_document(
                chat_id, abs_file_path, caption=caption)
            if resp and isinstance(resp, dict) and resp.get("ok"):
                ok_targets.append(chat_id)
            else:
                fail_targets.append(chat_id)
        except Exception:
            fail_targets.append(chat_id)

    if ok_targets:
        flash(
            f"Reporte enviado a {len(ok_targets)} chat(s): {', '.join(ok_targets)}",
            "success",
        )
    if fail_targets:
        flash(
            f"Falló el envío a {len(fail_targets)} chat(s): {', '.join(fail_targets)}",
            "danger",
        )
    if not ok_targets and not fail_targets:
        flash("No se realizaron envíos de reporte.", "warning")

    return redirect(url_for("config_page"))


@app.route("/live")
def live():
    log("web: /live requested", "INFO")
    try:
        items = storage.read_items(200)
        log(f"web: /live returning {len(items)} items", "INFO")
        return jsonify(items)
    except Exception as exc:
        log_exc("web: error in /live", exc)
        return jsonify([])


@app.route("/stream")
def stream():
    last_id = storage.get_latest_id()

    def event_stream():
        nonlocal last_id
        while True:
            try:
                items = storage.read_items(200)
                curr = items[0]["id"] if items else 0
                if curr != last_id:
                    last_id = curr
                    yield f"data: {json.dumps(items, ensure_ascii=False)}\n\n"
            except Exception:
                pass
            time.sleep(2)

    return Response(event_stream(), mimetype="text/event-stream")


@app.route("/config", methods=["GET", "POST"])
def config_page():
    def _normalize_sources_list(values, fallback=None):
        out: list[str] = []
        seen: set[str] = set()
        for raw in (values or []):
            src = str(raw or "").strip().lower()
            if not src or src == "reddit" or src in seen:
                continue
            seen.add(src)
            out.append(src)
        if out:
            return out
        return list(fallback or [])

    if request.method == "GET":
        cfg = storage.get_config("monitor_config") or {}
        cfg.setdefault("sources", [])
        cfg["sources"] = _normalize_sources_list(
            cfg.get("sources"), fallback=[])
        cfg["sources_monitor"] = _normalize_sources_list(
            cfg.get("sources_monitor") or cfg.get("sources"),
            fallback=cfg.get("sources") or [],
        )
        cfg["sources_bot"] = _normalize_sources_list(
            cfg.get("sources_bot") or cfg.get("sources"),
            fallback=cfg.get("sources") or [],
        )
        cfg.setdefault("limit", 5)
        cfg.setdefault("interval_minutes", 10)
        report_cfg = _normalize_reporting_cfg(cfg.get("reporting"))
        classifier_cfg = classifier.load_config() or {}
        sources = ["google", "bing", "hn",
                   "newsapi", "x", "facebook", "instagram"]
        telegram_targets = _load_telegram_targets_for_ui(cfg)
        return render_template(
            "config.html",
            sources=sources,
            params=cfg,
            classifier=classifier_cfg,
            telegram_targets=telegram_targets,
            report_cfg=report_cfg,
        )

    form = request.form
    cfg = storage.get_config("monitor_config") or {}

    selected_monitor_sources = _normalize_sources_list(
        form.getlist("sources_monitor") or form.getlist("sources"),
        fallback=[],
    )
    selected_bot_sources = _normalize_sources_list(
        form.getlist("sources_bot") or form.getlist("sources"),
        fallback=[],
    )
    monitor_sources_present = bool(form.get("sources_monitor_present"))
    bot_sources_present = bool(form.get("sources_bot_present"))

    prev_sources = _normalize_sources_list(
        cfg.get("sources") or [], fallback=[])
    prev_monitor_sources = _normalize_sources_list(
        cfg.get("sources_monitor") or prev_sources,
        fallback=prev_sources,
    )
    prev_bot_sources = _normalize_sources_list(
        cfg.get("sources_bot") or prev_sources,
        fallback=prev_sources,
    )

    if monitor_sources_present:
        cfg["sources_monitor"] = selected_monitor_sources
    else:
        cfg["sources_monitor"] = selected_monitor_sources or prev_monitor_sources

    if bot_sources_present:
        cfg["sources_bot"] = selected_bot_sources
    else:
        cfg["sources_bot"] = selected_bot_sources or prev_bot_sources

    cfg["sources"] = list(cfg.get("sources_monitor") or [])

    try:
        cfg["limit"] = int(form.get("limit", cfg.get("limit", 5)))
    except Exception:
        cfg["limit"] = cfg.get("limit", 5)
    try:
        cfg["interval_minutes"] = int(
            form.get("interval_minutes", cfg.get("interval_minutes", 10)))
    except Exception:
        cfg["interval_minutes"] = cfg.get("interval_minutes", 10)
    cfg["location"] = {
        "state": form.get("location_state", ""),
        "country": form.get("location_country", ""),
        "municipality": form.get("location_municipality", ""),
        "colony": form.get("location_colony", ""),
    }

    telegram_targets = _parse_telegram_targets_form(form)
    try:
        storage.replace_telegram_targets(telegram_targets)
    except Exception as e:
        log_exc("web: failed to replace telegram targets", e)
    _sync_telegram_fields(cfg, telegram_targets)

    source_options = cfg.get("source_options")
    if not isinstance(source_options, dict):
        source_options = {}

    newsapi_opts = source_options.get("newsapi")
    if not isinstance(newsapi_opts, dict):
        newsapi_opts = {}

    domains = (form.get("newsapi_domains", "") or "").strip()
    newsapi_opts["domains"] = [d.strip() for d in domains.split(
        ",") if d.strip()] if domains else newsapi_opts.get("domains", [])
    newsapi_opts["qInTitle"] = bool(form.get("newsapi_qInTitle"))
    newsapi_opts["language"] = form.get("newsapi_language", "")
    newsapi_opts["from_date"] = form.get("newsapi_from_date", "")
    newsapi_opts["to_date"] = form.get("newsapi_to_date", "")
    v = form.get("newsapi_page_size")
    if not v:
        newsapi_opts["page_size"] = newsapi_opts.get("page_size", None)
    else:
        try:
            newsapi_opts["page_size"] = int(v)
        except Exception:
            newsapi_opts["page_size"] = newsapi_opts.get("page_size", None)
    newsapi_opts["sort_by"] = form.get("newsapi_sort_by", "")

    x_opts = source_options.get("x")
    if not isinstance(x_opts, dict):
        x_opts = {}
    x_opts["lang"] = str(form.get("x_language", "") or "").strip().lower()
    x_opts["query_suffix"] = str(form.get("x_query_suffix", "") or "").strip()
    x_opts["exclude_retweets"] = bool(form.get("x_exclude_retweets"))
    x_opts["exclude_replies"] = bool(form.get("x_exclude_replies"))

    x_sort = str(form.get("x_sort_order", "") or "").strip().lower()
    if x_sort in ("recency", "relevancy"):
        x_opts["sort_order"] = x_sort
    else:
        x_opts["sort_order"] = ""

    x_max_results_raw = str(form.get("x_max_results", "") or "").strip()
    if not x_max_results_raw:
        x_opts["max_results"] = x_opts.get("max_results", None)
    else:
        try:
            x_opts["max_results"] = max(10, min(100, int(x_max_results_raw)))
        except Exception:
            x_opts["max_results"] = x_opts.get("max_results", None)

    source_options["newsapi"] = newsapi_opts
    source_options["x"] = x_opts
    cfg["source_options"] = source_options

    classifier_cfg = classifier.load_config() or {}

    def parse_kw_area(v: str):
        if not v:
            return []
        raw = v.replace('\r', '\n')
        chunks = []
        for line in raw.split('\n'):
            for piece in line.split(','):
                val = piece.strip()
                if val:
                    chunks.append(val)
        out = []
        seen = set()
        for val in chunks:
            k = val.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(val)
        return out

    classifier_cfg["alto"] = {"keywords": parse_kw_area(
        form.get("classifier_alto_keywords", ""))}
    classifier_cfg["medio"] = {"keywords": parse_kw_area(
        form.get("classifier_medio_keywords", ""))}
    classifier_cfg["bajo"] = {"keywords": parse_kw_area(
        form.get("classifier_bajo_keywords", ""))}

    cfg["exclude_keywords"] = parse_kw_area(form.get("exclude_keywords", ""))
    cfg.pop("use_context_keywords", None)
    cfg.pop("context_keywords", None)
    cfg["use_location_filter"] = True
    try:
        cfg.pop("location_radius", None)
    except Exception:
        pass

    try:
        current_reporting = _normalize_reporting_cfg(cfg.get("reporting"))
        cfg["reporting"] = _merge_reporting_cfg_from_form(
            current_reporting,
            form,
            telegram_targets=telegram_targets,
            files=request.files,
        )
    except Exception as e:
        log_exc("web: failed to parse reporting config", e)

    storage.set_config("monitor_config", cfg)
    try:
        classifier.set_config(classifier_cfg)
    except Exception:
        pass

    flash("Configuración guardada", "success")
    return redirect(url_for("config_page"))


@app.route("/api/alerts")
def api_alerts():
    try:
        items = storage.read_items(1000)
        alerts = [it for it in items if it.get("tg_message_id")]
        return jsonify(alerts)
    except Exception as e:
        log_exc("web: error in /api/alerts", e)
        return jsonify([])


@app.route("/alerts")
def alerts_page():
    try:
        cfg = storage.get_config("monitor_config") or {}
        telegram_targets = _load_telegram_targets_for_ui(cfg)
        target = ", ".join(
            [str(t.get("chat_id") or "").strip()
             for t in telegram_targets if str(t.get("chat_id") or "").strip()]
        )
        if not target:
            target = None
        items = storage.read_items(1000)
        alerts = [it for it in items if it.get("tg_message_id")]

        return render_template("alerts.html", alerts=alerts, target=target, telegram_targets=telegram_targets)
    except Exception as e:
        log_exc("web: error in /alerts", e)
        flash("No se pudieron cargar las alertas.", "danger")
        return redirect(url_for("config_page"))


@app.route("/config/send_test_alert", methods=["POST"])
def config_send_test_alert():
    try:
        cfg = storage.get_config("monitor_config") or {}
        if not isinstance(cfg, dict):
            cfg = {}

        level = str(request.form.get("test_level") or "alto").strip().lower()
        if level not in ("alto", "medio", "bajo"):
            level = "alto"

        level_meta = {
            "alto": {"emoji": "🔴", "title": "Prueba: alerta de impacto alto", "url": "https://example.com/test-alto-impact"},
            "medio": {"emoji": "🟠", "title": "Prueba: alerta de impacto medio", "url": "https://example.com/test-medio-impact"},
            "bajo": {"emoji": "🟢", "title": "Prueba: alerta de impacto bajo", "url": "https://example.com/test-bajo-impact"},
        }
        lvl_info = level_meta.get(level, level_meta["alto"])

        try:
            has_form_targets = bool(request.form.getlist("tg_row_idx"))
            if has_form_targets:
                preview_targets = _parse_telegram_targets_form(request.form)
                cfg = dict(cfg)
                _sync_telegram_fields(cfg, preview_targets)
        except Exception:
            pass

        try:
            core_telegram = importlib.import_module("core.telegram")
        except Exception as e:
            log_exc("web: failed to import core.telegram", e)
            flash("No se pudo cargar el módulo de Telegram. Revisa los logs.", "danger")
            return redirect(url_for("config_page"))

        item = {
            "title": lvl_info["title"],
            "url": lvl_info["url"],
            "summary": f"Mensaje de prueba enviado desde la interfaz de configuración para nivel {level}.",
            "published_at": now_mx_iso(),
            "level": level,
            "emoji": lvl_info["emoji"],
            "source": "web_config_test",
        }

        try:
            targets = core_telegram.get_target_chats_for_item(item, cfg=cfg)
        except Exception:
            targets = []

        if not targets:
            flash(
                f"No hay chats objetivo activos configurados para alertas de impacto {level}.", "warning")
            return redirect(url_for("config_page"))

        try:
            send_results = core_telegram.send_item_notification_to_targets(
                item, cfg=cfg)
        except Exception as e:
            log_exc("web: exception sending test alerts", e)
            send_results = []

        ok_chats = [r.get("chat_id") for r in send_results if r.get("ok")]
        fail_chats = [r.get("chat_id")
                      for r in send_results if not r.get("ok")]

        if ok_chats:
            flash(
                f"Mensaje de prueba ({level}) enviado a {len(ok_chats)} chat(s): {', '.join(str(c) for c in ok_chats)}",
                "success",
            )
        if fail_chats:
            flash(
                f"Falló el envío ({level}) a {len(fail_chats)} chat(s): {', '.join(str(c) for c in fail_chats)}",
                "danger",
            )
        if not ok_chats and not fail_chats:
            flash("No se realizaron envíos de prueba.", "warning")
        return redirect(url_for("config_page"))
    except Exception as e:
        log_exc("web: unexpected error in send_test_alert", e)
        flash("Error inesperado. Revisa logs.", "danger")
        return redirect(url_for("config_page"))


def run_web(**kwargs):
    app.run(**kwargs)


__all__ = ["app", "run_web"]


if __name__ == "__main__":
    host = os.environ.get("WEB_HOST", "127.0.0.1")
    port = int(os.environ.get("WEB_PORT", "5000"))
    log(f"flask: Iniciando web en http://{host}:{port}/", "INFO")
    try:
        log("flask: URL map:", "DEBUG")
        for r in sorted(str(r) for r in app.url_map.iter_rules()):
            log(f"flask:   {r}", "DEBUG")
    except Exception:
        pass
    run_web(host=host, port=port, debug=False, use_reloader=False)
