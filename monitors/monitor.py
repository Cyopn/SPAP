from __future__ import annotations

import time
import os
from typing import Iterable, Any
import importlib

from core import storage
from web import realtime
from core.news_finder import search_all_sources
import math
from datetime import datetime, timedelta
from core import classifier
from core.logger import log, log_exc
from core.timezone_mx import MX_TZ, now_mx, now_mx_iso

MONITOR_TZ = MX_TZ
_WEEKDAY_TO_INDEX = {
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


def load_config():
    return storage.get_config("monitor_config") or {}


def append_live_item(item: dict, persist: bool = True) -> dict[str, Any]:
    text = item.get("summary", "") or ""
    title = item.get("title", "") or ""
    kw = item.get("keyword", "") or ""

    provided_level = item.get("level")
    cls = None
    impacto = None
    if provided_level:
        try:
            pl = str(provided_level).strip().lower()
            if pl in ("alto", "medio", "bajo"):
                impacto = pl
        except Exception:
            impacto = None

    if impacto is None:
        cls = classifier.classify_text(text, title=title, keyword=kw)

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
            else:
                impacto = "medio"
        elif isinstance(cls, str):
            impacto = str(cls or "medio").strip().lower()
        else:
            impacto = "medio"

    if impacto not in ("alto", "medio", "bajo"):
        impacto = "medio"

    level = impacto
    emoji = ""
    color = ""
    if impacto == "alto":
        emoji = "🔴"
        color = "rojo"
    elif impacto == "medio":
        emoji = "🟠"
        color = "naranja"
    else:
        emoji = "🟢"
        color = "verde"

    extracted_at = now_mx_iso()
    origin = item.get("origin") or "monitor"
    meta = item.get("meta") if item.get("meta") is not None else cls

    enriched = {
        "source": item.get("source", "monitor"),
        "title": item.get("title", "(sin titulo)"),
        "url": item.get("url", ""),
        "summary": item.get("summary", ""),
        "published_at": item.get("published_at", ""),
        "keyword": item.get("keyword", ""),
        "extracted_at": extracted_at,
        "level": level,
        "emoji": emoji,
        "color": color,
        "origin": origin,
        "ingested_by": "monitor",
        "meta": meta,
    }

    if persist:
        row_id = storage.append_item(enriched)
        try:
            enriched["id"] = int(row_id or 0)
        except Exception:
            enriched["id"] = row_id
    return enriched


def publish_items(items: Iterable[dict]):
    for it in items:
        realtime.publish_item(it)


def run_once_for_keywords(*_args, **_kwargs):
    run_iteration()


def _parse_iso(s: str) -> datetime | None:
    if not s:
        return None
    try:
        cleaned = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MONITOR_TZ)
        return dt.astimezone(MONITOR_TZ)
    except Exception:
        return None


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    txt = str(value).strip().lower()
    if txt in ("1", "true", "t", "yes", "y", "on", "si", "sí"):
        return True
    if txt in ("0", "false", "f", "no", "n", "off"):
        return False
    return default


def _last_day_of_month(year: int, month: int) -> int:
    first = datetime(year, month, 1, tzinfo=MONITOR_TZ).date()
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
    effective_day = min(max(1, min(31, int(month_day))),
                        _last_day_of_month(year, month))
    return datetime(year, month, effective_day, tzinfo=MONITOR_TZ).date()


def _compute_auto_window_dates(now_local_date, freq: str, weekday_idx: int, month_day: int) -> tuple[str, str]:
    if freq == "diario":
        to_date = now_local_date
        from_date = to_date - timedelta(days=1)
        return from_date.isoformat(), to_date.isoformat()

    if freq == "semanal":
        delta_days = (weekday_idx - now_local_date.weekday()) % 7
        to_date = now_local_date + timedelta(days=delta_days)
        from_date = to_date - timedelta(days=7)
        return from_date.isoformat(), to_date.isoformat()

    this_anchor = _build_month_anchor(
        now_local_date.year, now_local_date.month, month_day)
    if now_local_date <= this_anchor:
        to_date = this_anchor
    else:
        next_year, next_month = _shift_month(
            now_local_date.year, now_local_date.month, 1)
        to_date = _build_month_anchor(next_year, next_month, month_day)

    prev_year, prev_month = _shift_month(to_date.year, to_date.month, -1)
    from_date = _build_month_anchor(prev_year, prev_month, month_day)
    return from_date.isoformat(), to_date.isoformat()


def _run_scheduled_report_if_due(cfg: dict[str, Any]) -> None:
    if not isinstance(cfg, dict):
        return

    reporting = cfg.get("reporting")
    if not isinstance(reporting, dict):
        return

    auto_cfg = reporting.get("auto")
    if not isinstance(auto_cfg, dict):
        return

    fmt = str(auto_cfg.get("format") or "pdf").strip().lower()
    if fmt not in ("pdf", "xlsx"):
        fmt = "pdf"

    freq = str(auto_cfg.get("frequency") or "diario").strip().lower()
    if freq not in ("diario", "semanal", "mensual"):
        freq = "diario"

    time_txt = str(auto_cfg.get("time") or "09:00").strip()
    try:
        hour_s, minute_s = time_txt.split(":", 1)
        sched_hour = max(0, min(23, int(hour_s)))
        sched_minute = max(0, min(59, int(minute_s)))
    except Exception:
        sched_hour = 9
        sched_minute = 0
        time_txt = "09:00"

    weekday_txt = str(auto_cfg.get("weekday") or "lunes").strip().lower()
    weekday_idx = _WEEKDAY_TO_INDEX.get(weekday_txt, 0)

    try:
        month_day = int(auto_cfg.get("month_day") or 1)
    except Exception:
        month_day = 1
    month_day = max(1, min(31, month_day))

    now_local = datetime.now(MONITOR_TZ)
    if (now_local.hour, now_local.minute) < (sched_hour, sched_minute):
        return
    if freq == "semanal" and now_local.weekday() != weekday_idx:
        return

    # Para meses con menos días, se usa el último día disponible del mes.
    next_month_hint = now_local.replace(day=28) + timedelta(days=4)
    last_day_of_month = (next_month_hint.replace(
        day=1) - timedelta(days=1)).day
    effective_month_day = min(month_day, last_day_of_month)
    if freq == "mensual" and now_local.day != effective_month_day:
        return

    slot_key = f"{freq}:{now_local.date().isoformat()}:{sched_hour:02d}:{sched_minute:02d}:{fmt}"
    try:
        last_slot = str(storage.get_config("monitor:report:last_slot") or "")
    except Exception:
        last_slot = ""
    if last_slot == slot_key:
        return

    levels_raw = auto_cfg.get("levels") if isinstance(
        auto_cfg.get("levels"), list) else []
    levels: list[str] = []
    for lvl_raw in levels_raw:
        lvl = str(lvl_raw or "").strip().lower()
        if lvl in ("alto", "medio", "bajo") and lvl not in levels:
            levels.append(lvl)
    if not levels:
        levels = ["alto", "medio", "bajo"]

    try:
        targets_raw = storage.list_telegram_targets(include_disabled=True)
    except Exception:
        targets_raw = cfg.get("telegram_targets") if isinstance(
            cfg.get("telegram_targets"), list) else []

    targets: list[dict[str, Any]] = []
    seen: set[str] = set()
    for t in targets_raw:
        if not isinstance(t, dict):
            continue
        chat_id = str(t.get("chat_id") or "").strip()
        if not chat_id:
            continue
        key = chat_id.lower()
        if key in seen:
            continue
        if not _as_bool(t.get("enabled"), True):
            continue
        if not _as_bool(t.get("send_report_auto"), False):
            continue

        allow = False
        for lvl in levels:
            if _as_bool(t.get(f"send_report_auto_{lvl}"), False):
                allow = True
                break
        if not allow:
            continue

        seen.add(key)
        targets.append({
            "chat_id": chat_id,
            "label": str(t.get("label") or "").strip(),
        })

    if not targets:
        try:
            storage.set_config("monitor:report:last_slot", slot_key)
        except Exception:
            pass
        log("monitor: reporte automático habilitado pero sin chats activos", "WARNING")
        return

    from_date, to_date = _compute_auto_window_dates(
        now_local.date(),
        freq,
        weekday_idx,
        month_day,
    )

    category = str(auto_cfg.get("category") or "").strip()
    keyword = str(auto_cfg.get("keyword") or "").strip()

    branding_raw = reporting.get("branding") if isinstance(
        reporting.get("branding"), dict) else {}
    branding = {
        "company_name": str(branding_raw.get("company_name") or "SPAP").strip(),
        "letterhead": str(branding_raw.get("letterhead") or "Monitoreo de noticias y alertas").strip(),
        "logo_path": str(branding_raw.get("logo_path") or "").strip(),
    }

    try:
        report_generator = importlib.import_module("tools.report_generator")
        file_path, meta = report_generator.generate_report(
            report_format=fmt,
            from_date=from_date,
            to_date=to_date,
            levels=levels,
            category=category,
            keyword=keyword,
            branding=branding,
            file_prefix="reporte_auto",
            limit=5000,
        )
        file_path = os.path.abspath(str(file_path))
    except RuntimeError as e:
        try:
            storage.set_config("monitor:report:last_slot", slot_key)
        except Exception:
            pass
        log(f"monitor: reporte automático no generado: {e}", "WARNING")
        return
    except Exception as e:
        try:
            storage.set_config("monitor:report:last_slot", slot_key)
        except Exception:
            pass
        log_exc("monitor: fallo generando reporte automático", e)
        return

    try:
        core_telegram = importlib.import_module("core.telegram")
    except Exception as e:
        try:
            storage.set_config("monitor:report:last_slot", slot_key)
        except Exception:
            pass
        log_exc("monitor: no se pudo importar core.telegram para reporte automático", e)
        return

    summary = meta.get("summary") if isinstance(meta, dict) else {}
    caption = (
        f"Reporte automático ({freq})\n"
        f"Noticias: {summary.get('items', 0)} | "
        f"Niveles: {', '.join(summary.get('levels') or [])}"
    )

    ok_targets: list[str] = []
    fail_targets: list[str] = []
    for t in targets:
        chat_id = str(t.get("chat_id") or "").strip()
        if not chat_id:
            continue
        try:
            resp = core_telegram.send_document(
                chat_id, file_path, caption=caption)
            if resp and isinstance(resp, dict) and resp.get("ok"):
                ok_targets.append(chat_id)
            else:
                fail_targets.append(chat_id)
        except Exception:
            fail_targets.append(chat_id)

    try:
        storage.set_config("monitor:report:last_slot", slot_key)
    except Exception:
        pass

    log(
        f"monitor: reporte automático slot={slot_key} archivo={file_path} "
        f"ok={len(ok_targets)} fail={len(fail_targets)}",
        "INFO",
    )


def run_iteration(cfg: dict | None = None) -> None:
    cfg = cfg or load_config() or {}
    if not isinstance(cfg, dict):
        try:
            cfg = dict(cfg)
        except Exception:
            cfg = {}

    sources = cfg.get("sources") or ["google", "bing", "hn"]
    try:
        limit = max(1, int(cfg.get("limit", 5) or 5))
    except Exception:
        limit = 5
    try:
        interval_minutes = max(1, int(cfg.get("interval_minutes", 5) or 5))
    except Exception:
        interval_minutes = 5

    try:
        exclude_kw = cfg.get("exclude_keywords") if isinstance(
            cfg, dict) else None
        if isinstance(exclude_kw, list):
            exclude_kw = [str(k).strip().lower()
                          for k in exclude_kw if k and isinstance(k, str)]
        else:
            exclude_kw = []
    except Exception:
        exclude_kw = []

    last_success_iso = None
    recovery_hours = None
    try:
        last_success_iso = storage.get_config("monitor:last_success")
        last_success_dt = _parse_iso(
            last_success_iso) if last_success_iso else None
        if last_success_dt:
            now_dt = datetime.now(MONITOR_TZ)
            last_success_local = last_success_dt.astimezone(MONITOR_TZ)
            downtime = now_dt - last_success_local
            if downtime.total_seconds() > 3600:
                recovery_hours = int(
                    math.ceil(downtime.total_seconds() / 3600.0))
                max_cap = int(cfg.get("max_backfill_hours", 168) or 168)
                recovery_hours = min(recovery_hours, max_cap)
    except Exception:
        recovery_hours = None

    if recovery_hours:
        cutoff_utc = now_mx() - timedelta(hours=recovery_hours)
    else:
        cutoff_utc = now_mx() - \
            timedelta(minutes=interval_minutes)
    window_end_utc = now_mx()

    try:
        cfg_search = dict(cfg)
    except Exception:
        cfg_search = cfg or {}
    cfg_search["sources"] = [s for s in (
        sources or []) if (s or "").lower() != "reddit"]

    try:
        search_limit = max(
            limit, limit * max(1, len(cfg_search.get("sources") or [])))
    except Exception:
        search_limit = limit

    log(
        f"monitor: run_iteration start - sources={cfg_search.get('sources')} limit={limit} "
        f"interval_minutes={interval_minutes} recovery_hours={recovery_hours} "
        f"window={cutoff_utc.isoformat()}..{window_end_utc.isoformat()}",
        "INFO",
    )

    try:
        candidates = search_all_sources(
            limit=search_limit,
            keyword="",
            cfg=cfg_search,
            persist=False,
            notify=False,
            window_start=cutoff_utc,
            window_end=window_end_utc,
            strict_window=True,
            prefer_specific_location_first=True,
            location_only_single_query=True,
        )
    except Exception as e:
        log_exc(f"monitor: search_all_sources failed: {e}", e)
        candidates = []

    filtered: list[dict[str, Any]] = []
    for it in (candidates or []):
        try:
            pub_dt = _parse_iso(str((it or {}).get("published_at", "") or ""))
            include = bool(pub_dt is not None and cutoff_utc <=
                           pub_dt <= window_end_utc)
            if include:
                filtered.append(dict(it))
        except Exception:
            continue

    if exclude_kw:
        keep: list[dict[str, Any]] = []
        for it in filtered:
            try:
                txt = f"{it.get('title', '')} {it.get('summary', '')}".lower()
                if any(ex in txt for ex in exclude_kw if ex):
                    continue
                keep.append(it)
            except Exception:
                keep.append(it)
        filtered = keep

    seen: set[tuple[str, str]] = set()
    unique: list[dict[str, Any]] = []
    for it in filtered:
        sig = (
            str(it.get("title", "") or "").strip().lower(),
            str(it.get("url", "") or "").strip().lower(),
        )
        if sig in seen:
            continue
        seen.add(sig)
        unique.append(it)

    try:
        unique.sort(key=lambda x: str(
            x.get("published_at", "") or ""), reverse=True)
    except Exception:
        pass

    source_label_by_cfg = {
        "google": "Google News",
        "bing": "Bing News",
        "hn": "Hacker News",
        "newsapi": "NewsAPI",
        "x": "X/Twitter",
        "facebook": "Facebook",
        "instagram": "Instagram",
    }

    per_source_counts: dict[str, int] = {}
    for src_cfg in (cfg_search.get("sources") or []):
        lbl = source_label_by_cfg.get(str(src_cfg).lower(), str(src_cfg))
        per_source_counts[lbl] = 0

    for it in unique:
        src_raw = str(it.get("source", "") or "desconocido")
        src_l = src_raw.lower().strip()
        if src_l in ("google", "google news"):
            src = "Google News"
        elif src_l in ("bing", "bing news"):
            src = "Bing News"
        elif src_l in ("hn", "hacker news"):
            src = "Hacker News"
        elif src_l in ("newsapi", "news api"):
            src = "NewsAPI"
        elif src_l in ("x", "x/twitter", "twitter"):
            src = "X/Twitter"
        elif src_l == "facebook":
            src = "Facebook"
        elif src_l == "instagram":
            src = "Instagram"
        else:
            src = src_raw
        per_source_counts[src] = per_source_counts.get(src, 0) + 1

    try:
        log(
            f"monitor: fetched total candidates={len(candidates)} unique_after_dedupe={len(unique)} "
            f"per_source_counts={per_source_counts}",
            "INFO",
        )
    except Exception:
        pass

    try:
        storage.set_config("monitor:last_success", now_mx_iso())
    except Exception as e:
        log_exc("monitor: failed to persist last_success timestamp", e)

    enriched_items: list[dict[str, Any]] = []
    for item_in in unique:
        try:
            enriched = append_live_item(dict(item_in))
            try:
                cfg_alert = {}
                try:
                    cfg_alert = storage.get_config("monitor_config") or {}
                except Exception:
                    cfg_alert = {}

                item_id = None
                try:
                    item_id = int(enriched.get("id") or 0) or None
                except Exception:
                    item_id = None

                try:
                    from core import telegram as core_telegram

                    send_results = core_telegram.send_item_notification_to_targets(
                        enriched,
                        cfg=cfg_alert,
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
                        log_exc("monitor: exception sending alerts", e)
                    except Exception:
                        pass
            except Exception:
                pass
            if enriched:
                enriched_items.append(enriched)
        except Exception:
            continue

    try:
        if enriched_items:
            publish_items(enriched_items)
    except Exception:
        pass


if __name__ == "__main__":
    log("monitor: starting continuous loop; configuration will be reloaded each iteration")
    try:
        last_cfg: dict | None = None
        while True:
            try:
                cfg = load_config() or {}
                try:
                    interval = int(cfg.get("interval_minutes", 5) or 5)
                except Exception:
                    interval = 5

                try:
                    if last_cfg is None:
                        last_cfg = dict(cfg)
                    else:
                        changes = {}
                        for key in ("sources", "limit", "interval_minutes", "use_keywords"):
                            prev = last_cfg.get(key)
                            curr = cfg.get(key)
                            if key == "sources":
                                try:
                                    prev_norm = list(
                                        prev) if prev is not None else None
                                except Exception:
                                    prev_norm = prev
                                try:
                                    curr_norm = list(
                                        curr) if curr is not None else None
                                except Exception:
                                    curr_norm = curr
                                if prev_norm != curr_norm:
                                    changes[key] = {
                                        "from": prev_norm, "to": curr_norm}
                            else:
                                if prev != curr:
                                    changes[key] = {"from": prev, "to": curr}
                        if changes:
                            log(
                                f"monitor: configuration changed: {changes}", "INFO")
                            last_cfg = dict(cfg)
                except Exception:
                    last_cfg = dict(cfg)

                run_iteration(cfg)
                try:
                    _run_scheduled_report_if_due(cfg)
                except Exception as e:
                    log_exc("monitor: scheduled report runner failed", e)
            except Exception as e:
                log_exc(f"monitor: run_iteration failed: {e}", e)
            time.sleep(max(1, interval) * 60)
    except KeyboardInterrupt:
        log("monitor: interrupted by user, exiting")
