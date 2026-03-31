from __future__ import annotations

import os
import time
from typing import Iterable, Any
import inspect
import re

from core import storage
from web import realtime
from core.news_finder import deduplicate, build_sources_map
import math
from datetime import timedelta
from core import classifier
import traceback
from datetime import datetime, timezone
from core.logger import log, log_exc

MONITOR_TZ = timezone(timedelta(hours=-6))


def load_config():
    return storage.get_config("monitor_config") or {}
    cfg = storage.get_config("monitor_config") or {}
    try:
        brief = {k: cfg.get(k) for k in (
            "sources", "limit", "interval_minutes", "use_keywords", "max_backfill_hours") if k in cfg}
        log(f"monitor: load_config -> keys={list(cfg.keys())} brief={brief}", "DEBUG")
    except Exception:
        pass
    return cfg


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
            if pl in ("high", "h", "alto", "critico", "crítico", "crítico") or "crit" in pl:
                impacto = "CRITICO"
            elif pl in ("medium", "m", "medio"):
                impacto = "MEDIO"
            elif pl in ("low", "l", "bajo"):
                impacto = "BAJO"
            elif str(provided_level).upper() in ("CRITICO", "MEDIO", "BAJO"):
                impacto = str(provided_level).upper()
        except Exception:
            impacto = None

    if impacto is None:
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
    emoji = ""
    color = ""
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
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def run_iteration(cfg: dict | None = None) -> None:
    cfg = cfg or load_config() or {}
    if not isinstance(cfg, dict):
        try:
            cfg = dict(cfg)
        except Exception:
            cfg = {}

    sources = cfg.get("sources") or ["google", "bing", "hn"]
    try:
        limit = int(cfg.get("limit", 5) or 5)
    except Exception:
        limit = 5
    try:
        interval_minutes = int(cfg.get("interval_minutes", 5) or 5)
    except Exception:
        interval_minutes = 5

    use_keywords_flag = bool(cfg.get("use_keywords", False))

    try:
        cls_conf = classifier.load_config() or {}
        high_kws = cls_conf.get("high", {}).get("keywords", []) or []
        med_kws = cls_conf.get("medium", {}).get("keywords", []) or []
        low_kws = cls_conf.get("low", {}).get("keywords", []) or []

        def _norm_list(lst):
            out = []
            for k in lst:
                try:
                    if k and isinstance(k, str):
                        v = k.strip()
                        if v and v not in out:
                            out.append(v)
                except Exception:
                    continue
            return out

        high_kws = _norm_list(high_kws)
        med_kws = _norm_list(med_kws)
        low_kws = _norm_list(low_kws)
        keywords_list = high_kws + [k for k in med_kws if k not in high_kws] + [
            k for k in low_kws if k not in high_kws and k not in med_kws]
    except Exception:
        keywords_list = []
    try:
        keywords_list_l = [k.lower() for k in (keywords_list or [])]
        high_kws_l = [k.lower() for k in (high_kws or [])]
        med_kws_l = [k.lower() for k in (med_kws or [])]
    except Exception:
        keywords_list_l = []
        high_kws_l = []
        med_kws_l = []
    try:
        keywords_list_l = [k.lower() for k in (keywords_list or [])]
    except Exception:
        keywords_list_l = []
    try:
        keywords_per_cycle = int(cfg.get("keywords_per_cycle", 8) or 8)
    except Exception:
        keywords_per_cycle = 8

    try:
        ctx_kw = cfg.get("context_keywords") if isinstance(cfg, dict) else None
        if isinstance(ctx_kw, list):
            ctx_kw = [str(k).strip()
                      for k in ctx_kw if k and isinstance(k, str)]
        else:
            ctx_kw = []
    except Exception:
        ctx_kw = []
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

    try:
        use_location_filter = bool(cfg.get("use_location_filter"))
        loc_cfg = cfg.get("location") if isinstance(cfg, dict) else None
        loc_tokens = []
        if isinstance(loc_cfg, dict):
            for field in ("state", "municipality", "colony", "country"):
                try:
                    v = loc_cfg.get(field)
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

    try:
        use_location_filter = bool(cfg.get("use_location_filter"))
        loc = cfg.get("location") if isinstance(cfg, dict) else None
        loc_tokens: list[str] = []
        if use_location_filter and isinstance(loc, dict):
            for field in ("state", "municipality", "colony", "country"):
                try:
                    v = loc.get(field)
                except Exception:
                    v = None
                if v and isinstance(v, str):
                    phrase = v.strip()
                    if phrase:
                        loc_tokens.append(phrase.lower())
                        parts = [p.strip() for p in re.split(
                            r"\W+", phrase) if p and len(p) > 2]
                        for p in parts:
                            if p:
                                loc_tokens.append(p.lower())
        loc_tokens = list(dict.fromkeys([t for t in loc_tokens if t]))
    except Exception:
        use_location_filter = False
        loc_tokens = []

    sources_map = build_sources_map()
    try:
        last_success_iso = storage.get_config("monitor:last_success")
        last_success_dt = _parse_iso(
            last_success_iso) if last_success_iso else None
    except Exception:
        last_success_dt = None
    recovery_hours = None
    try:
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

    try:
        log(f"monitor: last_success={last_success_iso} recovery_hours={recovery_hours}", "INFO")
    except Exception:
        pass
        recovery_hours = None
    ordered = []
    if "newsapi" in sources:
        ordered.append("newsapi")
    for s in sources:
        if s == "newsapi":
            continue
        if s in sources_map and s not in ordered:
            ordered.append(s)

    def _fetch_cycle(use_kw: bool):
        all_items_local = []
        per_source_counts_local = {}
        items_by_source_local = {}
        now_local_tz = datetime.now(MONITOR_TZ)
        if recovery_hours:
            cutoff_tz = now_local_tz - timedelta(hours=recovery_hours)
        else:
            cutoff_tz = now_local_tz - timedelta(minutes=interval_minutes)
        try:
            cutoff_local = cutoff_tz.astimezone(timezone.utc)
        except Exception:
            cutoff_local = datetime.now(
                timezone.utc) - timedelta(minutes=interval_minutes)

        tokens_local = {
            "x": os.environ.get("X_BEARER_TOKEN"),
            "facebook": os.environ.get("FACEBOOK_TOKEN"),
            "instagram": os.environ.get("INSTAGRAM_TOKEN"),
            "instagram_user_id": os.environ.get("INSTAGRAM_USER_ID"),
        }

        newsapi_opts_local = (cfg.get("source_options")
                              or {}).get("newsapi", {})
        try:
            loc_country = (cfg.get("location") or {}).get("country")
            if loc_country:
                newsapi_opts_local = dict(newsapi_opts_local)
                newsapi_opts_local["country"] = loc_country
        except Exception:
            pass

        for src in ordered:
            items = []
            try:
                func = sources_map.get(src)
                if not callable(func):
                    continue

                keyword = ""
                candidates = [
                    (keyword, limit),
                    (limit, keyword),
                    (keyword,),
                    (limit,),
                ]

                if src == "newsapi":
                    candidates = [
                        (keyword, limit, newsapi_opts_local),
                        (keyword, limit),
                        (limit, newsapi_opts_local),
                        (limit,),
                        (keyword, newsapi_opts_local),
                        (keyword,),
                    ]
                elif src == "x":
                    if not tokens_local.get("x"):
                        continue
                    bearer = tokens_local.get("x")
                    candidates = [
                        (keyword, limit, bearer),
                        (keyword, limit),
                        (limit, bearer),
                        (limit,),
                    ]
                elif src == "facebook":
                    if not tokens_local.get("facebook"):
                        continue
                    fb = tokens_local.get("facebook")
                    candidates = [
                        (keyword, limit, fb),
                        (keyword, limit),
                        (limit, fb),
                        (limit,),
                    ]
                elif src == "instagram":
                    if not (tokens_local.get("instagram") and tokens_local.get("instagram_user_id")):
                        continue
                    ig = tokens_local.get("instagram")
                    ig_id = tokens_local.get("instagram_user_id")
                    candidates = [
                        (keyword, limit, ig, ig_id),
                        (keyword, limit, ig),
                        (keyword, limit),
                        (limit,),
                    ]

                last_exc = None
                sig = None
                try:
                    sig = inspect.signature(func)
                except Exception:
                    sig = None

                    tried = False
                    kw_candidates = []
                    try:
                        kw_candidates = list(keywords_list or [])
                    except Exception:
                        kw_candidates = []
                    try:
                        if cfg.get("use_context_keywords"):
                            for k in (ctx_kw or []):
                                if k and k not in kw_candidates:
                                    kw_candidates.append(k)
                    except Exception:
                        pass
                    try:
                        loc = cfg.get("location") if isinstance(
                            cfg, dict) else None
                        if isinstance(loc, dict):
                            for field in ("state", "municipality", "colony", "country"):
                                try:
                                    v = loc.get(field)
                                except Exception:
                                    v = None
                                if v and isinstance(v, str):
                                    phrase = v.strip()
                                    if phrase and phrase not in kw_candidates:
                                        kw_candidates.append(phrase)
                                    parts = [p.strip() for p in re.split(
                                        r"\W+", phrase) if p and len(p) > 2]
                                    for p in parts:
                                        if p and p not in kw_candidates:
                                            kw_candidates.append(p)
                    except Exception:
                        pass
                    if use_kw and kw_candidates:
                        query_variants = []
                        try:
                            for kw in kw_candidates:
                                if kw and kw not in query_variants:
                                    query_variants.append(kw)
                                for t in (loc_tokens or []):
                                    v = f"{kw} {t}"
                                    if v not in query_variants:
                                        query_variants.append(v)
                            for t in (loc_tokens or []):
                                if t not in query_variants:
                                    query_variants.append(t)
                        except Exception:
                            query_variants = list(kw_candidates or [])
                        for q in query_variants:
                            for args in candidates:
                                call_args = tuple(
                                    (q if (isinstance(a, str) and a == "") else a) for a in args)
                                try:
                                    log(
                                        f"monitor: attempting query='{q}' src={src} call_args={call_args}")
                                except Exception:
                                    pass
                                res = _try_call(call_args)
                                tried = True
                                if res:
                                    items = res
                                    try:
                                        for itx in items:
                                            if isinstance(itx, dict):
                                                itx["matched_query"] = q
                                            else:
                                                try:
                                                    setattr(
                                                        itx, "matched_query", q)
                                                except Exception:
                                                    pass
                                    except Exception:
                                        pass
                                    try:
                                        log(
                                            f"monitor: query_result query='{q}' src={src} returned={len(items)}")
                                    except Exception:
                                        pass
                                    break

                def _try_call(call_args_tuple):
                    nonlocal last_exc
                    try:
                        call_args = call_args_tuple
                        if sig is not None:
                            pos_params = [p for p in sig.parameters.values()
                                          if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
                            if len(call_args) > len(pos_params):
                                call_args = call_args[: len(pos_params)]
                        return func(*call_args)
                    except TypeError as e:
                        last_exc = e
                        return None
                    except Exception as e:
                        last_exc = e
                        return None

                tried = False
                kw_candidates = []
                try:
                    kw_candidates = list(keywords_list or [])
                except Exception:
                    kw_candidates = []
                try:
                    if cfg.get("use_context_keywords"):
                        for k in (ctx_kw or []):
                            if k and k not in kw_candidates:
                                kw_candidates.append(k)
                except Exception:
                    pass

                try:
                    loc = cfg.get("location") if isinstance(
                        cfg, dict) else None
                    if isinstance(loc, dict):
                        for field in ("state", "municipality", "colony", "country"):
                            try:
                                v = loc.get(field)
                            except Exception:
                                v = None
                            if v and isinstance(v, str):
                                phrase = v.strip()
                                if phrase and phrase not in kw_candidates:
                                    kw_candidates.append(phrase)
                                parts = [p.strip() for p in re.split(
                                    r"\W+", phrase) if p and len(p) > 2]
                                for p in parts:
                                    if p and p not in kw_candidates:
                                        kw_candidates.append(p)
                except Exception:
                    pass

                if use_kw and kw_candidates:
                    query_variants = []
                    try:
                        for kw in kw_candidates:
                            if kw and kw not in query_variants:
                                query_variants.append(kw)
                            for t in (loc_tokens or []):
                                v = f"{kw} {t}"
                                if v not in query_variants:
                                    query_variants.append(v)
                        for t in (loc_tokens or []):
                            if t not in query_variants:
                                query_variants.append(t)
                    except Exception:
                        query_variants = list(kw_candidates or [])

                    for q in query_variants:
                        for args in candidates:
                            call_args = tuple(
                                (q if (isinstance(a, str) and a == "") else a) for a in args)
                            res = _try_call(call_args)
                            tried = True
                            if res:
                                items = res
                                try:
                                    for itx in items:
                                        if isinstance(itx, dict):
                                            itx["matched_query"] = q
                                        else:
                                            try:
                                                setattr(
                                                    itx, "matched_query", q)
                                            except Exception:
                                                pass
                                except Exception:
                                    pass
                                break
                        if items:
                            break

                if not items:
                    for args in candidates:
                        res = _try_call(args)
                        tried = True
                        if res:
                            items = res
                            break
                if not tried and last_exc:
                    raise last_exc
            except Exception as e:
                log_exc(f"monitor: error fetching from source '{src}': {e}", e)
                continue

            count_added = 0
            for it in items:
                if isinstance(it, dict):
                    pub_field = it.get("published_at", "")
                else:
                    pub_field = getattr(it, "published_at", "")
                pub_dt = _parse_iso(pub_field)
                if pub_dt is None:
                    include = True
                else:
                    include = pub_dt >= cutoff_local
                try:
                    if isinstance(it, dict):
                        text_blob = " ".join([str(it.get(k, "") or "") for k in (
                            "title", "summary", "url", "source")])
                    else:
                        text_blob = " ".join([str(getattr(it, k, "") or "") for k in (
                            "title", "summary", "url", "source")])
                    text_blob_l = text_blob.lower()
                except Exception:
                    text_blob_l = ""

                matched_loc = False
                matched_kw = False
                try:
                    if use_location_filter and loc_tokens_l:
                        matched_loc = any(
                            tok in text_blob_l for tok in loc_tokens_l)
                except Exception:
                    matched_loc = False

                try:
                    if keywords_list_l:
                        matched_kw = any(
                            kw in text_blob_l for kw in keywords_list_l)
                    mq = ""
                    if isinstance(it, dict):
                        mq = (it.get("matched_query") or "").lower()
                    else:
                        mq = str(getattr(it, "matched_query", "")
                                 or "").lower()
                    if mq:
                        for kw in (keywords_list_l or []):
                            if kw and kw in mq:
                                matched_kw = True
                                break
                except Exception:
                    matched_kw = matched_kw or False

                if (use_location_filter and loc_tokens_l) or (keywords_list_l):
                    if not (matched_loc or matched_kw):
                        include = False

                if include:
                    all_items_local.append(it)
                    try:
                        items_by_source_local.setdefault(src, []).append(it)
                    except Exception:
                        pass
                    count_added += 1
            try:
                per_source_counts_local[src] = per_source_counts_local.get(
                    src, 0) + count_added
            except Exception:
                per_source_counts_local[src] = count_added

        unique_local = deduplicate(all_items_local)
        try:
            def _priority(it):
                try:
                    txt = " ".join([str(it.get(k, "") if isinstance(it, dict) else getattr(
                        it, k, "") or "") for k in ("title", "summary", "url", "source")]).lower()
                except Exception:
                    txt = ""
                mq = ""
                try:
                    if isinstance(it, dict):
                        mq = (it.get("matched_query") or "").lower()
                    else:
                        mq = str(getattr(it, "matched_query", "")
                                 or "").lower()
                except Exception:
                    mq = ""
                try:
                    for hk in (high_kws_l or []):
                        if hk and (hk in txt or (mq and hk in mq)):
                            return 2
                    for mk in (med_kws_l or []):
                        if mk and (mk in txt or (mq and mk in mq)):
                            return 1
                except Exception:
                    pass
                return 0

            unique_local.sort(key=lambda x: (_priority(x), 0 if (getattr(x, "source", "") or "").lower(
            ) == "newsapi" else 1, getattr(x, "published_at", "")), reverse=True)
        except Exception:
            pass

        if exclude_kw:
            filtered = []
            for it in unique_local:
                try:
                    title = (it.get("title") if isinstance(it, dict)
                             else getattr(it, "title", "")) or ""
                    summary = (it.get("summary") if isinstance(
                        it, dict) else getattr(it, "summary", "")) or ""
                    txt = (title + " " + summary).lower()
                    skip = False
                    for ex in exclude_kw:
                        if ex and ex in txt:
                            skip = True
                            break
                    if not skip:
                        filtered.append(it)
                except Exception:
                    filtered.append(it)
            unique_local = filtered

        return all_items_local, unique_local, per_source_counts_local, items_by_source_local

    log(f"monitor: run_iteration start - sources={sources} limit={limit} interval_minutes={interval_minutes} use_keywords={use_keywords_flag} keywords_count={len(keywords_list)} recovery_hours={recovery_hours}")
    all_items, unique, per_source_counts, items_by_source = _fetch_cycle(
        use_keywords_flag)

    try:
        log(f"monitor: fetched total candidates={len(all_items)} unique_after_dedupe={len(unique)} per_source_counts={per_source_counts}")
    except Exception:
        pass

    if recovery_hours:
        try:
            cutoff_tz = datetime.now(MONITOR_TZ) - \
                timedelta(hours=recovery_hours)
            cutoff_utc = cutoff_tz.astimezone(timezone.utc)
            log(f"monitor: recovery_hours active cutoff_local={cutoff_tz.isoformat()} cutoff_utc={cutoff_utc.isoformat()} recovery_hours={recovery_hours} per_source_counts={per_source_counts}", "INFO")
            for src, items in (items_by_source or {}).items():
                try:
                    log(
                        f"monitor: recovery results source={src} count={len(items)}", "INFO")
                    for it in (items or [])[:20]:
                        title = it.get("title") if isinstance(
                            it, dict) else getattr(it, "title", "")
                        url = it.get("url") if isinstance(
                            it, dict) else getattr(it, "url", "")
                        pub = it.get("published_at") if isinstance(
                            it, dict) else getattr(it, "published_at", "")
                        log(
                            f"monitor: recovery_item source={src} title={title} url={url} published_at={pub}", "INFO")
                except Exception:
                    pass
        except Exception:
            pass

    if not all_items and not use_keywords_flag and keywords_list:
        log("monitor: no results found without keywords, retrying with keywords enabled as fallback")
        try:
            full_kw = keywords_list
        except Exception:
            full_kw = []
        log(f"monitor: run_iteration (retry) start - sources={sources} limit={limit} interval_minutes={interval_minutes} use_keywords=True keywords_count={len(keywords_list)} keywords={full_kw}")
        all_items, unique, per_source_counts, items_by_source = _fetch_cycle(
            True)

        try:
            log(
                f"monitor: fetched total candidates={len(all_items)} unique_after_dedupe={len(unique)} per_source_counts={per_source_counts}")
        except Exception:
            pass

    try:
        storage.set_config("monitor:last_success",
                           datetime.now(timezone.utc).isoformat())
    except Exception as e:
        log_exc("monitor: failed to persist last_success timestamp", e)

    try:
        enriched_items = []
        for it in unique:
            try:
                if isinstance(it, dict):
                    item_in = dict(it)
                else:
                    item_in = {
                        "title": getattr(it, "title", ""),
                        "summary": getattr(it, "summary", ""),
                        "url": getattr(it, "url", ""),
                        "published_at": getattr(it, "published_at", ""),
                        "source": getattr(it, "source", ""),
                    }
                enriched = append_live_item(item_in)
                try:
                    lvl = (enriched.get("level") or "").upper()
                    cfg_alert = {}
                    try:
                        cfg_alert = storage.get_config("monitor_config") or {}
                    except Exception:
                        cfg_alert = {}
                    target_chat = None
                    try:
                        target_chat = cfg_alert.get("telegram_target_chat") if isinstance(
                            cfg_alert, dict) else None
                    except Exception:
                        target_chat = None
                    if not target_chat:
                        target_chat = os.environ.get("TELEGRAM_TARGET_CHAT_ID")
                    alerts_enabled = False
                    try:
                        alerts_enabled = bool(cfg_alert.get("telegram_alerts")) if isinstance(
                            cfg_alert, dict) else False
                    except Exception:
                        alerts_enabled = False
                    if lvl == "CRITICO" and target_chat and alerts_enabled:
                        try:
                            from core import telegram as core_telegram
                            resp = core_telegram.send_item_notification(
                                enriched, str(target_chat))
                            if resp and isinstance(resp, dict) and resp.get("ok"):
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
                                try:
                                    log(
                                        f"monitor: sent immediate alert to {target_chat} for title={enriched.get('title')} message_id={msg_id}")
                                except Exception:
                                    pass
                            else:
                                try:
                                    log(
                                        f"monitor: failed to send immediate alert to {target_chat} resp={resp}", "ERROR")
                                except Exception:
                                    pass
                        except Exception as e:
                            try:
                                log_exc(
                                    "monitor: exception sending immediate alert", e)
                            except Exception:
                                pass
                except Exception:
                    pass
                if enriched:
                    enriched_items.append(enriched)
            except Exception:
                pass
        try:
            if enriched_items:
                publish_items(enriched_items)
        except Exception:
            pass
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
            except Exception as e:
                log_exc(f"monitor: run_iteration failed: {e}", e)
            time.sleep(max(1, interval) * 60)
    except KeyboardInterrupt:
        log("monitor: interrupted by user, exiting")
