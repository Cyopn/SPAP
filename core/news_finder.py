from __future__ import annotations

import csv
import json
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Callable, Iterable
from urllib.parse import quote_plus

import feedparser
import requests
import inspect
from core.logger import log, log_exc
from core.timezone_mx import MX_TZ, now_mx_iso

try:
    from external_newsmelt.news_crawler_adapter import NewsCrawlerAdapter
    _HAS_NEWSMELT_ADAPTERS = True
except Exception:
    NewsCrawlerAdapter = None
    _HAS_NEWSMELT_ADAPTERS = False

_CRAWLER_CACHE: dict[str, object] = {}


DEFAULT_TIMEOUT = 15
DEFAULT_USER_AGENT = "news-finder/1.0 (+https://local-script)"


@dataclass
class NewsItem:
    source: str
    channel: str
    title: str
    url: str
    summary: str
    published_at: str
    keyword: str
    extracted_at: str = field(default_factory=now_mx_iso)
    level: str = ""
    emoji: str = ""
    color: str = ""
    origin: str = ""
    meta: dict | None = None


def _apply_level_defaults(item: NewsItem) -> None:
    if not item.emoji and item.level:
        lvl = (item.level or "").lower()
        if lvl == "alto":
            item.emoji = "🔴"
            item.color = "rojo"
        elif lvl == "medio":
            item.emoji = "🟠"
            item.color = "naranja"
        elif lvl == "bajo":
            item.emoji = "🟢"
            item.color = "verde"


def _safe_parse_date(value: str | None) -> str:
    if not value:
        return ""
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MX_TZ)
        return dt.astimezone(MX_TZ).isoformat()
    except (TypeError, ValueError):
        pass

    try:
        cleaned = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MX_TZ)
        return dt.astimezone(MX_TZ).isoformat()
    except ValueError:
        return ""


def _request_json(url: str, *, headers: dict[str, str] | None = None) -> dict:
    response = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    return response.json()


def _request_text(url: str, *, headers: dict[str, str] | None = None) -> str:
    response = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    return response.text


def _country_to_code(name: str | None) -> str | None:
    if not name:
        return None
    val = name.strip().lower()
    mapping = {
        "méxico": "mx",
        "mexico": "mx",
        "mx": "mx",
        "méjico": "mx",
        "estados unidos": "us",
        "united states": "us",
        "eeuu": "us",
        "usa": "us",
        "españa": "es",
        "spain": "es",
        "ar": "ar",
        "argentina": "ar",
        "chile": "cl",
        "colombia": "co",
        "peru": "pe",
        "brasil": "br",
        "brazil": "br",
        "francia": "fr",
        "france": "fr",
    }
    if val in mapping:
        return mapping[val]
    if len(val) == 2 and val.isalpha():
        return val.lower()
    return None


def search_google_news(keyword: str, limit: int) -> list[NewsItem]:
    raw_query = keyword or ""
    encoded = quote_plus(raw_query)
    if not raw_query.strip():
        log("news_finder: search_google_news skipped because keyword is empty", "INFO")
        return []

    try:
        url_preview = f"https://news.google.com/rss/search?q={encoded}"
        log(
            f"news_finder: search_google_news raw='{raw_query}' encoded='{encoded}' url='{url_preview}'", "INFO")
    except Exception:
        pass
    try:
        from core import storage as _storage
        cfg = _storage.get_config("monitor_config") or {}
        loc_country = (cfg.get("location") or {}).get("country")
    except Exception:
        loc_country = None

    country_code = _country_to_code(loc_country)
    if country_code:
        gl = country_code.upper()
        ceid = f"{country_code.upper()}:es"
    else:
        gl = "ES"
        ceid = "ES:es"

    url = f"https://news.google.com/rss/search?q={encoded}&hl=es-419&gl={gl}&ceid={ceid}"
    raw_xml = _request_text(url)
    parsed = feedparser.parse(raw_xml)

    items: list[NewsItem] = []
    for entry in parsed.entries[:limit]:
        items.append(
            NewsItem(
                source="Google News",
                channel="search-engine",
                title=getattr(entry, "title", "(sin titulo)").strip(),
                url=getattr(entry, "link", "").strip(),
                summary=getattr(entry, "summary", "").strip(),
                published_at=_safe_parse_date(getattr(entry, "published", "")),
                keyword=keyword,
            )
        )
    return items


def search_bing_news(keyword: str, limit: int) -> list[NewsItem]:
    raw_query = keyword or ""
    if not str(raw_query).strip():
        log("news_finder: search_bing_news skipped because keyword is empty", "INFO")
        return []
    encoded = quote_plus(raw_query)
    try:
        url = f"https://www.bing.com/news/search?q={encoded}&format=rss"
        log(
            f"news_finder: search_bing_news raw='{raw_query}' encoded='{encoded}' url='{url}'", "INFO")
    except Exception:
        url = f"https://www.bing.com/news/search?q={encoded}&format=rss"
    raw_xml = _request_text(url)
    parsed = feedparser.parse(raw_xml)

    items: list[NewsItem] = []
    for entry in parsed.entries[:limit]:
        items.append(
            NewsItem(
                source="Bing News",
                channel="search-engine",
                title=getattr(entry, "title", "(sin titulo)").strip(),
                url=getattr(entry, "link", "").strip(),
                summary=getattr(entry, "summary", "").strip(),
                published_at=_safe_parse_date(getattr(entry, "published", "")),
                keyword=keyword,
            )
        )
    return items


def search_hacker_news(keyword: str, limit: int) -> list[NewsItem]:
    raw_query = keyword or ""
    if not str(raw_query).strip():
        log("news_finder: search_hacker_news skipped because keyword is empty", "INFO")
        return []
    encoded = quote_plus(raw_query)
    try:
        url_preview = (
            "https://hn.algolia.com/api/v1/search_by_date"
            f"?query={encoded}&tags=story&hitsPerPage={limit}"
        )
        log(
            f"news_finder: search_hacker_news raw='{raw_query}' encoded='{encoded}' url='{url_preview}'", "INFO")
    except Exception:
        pass
    url = (
        "https://hn.algolia.com/api/v1/search_by_date"
        f"?query={encoded}&tags=story&hitsPerPage={limit}"
    )
    payload = _request_json(url)

    items: list[NewsItem] = []
    hits = payload.get("hits", [])
    for hit in hits[:limit]:
        title = hit.get("title") or hit.get("story_title") or "(sin titulo)"
        story_url = hit.get("url") or hit.get("story_url") or ""
        items.append(
            NewsItem(
                source="Hacker News",
                channel="social-network",
                title=str(title).strip(),
                url=str(story_url).strip(),
                summary="",
                published_at=_safe_parse_date(str(hit.get("created_at", ""))),
                keyword=keyword,
            )
        )
    return items


def search_x(keyword: str, limit: int, bearer_token: str | None = None) -> list[NewsItem]:
    raw_query = keyword or ""
    if not str(raw_query).strip():
        log("news_finder: search_x skipped because keyword is empty", "INFO")
        return []
    if not bearer_token:
        raise ValueError("No X bearer token provided")
    encoded = quote_plus(raw_query)
    try:
        url_preview = (
            "https://api.twitter.com/2/tweets/search/recent"
            f"?query={encoded}&tweet.fields=created_at,text&max_results={max(1, min(limit, 100))}"
        )
        log(f"news_finder: search_x raw='{raw_query}' encoded='{encoded}' url='{url_preview}'", "INFO")
    except Exception:
        pass
    max_results = max(1, min(limit, 100))
    url = (
        "https://api.twitter.com/2/tweets/search/recent"
        f"?query={encoded}&tweet.fields=created_at,text&max_results={max_results}"
    )
    headers = {"Authorization": f"Bearer {bearer_token}",
               "User-Agent": DEFAULT_USER_AGENT}
    payload = _request_json(url, headers=headers)

    items: list[NewsItem] = []
    for hit in payload.get("data", [])[:limit]:
        tid = hit.get("id")
        text = hit.get("text", "")
        created = hit.get("created_at", "")
        items.append(
            NewsItem(
                source="X/Twitter",
                channel="social-network",
                title=(text[:200] or "(sin titulo)").strip(),
                url=(
                    f"https://twitter.com/i/web/status/{tid}" if tid else "").strip(),
                summary=text.strip(),
                published_at=_safe_parse_date(created),
                keyword=keyword,
            )
        )
    return items


def search_facebook(keyword: str, limit: int, access_token: str | None = None) -> list[NewsItem]:
    raw_query = keyword or ""
    if not str(raw_query).strip():
        log("news_finder: search_facebook skipped because keyword is empty", "INFO")
        return []
    if not access_token:
        raise ValueError("No Facebook access token provided")
    encoded = quote_plus(raw_query)
    try:
        url = f"https://graph.facebook.com/v15.0/search?type=post&q={encoded}&limit={limit}&access_token={access_token}"
        log(
            f"news_finder: search_facebook raw='{raw_query}' encoded='{encoded}' url='{url}'", "INFO")
    except Exception:
        url = f"https://graph.facebook.com/v15.0/search?type=post&q={encoded}&limit={limit}&access_token={access_token}"
    payload = _request_json(url)

    items: list[NewsItem] = []
    for entry in payload.get("data", [])[:limit]:
        message = entry.get("message") or entry.get("story") or ""
        created = entry.get("created_time", "")
        post_id = entry.get("id", "")
        post_url = f"https://www.facebook.com/{post_id}" if post_id else ""
        items.append(
            NewsItem(
                source="Facebook",
                channel="social-network",
                title=(message[:200] or "(sin titulo)").strip(),
                url=post_url,
                summary=message.strip(),
                published_at=_safe_parse_date(created),
                keyword=keyword,
            )
        )
    return items


def search_instagram(keyword: str, limit: int, access_token: str | None = None, ig_user_id: str | None = None) -> list[NewsItem]:
    raw_query = keyword or ""
    if not str(raw_query).strip():
        log("news_finder: search_instagram skipped because keyword is empty", "INFO")
        return []
    if not access_token or not ig_user_id:
        raise ValueError(
            "Instagram search requires access_token and ig_user_id")

    hashtag = "".join(ch for ch in raw_query if ch.isalnum()).lower()
    try:
        tag_search_url = (
            f"https://graph.facebook.com/ig_hashtag_search?user_id={ig_user_id}&q={quote_plus(hashtag)}&access_token={access_token}"
        )
        log(
            f"news_finder: search_instagram keyword='{keyword}' hashtag='{hashtag}' url='{tag_search_url}'", "INFO")
    except Exception:
        tag_search_url = (
            f"https://graph.facebook.com/ig_hashtag_search?user_id={ig_user_id}&q={quote_plus(hashtag)}&access_token={access_token}"
        )
    tag_payload = _request_json(tag_search_url)
    tag_id = None
    data = tag_payload.get("data", [])
    if data:
        tag_id = data[0].get("id")

    if not tag_id:
        return []

    media_url = (
        f"https://graph.facebook.com/{tag_id}/recent_media?user_id={ig_user_id}&fields=id,caption,permalink,timestamp&limit={limit}&access_token={access_token}"
    )
    media_payload = _request_json(media_url)

    items: list[NewsItem] = []
    for m in media_payload.get("data", [])[:limit]:
        caption = m.get("caption", "")
        permalink = m.get("permalink", "")
        ts = m.get("timestamp", "")
        items.append(
            NewsItem(
                source="Instagram",
                channel="social-network",
                title=(caption[:200] or "(sin titulo)").strip(),
                url=permalink,
                summary=caption.strip(),
                published_at=_safe_parse_date(ts),
                keyword=keyword,
            )
        )
    return items


def search_newsapi(keyword: str, limit: int, options: dict | None = None) -> list[NewsItem]:
    options = options or {}
    cfg = {
        "query": keyword,
        "from_date": options.get("from_date") or None,
        "to_date": options.get("to_date") or None,
        "language": options.get("language", "es"),
        "sort_by": options.get("sort_by", "publishedAt"),
        "page_size": options.get("page_size", max(1, min(limit, 100))),
        "max_retries": options.get("max_retries", 2),
        "retry_delay": options.get("retry_delay", 1),
        "qInTitle": options.get("qInTitle", False),
        "domains": options.get("domains", []),
    }

    api_key = os.environ.get("NEWS_API")
    if not keyword or not str(keyword).strip():
        try:
            log(f"news_finder: search_newsapi skipped because keyword is empty", "INFO")
        except Exception:
            pass
        return []
    country_code = _country_to_code(options.get("country"))

    force_everything = bool(options.get("from_date") or options.get("to_date"))

    if api_key:
        if country_code and not force_everything:
            url = "https://newsapi.org/v2/top-headlines"
            params = {
                "q": keyword or "",
                "pageSize": max(1, min(limit, 100)),
                "country": country_code,
            }
            try:
                log(
                    f"news_finder: search_newsapi search_text='{keyword}' params={params} url={url}", "INFO")
            except Exception:
                pass
        else:
            url = "https://newsapi.org/v2/everything"
            params = {
                "q": keyword or "",
                "pageSize": max(1, min(limit, 100)),
                "language": options.get("language", "es"),
                "sortBy": options.get("sort_by", "publishedAt"),
            }
            try:
                log(
                    f"news_finder: search_newsapi search_text='{keyword}' params={params} url={url}", "INFO")
            except Exception:
                pass
            if options.get("from_date"):
                params["from"] = options.get("from_date")
            if options.get("to_date"):
                params["to"] = options.get("to_date")

        resp = requests.get(
            url, params={**params, "apiKey": api_key}, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
        raw_articles = payload.get("articles", [])

        raw = []
        for a in raw_articles:
            raw.append({
                "title": a.get("title"),
                "url": a.get("url"),
                "description": a.get("description"),
                "publishedAt": a.get("publishedAt"),
            })
    elif _HAS_NEWSMELT_ADAPTERS and callable(NewsCrawlerAdapter):
        crawler = None
        last_exc = None
        attempts = [
            ((), {}),
            ((api_key,), {}),
            ((), {"api_key": api_key}),
            ((), {"config": {"api_key": api_key}}),
        ]

        try:
            sig = inspect.signature(NewsCrawlerAdapter)
            log(f"news_finder: NewsCrawlerAdapter signature: {sig}", "DEBUG")
        except Exception:
            sig = None

        for idx, (args, kwargs) in enumerate(attempts):
            try:
                call_kwargs = kwargs
                if sig is not None and kwargs:
                    call_kwargs = {k: v for k,
                                   v in kwargs.items() if k in sig.parameters}
                log(
                    f"news_finder: Attempt {idx+1}: NewsCrawlerAdapter(*{args}, **{call_kwargs})", "DEBUG")
                cache_key = None
                try:
                    ak = call_kwargs.get("api_key") if isinstance(
                        call_kwargs, dict) else None
                    if not ak:
                        if args and isinstance(args[0], str):
                            ak = args[0]
                    cache_key = str(ak) if ak is not None else "__no_key__"
                except Exception:
                    cache_key = "__no_key__"

                if cache_key in _CRAWLER_CACHE:
                    crawler = _CRAWLER_CACHE[cache_key]
                    log(
                        f"news_finder: using cached NewsCrawlerAdapter for key={cache_key}", "DEBUG")
                else:
                    crawler = NewsCrawlerAdapter(*args, **call_kwargs)
                    _CRAWLER_CACHE[cache_key] = crawler
                    log(
                        f"news_finder: NewsCrawlerAdapter instantiated with attempt {idx+1}", "DEBUG")
                break
            except TypeError as e:
                last_exc = e
                log_exc(
                    f"news_finder: Attempt {idx+1} TypeError initializing NewsCrawlerAdapter: {e}", e)
                continue
            except Exception as e:
                last_exc = e
                log_exc(
                    f"news_finder: Attempt {idx+1} Exception initializing NewsCrawlerAdapter: {e}", e)
                continue

        if crawler is None:
            log_exc(
                "news_finder: Error iniciando NewsCrawlerAdapter: ninguna firma de constructor conocida coincidió.", last_exc)
            raise ValueError(
                "Error iniciando NewsCrawlerAdapter: ninguna firma de constructor conocida coincidió. "
                f"Último error: {last_exc}") from last_exc
        raw = crawler.fetch_articles(cfg)
    else:
        if not api_key:
            raise ValueError(
                "NewsAPI adapter no disponible y NEWS_API no está definida. Instala/importe external_newsmelt o define NEWS_API env var.")

        if country_code and not force_everything:
            url = "https://newsapi.org/v2/top-headlines"
            params = {
                "q": keyword or "",
                "pageSize": max(1, min(limit, 100)),
                "country": country_code,
            }
        else:
            url = "https://newsapi.org/v2/everything"
            params = {
                "q": keyword or "",
                "pageSize": max(1, min(limit, 100)),
                "language": options.get("language", "es"),
                "sortBy": options.get("sort_by", "publishedAt"),
            }
            if options.get("from_date"):
                params["from"] = options.get("from_date")
            if options.get("to_date"):
                params["to"] = options.get("to_date")

        resp = requests.get(
            url, params={**params, "apiKey": api_key}, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
        raw_articles = payload.get("articles", [])

        raw = []
        for a in raw_articles:
            raw.append({
                "title": a.get("title"),
                "url": a.get("url"),
                "description": a.get("description"),
                "publishedAt": a.get("publishedAt"),
            })

    items: list[NewsItem] = []
    for a in raw[:limit]:
        published = a.get("publishedAt") or "" if isinstance(
            a, dict) else (a.get("publishedAt") or "")
        items.append(
            NewsItem(
                source="NewsAPI",
                channel="search-engine",
                title=((a.get("title") if isinstance(a, dict)
                       else a.get("title")) or "(sin titulo)").strip(),
                url=((a.get("url") if isinstance(a, dict)
                     else a.get("url")) or "").strip(),
                summary=((a.get("description") if isinstance(a, dict)
                         else a.get("description")) or "").strip(),
                published_at=published,
                keyword=keyword,
            )
        )
    return items


def deduplicate(items: Iterable[NewsItem]) -> list[NewsItem]:
    seen: set[tuple[str, str]] = set()
    unique: list[NewsItem] = []

    for item in items:
        signature = (item.title.lower().strip(), item.url.lower().strip())
        if signature in seen:
            continue
        seen.add(signature)
        unique.append(item)

    unique.sort(key=lambda x: x.published_at, reverse=True)
    return unique


def save_json(path: str, items: list[NewsItem]) -> None:
    for it in items:
        _apply_level_defaults(it)
    with open(path, "w", encoding="utf-8") as f:
        json.dump([asdict(item) for item in items],
                  f, ensure_ascii=False, indent=2)


def save_csv(path: str, items: list[NewsItem]) -> None:
    if not items:
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["source", "channel", "title", "url",
                            "summary", "published_at", "keyword"])
        return

    for it in items:
        _apply_level_defaults(it)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(asdict(items[0]).keys()))
        writer.writeheader()
        writer.writerows(asdict(item) for item in items)


def print_results(items: list[NewsItem], max_rows: int) -> None:
    if not items:
        log("news_finder: No se encontraron resultados.", "INFO")
        return

    log(f"news_finder: Se encontraron {len(items)} resultados unicos. Mostrando {min(max_rows, len(items))}:", "INFO")
    for index, item in enumerate(items[:max_rows], start=1):
        _apply_level_defaults(item)
        log(f"{index}. [{item.source}] {item.title}", "INFO")
        log(f"   Fecha: {item.published_at or 'sin fecha'}", "INFO")
        log(f"   Keyword: {item.keyword}", "INFO")
        log(f"   URL: {item.url}", "INFO")
        if item.summary:
            compact = " ".join(item.summary.split())
            log(f"   Resumen: {compact[:220]}{'...' if len(compact) > 220 else ''}", "INFO")
        log("", "INFO")


def build_sources_map() -> dict[str, Callable[[str, int], list[NewsItem]]]:
    m = {
        "google": search_google_news,
        "bing": search_bing_news,
        "hn": search_hacker_news,
    }
    if _HAS_NEWSMELT_ADAPTERS:
        m["newsapi"] = search_newsapi
    return m


def _compute_impact_level(item: dict) -> str:
    """Heurística simple para asignar nivel de impacto a un ítem.

    Devuelve una de: 'bajo', 'medio', 'alto'
    """
    text = " ".join([str(item.get(k, "") or "")
                    for k in ("title", "summary", "keyword")]).lower()
    if any(w in text for w in ("crític", "critic", "urgente", "inmediato", "evacu", "explos", "ataque", "muert", "herid", "colapso")):
        return "alto"
    if any(w in text for w in ("alto", "grave", "incendio", "derrumbe", "manifestaci", "protesta", "corte", "fuga")):
        return "alto"
    if any(w in text for w in ("medio", "moderado", "investiga", "sospech")):
        return "medio"
    return "bajo"


def search_all_sources(
    limit: int = 10,
    keyword: str | None = None,
    cfg: dict | None = None,
    *,
    persist: bool = True,
    notify: bool = True,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
    strict_window: bool = False,
    include_location_only_when_keyword: bool = True,
    prefer_specific_location_first: bool = False,
    keyword_with_location_only: bool = False,
    location_only_single_query: bool = False,
) -> list[dict]:
    """Método unificado de búsqueda para bot y monitor.

    Prioriza ubicación en la construcción de queries (país -> estado -> municipio -> colonia)
    y permite activar/desactivar persistencia y notificaciones.
    """
    from core import storage as _storage

    try:
        cfg_global = cfg or (_storage.get_config("monitor_config") or {})
    except Exception:
        cfg_global = cfg or {}

    try:
        limit = max(1, int(limit or 10))
    except Exception:
        limit = 10

    window_start_utc = None
    window_end_utc = None
    try:
        if window_start is not None:
            ws = window_start
            if ws.tzinfo is None:
                ws = ws.replace(tzinfo=MX_TZ)
            window_start_utc = ws.astimezone(MX_TZ)
    except Exception:
        window_start_utc = None
    try:
        if window_end is not None:
            we = window_end
            if we.tzinfo is None:
                we = we.replace(tzinfo=MX_TZ)
            window_end_utc = we.astimezone(MX_TZ)
    except Exception:
        window_end_utc = None

    sources = cfg_global.get("sources") or list(build_sources_map().keys())
    sources = [s for s in (sources or []) if (s or "").lower() != "reddit"]

    use_location_filter = True

    loc_values: list[str] = []
    loc_tokens: list[str] = []
    try:
        loc_cfg = cfg_global.get("location") or {}
        if isinstance(loc_cfg, dict):
            for field in ("country", "state", "municipality", "colony", "city"):
                raw = str(loc_cfg.get(field, "") or "").strip()
                if not raw:
                    continue
                low = raw.lower()
                if low in ("mundo", "world", "global", "all", "todos"):
                    continue
                loc_values.append(raw)
                loc_tokens.append(raw)
                parts = [p.strip()
                         for p in re.split(r"\W+", raw) if p and len(p) > 2]
                for p in parts:
                    loc_tokens.append(p)

        for p in (cfg_global.get("location_phrases") or []):
            pp = str(p or "").strip()
            if pp:
                loc_tokens.append(pp)
    except Exception:
        loc_values = []
        loc_tokens = []

    loc_values = list(dict.fromkeys([t for t in loc_values if t]))
    loc_tokens = list(dict.fromkeys([t for t in loc_tokens if t]))
    loc_tokens_l = [t.lower() for t in loc_tokens]

    user_keyword = str(keyword or "").strip()

    location_queries: list[str] = []
    for i in range(1, len(loc_values) + 1):
        q = " ".join(loc_values[:i]).strip()
        if q:
            location_queries.append(q)

    ordered_location_queries = list(location_queries)
    if prefer_specific_location_first:
        ordered_location_queries = list(reversed(ordered_location_queries))

    query_variants: list[str] = []
    if location_queries and user_keyword:
        if keyword_with_location_only:
            top_location = ordered_location_queries[0] if ordered_location_queries else ""
            if top_location:
                query_variants.append(f"{user_keyword} {top_location}".strip())
            else:
                query_variants.append(user_keyword)
        else:
            if include_location_only_when_keyword:
                query_variants.extend(ordered_location_queries)
            for lq in ordered_location_queries:
                query_variants.append(f"{user_keyword} {lq}".strip())
            query_variants.append(user_keyword)
    elif location_queries:
        if location_only_single_query:
            top_location = ordered_location_queries[0] if ordered_location_queries else ""
            if top_location:
                query_variants.append(top_location)
        else:
            query_variants.extend(ordered_location_queries)
    elif user_keyword:
        query_variants.append(user_keyword)

    query_variants = list(dict.fromkeys(
        [q.strip() for q in query_variants if q and q.strip()]))
    if not query_variants and loc_values:
        query_variants = [" ".join(loc_values).strip()]

    src_map = build_sources_map()
    collected: list[NewsItem] = []

    tokens = {
        "x": os.environ.get("X_BEARER_TOKEN"),
        "facebook": os.environ.get("FACEBOOK_TOKEN"),
        "instagram": os.environ.get("INSTAGRAM_TOKEN"),
        "instagram_user_id": os.environ.get("INSTAGRAM_USER_ID"),
    }

    source_page_limit = max(1, min(limit, 100))
    newsapi_rate_limited = False
    for s in sources:
        try:
            if s == "newsapi":
                newsapi_opts = (cfg_global.get("source_options")
                                or {}).get("newsapi", {})
                try:
                    loc_country = (cfg_global.get("location")
                                   or {}).get("country")
                    if loc_country and str(loc_country).strip().lower() not in ("mundo", "world", "global", "all", "todos"):
                        newsapi_opts = dict(newsapi_opts)
                        newsapi_opts["country"] = loc_country
                except Exception:
                    pass
                try:
                    if window_start_utc is not None:
                        newsapi_opts = dict(newsapi_opts)
                        newsapi_opts["from_date"] = window_start_utc.isoformat()
                    if window_end_utc is not None:
                        newsapi_opts = dict(newsapi_opts)
                        newsapi_opts["to_date"] = window_end_utc.isoformat()
                except Exception:
                    pass

                for q in (query_variants or ([user_keyword] if user_keyword else []))[:6]:
                    try:
                        found = search_newsapi(
                            q, source_page_limit, newsapi_opts) or []
                        for f in found:
                            try:
                                setattr(f, "matched_query", q)
                            except Exception:
                                pass
                        collected.extend(found)
                    except Exception as e:
                        try:
                            status = getattr(
                                getattr(e, "response", None), "status_code", None)
                            msg = str(e).lower()
                            if status == 429 or "429" in msg or "too many" in msg:
                                newsapi_rate_limited = True
                        except Exception:
                            pass
                        continue
                continue

            if s == "google":
                for q in (query_variants or ([user_keyword] if user_keyword else []))[:6]:
                    try:
                        found = search_google_news(q, source_page_limit) or []
                        for f in found:
                            try:
                                setattr(f, "matched_query", q)
                            except Exception:
                                pass
                        collected.extend(found)
                    except Exception:
                        continue
                continue

            if s == "bing":
                for q in (query_variants or ([user_keyword] if user_keyword else []))[:4]:
                    try:
                        found = search_bing_news(q, source_page_limit) or []
                        for f in found:
                            try:
                                setattr(f, "matched_query", q)
                            except Exception:
                                pass
                        collected.extend(found)
                    except Exception:
                        continue
                continue

            if s == "hn":
                for q in (query_variants or ([user_keyword] if user_keyword else []))[:4]:
                    try:
                        found = search_hacker_news(q, source_page_limit) or []
                        for f in found:
                            try:
                                setattr(f, "matched_query", q)
                            except Exception:
                                pass
                        collected.extend(found)
                    except Exception:
                        continue
                continue

            if s == "x" and tokens.get("x"):
                for q in (query_variants or ([user_keyword] if user_keyword else []))[:3]:
                    try:
                        found = search_x(q, source_page_limit,
                                         tokens.get("x")) or []
                        for f in found:
                            try:
                                setattr(f, "matched_query", q)
                            except Exception:
                                pass
                        collected.extend(found)
                    except Exception:
                        continue
                continue

            if s == "facebook" and tokens.get("facebook"):
                for q in (query_variants or ([user_keyword] if user_keyword else []))[:3]:
                    try:
                        found = search_facebook(
                            q, source_page_limit, tokens.get("facebook")) or []
                        for f in found:
                            try:
                                setattr(f, "matched_query", q)
                            except Exception:
                                pass
                        collected.extend(found)
                    except Exception:
                        continue
                continue

            if s == "instagram" and tokens.get("instagram") and tokens.get("instagram_user_id"):
                for q in (query_variants or ([user_keyword] if user_keyword else []))[:3]:
                    try:
                        found = search_instagram(
                            q,
                            source_page_limit,
                            tokens.get("instagram"),
                            tokens.get("instagram_user_id"),
                        ) or []
                        for f in found:
                            try:
                                setattr(f, "matched_query", q)
                            except Exception:
                                pass
                        collected.extend(found)
                    except Exception:
                        continue
                continue

            fn = src_map.get(s)
            if fn:
                for q in (query_variants or ([user_keyword] if user_keyword else []))[:4]:
                    try:
                        found = fn(q, source_page_limit) or []
                        for f in found:
                            try:
                                setattr(f, "matched_query", q)
                            except Exception:
                                pass
                        collected.extend(found)
                    except Exception:
                        continue
        except Exception:
            continue

    if newsapi_rate_limited:
        try:
            log("news_finder: NewsAPI rate-limited (429), se considera sin resultados en este ciclo", "WARNING")
        except Exception:
            pass

    norm: list[dict] = []
    for it in deduplicate(collected):
        try:
            norm.append({
                "source": getattr(it, "source", ""),
                "channel": getattr(it, "channel", ""),
                "title": getattr(it, "title", ""),
                "url": getattr(it, "url", ""),
                "summary": getattr(it, "summary", ""),
                "published_at": getattr(it, "published_at", ""),
                "keyword": getattr(it, "keyword", ""),
                "matched_query": getattr(it, "matched_query", ""),
            })
        except Exception:
            continue

    if use_location_filter and loc_tokens_l:
        def _loc_match(it: dict) -> bool:
            text = " ".join([str(it.get(k, "") or "") for k in (
                "title", "summary", "keyword", "matched_query")]).lower()
            return any(t in text for t in loc_tokens_l)

        norm = [it for it in norm if _loc_match(it)]

    if window_start_utc is not None or window_end_utc is not None:
        def _parse_pub_dt(value: str) -> datetime | None:
            if not value:
                return None
            try:
                parsed = _safe_parse_date(value)
                if parsed:
                    v = parsed
                else:
                    v = value
                dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=MX_TZ)
                return dt.astimezone(MX_TZ)
            except Exception:
                return None

        filtered_window: list[dict] = []
        for it in norm:
            dt = _parse_pub_dt(str(it.get("published_at", "") or ""))
            if dt is None:
                if strict_window:
                    continue
                filtered_window.append(it)
                continue
            if window_start_utc is not None and dt < window_start_utc:
                continue
            if window_end_utc is not None and dt > window_end_utc:
                continue
            filtered_window.append(it)
        norm = filtered_window

    final: list[dict] = []
    core_telegram = None
    core_classifier = None
    if notify:
        try:
            from core import telegram as core_telegram
        except Exception:
            core_telegram = None
    try:
        from core import classifier as core_classifier
    except Exception:
        core_classifier = None

    for it in norm:
        try:
            lvl = "bajo"
            if core_classifier is not None:
                try:
                    cls = core_classifier.classify_text(
                        str(it.get("summary", "") or ""),
                        title=str(it.get("title", "") or ""),
                        keyword=str(it.get("keyword", "") or ""),
                    )
                    imp = ""
                    if isinstance(cls, dict):
                        imp = str(cls.get("impacto", "") or "").lower()
                    elif isinstance(cls, tuple) and len(cls) >= 1:
                        imp = str(cls[0] or "").lower()
                    elif isinstance(cls, str):
                        imp = str(cls).lower()

                    if imp == "alto":
                        lvl = "alto"
                    elif imp == "medio":
                        lvl = "medio"
                    else:
                        lvl = "bajo"
                except Exception:
                    lvl = _compute_impact_level(it)
            else:
                lvl = _compute_impact_level(it)

            it["level"] = lvl
            if not it.get("emoji"):
                if lvl == "alto":
                    it["emoji"] = "🔴"
                    it["color"] = "rojo"
                elif lvl == "medio":
                    it["emoji"] = "🟠"
                    it["color"] = "naranja"
                else:
                    it["emoji"] = "🟢"
                    it["color"] = "verde"

            item_id = None
            if persist:
                try:
                    item_id = _storage.append_item(it)
                except Exception:
                    item_id = None

            if notify and core_telegram:
                try:
                    send_results = core_telegram.send_item_notification_to_targets(
                        it,
                        cfg=cfg_global if isinstance(
                            cfg_global, dict) else None,
                        item_id=item_id if persist else None,
                    )
                    if persist and item_id:
                        for r in send_results:
                            mid = r.get("message_id")
                            if r.get("ok") and mid:
                                try:
                                    _storage.set_tg_message_id(item_id, mid)
                                except Exception:
                                    pass
                                break
                except Exception:
                    pass

            final.append(it)
        except Exception:
            continue

    return final[:limit]
