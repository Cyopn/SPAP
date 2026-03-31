from __future__ import annotations

import csv
import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Callable, Iterable
from urllib.parse import quote_plus

import feedparser
import requests
import inspect
from core.logger import log, log_exc

try:
    from external_newsmelt.news_crawler_adapter import NewsCrawlerAdapter
    from external_newsmelt.content_extractor_adapter import ContentExtractorAdapter
    _HAS_NEWSMELT_ADAPTERS = True
except Exception:
    NewsCrawlerAdapter = None
    ContentExtractorAdapter = None
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
    extracted_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat())
    level: str = ""
    emoji: str = ""
    color: str = ""
    origin: str = ""
    meta: dict | None = None


def _apply_level_defaults(item: NewsItem) -> None:
    if not item.emoji and item.level:
        lvl = (item.level or "").lower()
        if lvl in ("critico", "crítico", "alto", "alta", "high"):
            item.emoji = "🔴"
            item.color = "rojo"
        elif lvl in ("medio", "moderado", "medium"):
            item.emoji = "🟠"
            item.color = "naranja"
        elif lvl in ("bajo", "baja", "low"):
            item.emoji = "🟢"
            item.color = "verde"


def _safe_parse_date(value: str | None) -> str:
    if not value:
        return ""
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except (TypeError, ValueError):
        pass

    try:
        cleaned = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
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
    query = quote_plus(keyword)
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

    url = f"https://news.google.com/rss/search?q={query}&hl=es-419&gl={gl}&ceid={ceid}"
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
    query = quote_plus(keyword)
    url = f"https://www.bing.com/news/search?q={query}&format=rss"
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


def search_reddit(keyword: str, limit: int) -> list[NewsItem]:
    query = quote_plus(keyword)
    fetch_limit = max(limit * 3, limit)
    url = f"https://www.reddit.com/search.json?q={query}&sort=new&limit={fetch_limit}&t=week"
    payload = _request_json(url, headers={"User-Agent": DEFAULT_USER_AGENT})

    NEWS_SUBREDDITS = {"news", "worldnews",
                       "noticias", "mexico", "mundo", "politics"}
    NEWS_DOMAIN_KEYWORDS = {"news", "press", "eluniversal", "elpais",
                            "bbc", "cnn", "reuters", "apnews", "la-nacion", "clarin"}

    def _is_reddit_news(data: dict) -> bool:
        is_self = data.get("is_self")
        domain = (data.get("domain") or "").lower()
        subreddit = (data.get("subreddit") or "").lower()
        url_field = (data.get("url") or "").lower()
        title = (data.get("title") or "").lower()

        if subreddit in NEWS_SUBREDDITS:
            return True

        if not is_self and domain and not domain.startswith("self.") and "reddit" not in domain:
            return True

        for k in NEWS_DOMAIN_KEYWORDS:
            if k in domain or k in url_field:
                return True

        if "news" in title or "noticia" in title or "alerta" in title:
            return True

        return False

    items: list[NewsItem] = []
    children = payload.get("data", {}).get("children", [])
    added = 0
    for child in children:
        if added >= limit:
            break
        data = child.get("data", {})

        if not _is_reddit_news(data):
            continue

        created_utc = data.get("created_utc")
        published = ""
        if isinstance(created_utc, (int, float)):
            published = datetime.fromtimestamp(
                created_utc, tz=timezone.utc).isoformat()

        permalink = data.get("permalink", "")
        post_url = f"https://www.reddit.com{permalink}" if permalink else data.get(
            "url", "")

        items.append(
            NewsItem(
                source="Reddit",
                channel="social-network",
                title=str(data.get("title", "(sin titulo)")).strip(),
                url=post_url.strip(),
                summary=str(data.get("selftext", "")).strip(),
                published_at=published,
                keyword=keyword,
            )
        )
        added += 1

    return items


def search_hacker_news(keyword: str, limit: int) -> list[NewsItem]:
    query = quote_plus(keyword)
    url = (
        "https://hn.algolia.com/api/v1/search_by_date"
        f"?query={query}&tags=story&hitsPerPage={limit}"
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
    if not bearer_token:
        raise ValueError("No X bearer token provided")

    query = quote_plus(keyword)
    max_results = max(1, min(limit, 100))
    url = (
        "https://api.twitter.com/2/tweets/search/recent"
        f"?query={query}&tweet.fields=created_at,text&max_results={max_results}"
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
    if not access_token:
        raise ValueError("No Facebook access token provided")

    query = quote_plus(keyword)
    url = f"https://graph.facebook.com/v15.0/search?type=post&q={query}&limit={limit}&access_token={access_token}"
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
    if not access_token or not ig_user_id:
        raise ValueError(
            "Instagram search requires access_token and ig_user_id")

    hashtag = "".join(ch for ch in keyword if ch.isalnum()).lower()
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
    country_code = _country_to_code(options.get("country"))

    if api_key:
        if country_code:
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

        if country_code:
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
        "reddit": search_reddit,
        "hn": search_hacker_news,
    }
    if _HAS_NEWSMELT_ADAPTERS:
        m["newsapi"] = search_newsapi
    return m
