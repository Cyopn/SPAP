from __future__ import annotations

from datetime import datetime, timezone
import time
import os
import json
import logging
from pathlib import Path
import requests
from typing import Dict, Any, List

DEFAULT_TIMEOUT = 15


class NewsCrawlerAdapter:
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.environ.get("NEWS_API")
        if not self.api_key:
            raise ValueError("NEWS_API key required for NewsCrawlerAdapter")
        self.endpoint = "https://newsapi.org/v2/everything"
        self.logger = logging.getLogger("news_crawler_adapter")
        self._stats_file = Path(__file__).parent.parent / \
            "newsapi_request_stats.json"
        try:
            if not self._stats_file.exists():
                with open(self._stats_file, "w", encoding="utf-8") as sf:
                    json.dump(
                        {"total_requests": 0, "last_call": None, "daily": {}}, sf)
        except Exception:
            pass

    def fetch_articles(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        params = {
            "q": config.get("query", ""),
            "from": config.get("from_date"),
            "to": config.get("to_date"),
            "language": config.get("language", "en"),
            "sortBy": config.get("sort_by", "publishedAt"),
            "qInTitle": config.get("qInTitle", None),
            "domains": ",".join(config.get("domains", [])) if config.get("domains") else None,
            "pageSize": config.get("page_size", 100),
            "apiKey": self.api_key,
        }

        articles: List[Dict[str, Any]] = []
        max_retries = int(config.get("max_retries", 2))
        retry_delay = float(config.get("retry_delay", 1))

        for attempt in range(max_retries):
            try:
                log_params = {k: v for k, v in params.items() if k != "apiKey"}
                try:
                    self.logger.info("NewsAPI request params: %s", log_params)
                except Exception:
                    pass

                resp = requests.get(
                    self.endpoint, params=params, timeout=DEFAULT_TIMEOUT)
                resp.raise_for_status()
                payload = resp.json()
                try:
                    now = datetime.now(timezone.utc)
                    s = {"total_requests": 0, "last_call": None, "daily": {}}
                    if self._stats_file.exists():
                        with open(self._stats_file, "r", encoding="utf-8") as sf:
                            try:
                                s = json.load(sf)
                            except Exception:
                                s = {"total_requests": 0,
                                     "last_call": None, "daily": {}}
                    s["total_requests"] = s.get("total_requests", 0) + 1
                    s["last_call"] = now.isoformat()
                    day = now.date().isoformat()
                    daily = s.get("daily", {})
                    daily[day] = daily.get(day, 0) + 1
                    s["daily"] = daily
                    with open(self._stats_file, "w", encoding="utf-8") as sf:
                        json.dump(s, sf, ensure_ascii=False, indent=2)
                except Exception:
                    pass
                for a in payload.get("articles", []):
                    articles.append({
                        "url": a.get("url", ""),
                        "title": (a.get("title") or "").strip(),
                        "description": (a.get("description") or "").strip(),
                        "content": (a.get("content") or "").strip(),
                        "publishedAt": a.get("publishedAt", ""),
                        "origin": "newsapi",
                    })
                return articles
            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                raise
