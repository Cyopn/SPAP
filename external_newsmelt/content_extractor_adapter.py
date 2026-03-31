from __future__ import annotations

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import requests
import os
from typing import List, Dict, Any

DEFAULT_TIMEOUT = 15


class ContentExtractorAdapter:
    def __init__(self, max_workers: int = 3):
        self.max_workers = max_workers

    def _fetch_content(self, url: str) -> str:
        try:
            resp = requests.get(url, timeout=DEFAULT_TIMEOUT, headers={
                                "User-Agent": "news-finder/1.0"})
            resp.raise_for_status()
            text = resp.text or ""
            return text[:20000]
        except requests.RequestException:
            return ""

    def process_articles(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []

        def work(item: Dict[str, Any]) -> Dict[str, Any]:
            url = item.get("url") or ""
            content = ""
            if url:
                content = self._fetch_content(url)
            item = dict(item)
            item.setdefault("content", "")
            if content:
                item["content"] = content
            item["extracted_at"] = datetime.now(timezone.utc).isoformat()
            return item

        if self.max_workers and self.max_workers > 1:
            with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
                futures = [ex.submit(work, it) for it in items]
                for f in as_completed(futures):
                    try:
                        results.append(f.result())
                    except Exception:
                        continue
        else:
            for it in items:
                try:
                    results.append(work(it))
                except Exception:
                    results.append(it)

        return results
