from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import time
import logging
from typing import List, Dict, Any

import requests
import newspaper
import spacy
from newspaper import Config as NewspaperConfig

DEFAULT_TIMEOUT = 15


class ContentExtractorFull:
    def __init__(self, max_workers: int = 5):
        self.max_workers = max_workers
        self.nlp = self._load_spacy_model()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "news-finder/1.0"})

    def _load_spacy_model(self):
        try:
            return spacy.load("en_core_web_sm")
        except OSError:
            import subprocess
            subprocess.run(["python", "-m", "spacy", "download",
                           "en_core_web_sm"], check=False)
            return spacy.load("en_core_web_sm")

    def find_final_url(self, url: str) -> str:
        try:
            resp = self.session.head(
                url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
            final = resp.url
            skip_keywords = ["consent", "error", "blocked",
                             "access-denied", "captcha", "robot"]
            if any(k in final.lower() for k in skip_keywords):
                return url
            return final
        except Exception:
            return url

    def get_article_content(self, url: str) -> str:
        cfg = NewspaperConfig()
        cfg.request_timeout = DEFAULT_TIMEOUT
        cfg.browser_user_agent = "news-finder/1.0"
        cfg.fetch_images = False
        cfg.memoize_articles = False
        try:
            art = newspaper.Article(url, config=cfg)
            art.download()
            art.parse()
            return art.text or ""
        except Exception as e:
            logging.debug(f"newspaper extraction failed for {url}: {e}")
            return ""

    def filter_content(self, text: str, num_sentences: int = 4) -> str:
        if not text:
            return ""
        try:
            doc = self.nlp(text)
            sents = [s.text.strip() for s in doc.sents]
            return " ".join(sents[:num_sentences])
        except Exception:
            return text[:1000]

    def clean_text(self, text: str) -> str:
        if not text:
            return ""
        import re
        t = str(text)
        t = re.sub(r'\s+', ' ', t).strip()
        t = re.sub(r'[^0-9a-zA-Z\s\.,!?;:()\-\n]', '', t)
        return t

    def process_single(self, item: Dict[str, Any]) -> Dict[str, Any]:
        url = item.get("url") or ""
        final_url = self.find_final_url(url) if url else url
        raw = self.get_article_content(final_url) if final_url else ""
        spacy_filtered = self.filter_content(raw)
        raw_clean = self.clean_text(raw)
        spacy_clean = self.clean_text(spacy_filtered)

        final_content = ""
        parts = [p for p in [spacy_clean, raw_clean, item.get(
            "summary", ""), item.get("title", "")] if p]
        if parts:
            final_content = " ".join(parts)

        item = dict(item)
        item["raw_full_content"] = raw_clean
        item["spacy_full_content"] = spacy_clean
        item["final_full_content"] = final_content
        item["extracted_at"] = datetime.now(timezone.utc).isoformat()
        return item

    def process_articles(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        from concurrent.futures import ThreadPoolExecutor, as_completed

        if not items:
            return []

        max_workers = min(self.max_workers, len(
            items)) if self.max_workers and self.max_workers > 0 else 1

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(self.process_single, it): it for it in items}
            for f in as_completed(futures):
                try:
                    results.append(f.result())
                except Exception:
                    try:
                        results.append(futures[f])
                    except Exception:
                        continue

        return results
