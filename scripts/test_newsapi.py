from __future__ import annotations
from core.logger import log, log_exc

import os
import sys
from pathlib import Path
try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*_args, **_kwargs):
        return False

ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(ROOT))

env_file = ROOT / ".env"
if env_file.exists():
    load_dotenv(env_file)


try:
    from bots.telegram_bot import perform_search
except Exception:
    try:
        from core.news_finder import search_newsapi
        perform_search = None
    except Exception as e:
        log_exc(
            "scripts/test_newsapi: No se pudo importar perform_search ni search_newsapi", e)
        raise


def pretty_print(items: list[dict]):
    if not items:
        log("scripts/test_newsapi: No se encontraron resultados.", "INFO")
        return
    for i, it in enumerate(items, start=1):
        def get(k):
            try:
                if isinstance(it, dict):
                    return it.get(k, "")
                return getattr(it, k, "")
            except Exception:
                return ""

        title = (get("title") or "(sin título)").strip()
        src = (get("source") or "").strip()
        pub = (get("published_at") or "").strip()
        url = (get("url") or "").strip()
        summary = (get("summary") or "")

        log(f"{i}. {title}", "INFO")
        log(f"   Fuente: {src} | Fecha: {pub}", "INFO")
        if url:
            log(f"   URL: {url}", "INFO")
        if summary:
            s = " ".join(str(summary).split())
            log(f"   Resumen: {s[:300]}{'...' if len(s) > 300 else ''}", "INFO")
        log("", "INFO")


def main():
    if len(sys.argv) < 2:
        log("Uso: python scripts/test_newsapi.py <keyword> [limit]", "INFO")
        sys.exit(1)
    keyword = sys.argv[1]
    try:
        limit = int(sys.argv[2]) if len(sys.argv) >= 3 else 5
    except Exception:
        limit = 5

    api_key = os.environ.get("NEWS_API")
    try:
        cfg_path = ROOT / "monitor_config.json"
        if cfg_path.exists():
            import json as _json
            try:
                with open(cfg_path, "r", encoding="utf-8") as f:
                    cfg = _json.load(f)
                sources = cfg.get("sources", ["google", "bing", "hn"]) or [
                    "google", "bing", "hn"]
                cfg_limit = int(cfg.get("limit", limit) or limit)
            except Exception:
                sources = ["google", "bing", "hn"]
                cfg_limit = limit
        else:
            sources = ["google", "bing", "hn"]
            cfg_limit = limit

        if perform_search is not None:
            log(f"Buscando '{keyword}' usando perform_search (limit={cfg_limit})...", "INFO")
            items = perform_search(keyword, sources, cfg_limit) or []
        else:
            if not api_key:
                log("ERROR: NEWS_API no definida en el entorno. Añádela a .env o exporta la variable.", "ERROR")
                sys.exit(2)
            log(
                f"perform_search no disponible; consultando NewsAPI directamente (limit={limit})...", "INFO")
            items = search_newsapi(keyword, limit) or []
    except Exception as e:
        log_exc("scripts/test_newsapi: Error durante la búsqueda", e)
        sys.exit(3)

    try:
        n_newsapi = sum(1 for it in items if ((it.get("source") if isinstance(
            it, dict) else getattr(it, "source", "")) or "").lower() == "newsapi")
        n_total = len(items)
        log(f"Resultados totales: {n_total} (newsapi={n_newsapi}, otras={n_total - n_newsapi})", "INFO")
    except Exception:
        pass

    pretty_print(items)


if __name__ == '__main__':
    main()
