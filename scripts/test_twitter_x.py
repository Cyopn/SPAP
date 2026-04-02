"""
Script de prueba para búsqueda en X/Twitter
Requiere X_BEARER_TOKEN en .env

Uso:
    python scripts/test_twitter_x.py "palabra clave" [límite]
    python scripts/test_twitter_x.py "inteligencia artificial" 5
    python scripts/test_twitter_x.py "python" 10
"""

from __future__ import annotations

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


from core.logger import log, log_exc
from core.news_finder import search_x

def pretty_print(items: list):
    """Imprime resultados con formato legible"""
    if not items:
        log("scripts/test_twitter_x: No se encontraron resultados.", "INFO")
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
            log(f"   Tweet: {s[:300]}{'...' if len(s) > 300 else ''}", "INFO")
        log("", "INFO")


def main():
    if len(sys.argv) < 2:
        log("Uso: python scripts/test_twitter_x.py <keyword> [limit]", "INFO")
        log("Ejemplo: python scripts/test_twitter_x.py 'inteligencia artificial' 5", "INFO")
        sys.exit(1)

    keyword = sys.argv[1]
    try:
        limit = int(sys.argv[2]) if len(sys.argv) >= 3 else 5
    except Exception:
        limit = 5

    bearer_token = os.environ.get("X_BEARER_TOKEN")
    print(f"DEBUG: Bearer Token obtenido: {'Sí' if bearer_token else 'No'}")

    if not bearer_token:
        log("❌ ERROR: X_BEARER_TOKEN no configurado en .env", "ERROR")
        log("", "INFO")
        log("INSTRUCCIONES PARA OBTENER X_BEARER_TOKEN:", "INFO")
        log("", "INFO")
        log("1. Ve a: https://developer.twitter.com/en/portal/dashboard", "INFO")
        log("2. Crea una 'App' si no tienes una", "INFO")
        log("3. En la sección 'Keys & tokens' → 'Bearer Token'", "INFO")
        log("4. Copia tu Bearer Token", "INFO")
        log("5. Agrega a tu .env:", "INFO")
        log("   X_BEARER_TOKEN=tu_bearer_token_aqui", "INFO")
        log("", "INFO")
        sys.exit(1)

    log(f"🔍 Buscando tweets con keyword: '{keyword}'", "INFO")
    log(f"📊 Límite de resultados: {limit}", "INFO")
    log("", "INFO")

    try:
        items = search_x(keyword=keyword, limit=limit,
                         bearer_token=bearer_token)

        if not items:
            log(f"No se encontraron tweets para: '{keyword}'", "INFO")
        else:
            log(f"✅ Se encontraron {len(items)} tweet(s):", "INFO")
            log("", "INFO")
            pretty_print(items)

    except ValueError as e:
        log(f"❌ Error de validación: {e}", "ERROR")
        sys.exit(1)
    except Exception as e:
        log_exc(f"❌ Error al buscar en X/Twitter para '{keyword}'", e)
        log("", "INFO")
        log("Posibles causas:", "INFO")
        log("  - Bearer Token inválido o expirado", "INFO")
        log("  - Límite de API alcanzado (500K tweets/mes para Free)", "INFO")
        log("  - Keyword muy específica sin resultados", "INFO")
        sys.exit(1)


if __name__ == "__main__":
    main()
