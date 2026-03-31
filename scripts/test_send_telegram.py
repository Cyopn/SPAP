from __future__ import annotations

from core import storage
from core import telegram
import time


def main():
    cfg = storage.get_config("monitor_config") or {}
    target = None
    try:
        target = cfg.get("telegram_target_chat") if isinstance(
            cfg, dict) else None
    except Exception:
        target = None

    if not target:
        print("No target chat configured in monitor_config (key 'telegram_target_chat'). Aborting.")
        return

    item = {
        "title": "Prueba: artículo de impacto crítico",
        "url": "https://example.com/test-high-impact",
        "summary": "Este es un mensaje de prueba generado por scripts/test_send_telegram.py",
        "published_at": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        "level": "CRITICO",
        "emoji": "🔴",
        "source": "test_script",
    }

    print(f"Sending test alert to configured chat: {target}")
    ok = telegram.send_item_notification(item, str(target))
    print("Sent:", ok)


if __name__ == "__main__":
    main()
