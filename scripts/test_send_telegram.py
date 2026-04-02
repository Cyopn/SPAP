from __future__ import annotations

from core import storage
from core import telegram
from core.timezone_mx import now_mx_iso


def main():
    cfg = storage.get_config("monitor_config") or {}

    item = {
        "title": "Prueba: artículo de impacto alto",
        "url": "https://example.com/test-alto-impact",
        "summary": "Este es un mensaje de prueba generado por scripts/test_send_telegram.py",
        "published_at": now_mx_iso(),
        "level": "alto",
        "emoji": "🔴",
        "source": "test_script",
    }

    targets = telegram.get_target_chats_for_item(item, cfg=cfg)
    if not targets:
        print("No hay chats activos configurados para recibir alertas de impacto alto.")
        return

    print("Enviando prueba a chats:", ", ".join(
        t.get("chat_id", "") for t in targets))
    results = telegram.send_item_notification_to_targets(item, cfg=cfg)
    ok = [r.get("chat_id") for r in results if r.get("ok")]
    fail = [r.get("chat_id") for r in results if not r.get("ok")]
    print("Enviados OK:", ok)
    print("Fallidos:", fail)


if __name__ == "__main__":
    main()
