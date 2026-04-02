from __future__ import annotations

import json
import sqlite3
from typing import Any
from core.timezone_mx import now_mx_iso

DB_PATH = "pa_feed.db"


def _connect():
    return sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if s in ("1", "true", "t", "yes", "y", "on", "si", "sí"):
        return True
    if s in ("0", "false", "f", "no", "n", "off"):
        return False
    return default


def _ensure_schema():
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source TEXT,
            title TEXT,
            url TEXT,
            summary TEXT,
            published_at TEXT,
            keyword TEXT
        )
        """
        )

        cur.execute("PRAGMA table_info(items)")
        existing_cols = {r[1] for r in cur.fetchall()}
        extra_cols = {
            "extracted_at": "TEXT",
            "level": "TEXT",
            "emoji": "TEXT",
            "color": "TEXT",
            "origin": "TEXT",
            "ingested_by": "TEXT",
            "tg_message_id": "TEXT",
            "views_count": "INTEGER DEFAULT 0",
            "shares_count": "INTEGER DEFAULT 0",
        }
        for col, typ in extra_cols.items():
            if col not in existing_cols:
                cur.execute(f"ALTER TABLE items ADD COLUMN {col} {typ}")

        try:
            cur.execute(
                """
            UPDATE items
            SET ingested_by = CASE
                WHEN LOWER(COALESCE(ingested_by, '')) IN ('monitor', 'telegram_search', 'telegram_add') THEN LOWER(ingested_by)
                WHEN LOWER(COALESCE(origin, '')) = 'telegram_search' THEN 'telegram_search'
                WHEN LOWER(COALESCE(origin, '')) IN ('telegram_manual', 'telegram_add') THEN 'telegram_add'
                WHEN LOWER(COALESCE(source, '')) = 'telegram' THEN 'telegram_add'
                ELSE 'monitor'
            END
            WHERE COALESCE(TRIM(ingested_by), '') = ''
               OR LOWER(COALESCE(ingested_by, '')) IN ('search', 'telegram_manual', 'telegram', 'realtime')
            """
            )
        except Exception:
            pass

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS search_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            results TEXT
        )
        """
        )

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS chat_states (
            chat_id TEXT PRIMARY KEY,
            state TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        )

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS duplicates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signature TEXT,
            item_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        )

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS configs (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        )

        try:
            cur.execute("SELECT value FROM configs WHERE key = ?",
                        ("classifier_config",))
            row = cur.fetchone()
            cfg_raw = json.loads(row[0]) if row and row[0] else None
            if isinstance(cfg_raw, dict) and (
                "high" in cfg_raw or "medium" in cfg_raw or "low" in cfg_raw
            ):
                migrated_cfg = {
                    "alto": {"keywords": (cfg_raw.get("alto") or cfg_raw.get("high") or {}).get("keywords", [])},
                    "medio": {"keywords": (cfg_raw.get("medio") or cfg_raw.get("medium") or {}).get("keywords", [])},
                    "bajo": {"keywords": (cfg_raw.get("bajo") or cfg_raw.get("low") or {}).get("keywords", [])},
                }
                cur.execute(
                    "UPDATE configs SET value = ?, updated_at = ? WHERE key = ?",
                    (json.dumps(migrated_cfg, ensure_ascii=False), now_mx_iso(), "classifier_config"),
                )
        except Exception:
            pass

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS telegram_targets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL UNIQUE,
            label TEXT,
            enabled INTEGER DEFAULT 1,
            send_alerts INTEGER DEFAULT 1,
            send_alto INTEGER DEFAULT 1,
            send_medio INTEGER DEFAULT 0,
            send_bajo INTEGER DEFAULT 0,
            send_report_manual INTEGER DEFAULT 1,
            send_report_manual_alto INTEGER DEFAULT 1,
            send_report_manual_medio INTEGER DEFAULT 1,
            send_report_manual_bajo INTEGER DEFAULT 1,
            send_report_auto INTEGER DEFAULT 0,
            send_report_auto_alto INTEGER DEFAULT 1,
            send_report_auto_medio INTEGER DEFAULT 1,
            send_report_auto_bajo INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        )

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS item_telegram_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id INTEGER NOT NULL,
            chat_id TEXT NOT NULL,
            message_id TEXT,
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(item_id, chat_id)
        )
        """
        )

        cur.execute("PRAGMA table_info(telegram_targets)")
        target_cols = {r[1] for r in cur.fetchall()}
        target_extra_cols = {
            "label": "TEXT",
            "enabled": "INTEGER DEFAULT 1",
            "send_alerts": "INTEGER DEFAULT 1",
            "send_alto": "INTEGER DEFAULT 1",
            "send_medio": "INTEGER DEFAULT 0",
            "send_bajo": "INTEGER DEFAULT 0",
            "send_report_manual": "INTEGER DEFAULT 1",
            "send_report_manual_alto": "INTEGER DEFAULT 1",
            "send_report_manual_medio": "INTEGER DEFAULT 1",
            "send_report_manual_bajo": "INTEGER DEFAULT 1",
            "send_report_auto": "INTEGER DEFAULT 0",
            "send_report_auto_alto": "INTEGER DEFAULT 1",
            "send_report_auto_medio": "INTEGER DEFAULT 1",
            "send_report_auto_bajo": "INTEGER DEFAULT 1",
            "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        }
        for col, typ in target_extra_cols.items():
            if col not in target_cols:
                cur.execute(
                    f"ALTER TABLE telegram_targets ADD COLUMN {col} {typ}")
        target_cols.update(target_extra_cols.keys())

        # Migración one-shot de columnas legacy -> columnas canónicas.
        try:
            if "send_alto" in target_cols:
                if "send_high" in target_cols and "send_critical" in target_cols:
                    cur.execute(
                        """
                        UPDATE telegram_targets
                        SET send_alto = CASE
                            WHEN COALESCE(send_alto, 0) = 1
                              OR COALESCE(send_high, 0) = 1
                              OR COALESCE(send_critical, 0) = 1 THEN 1
                            ELSE 0
                        END
                        """
                    )
                elif "send_high" in target_cols:
                    cur.execute(
                        """
                        UPDATE telegram_targets
                        SET send_alto = CASE
                            WHEN COALESCE(send_alto, 0) = 1 OR COALESCE(send_high, 0) = 1 THEN 1
                            ELSE 0
                        END
                        """
                    )
                elif "send_critical" in target_cols:
                    cur.execute(
                        """
                        UPDATE telegram_targets
                        SET send_alto = CASE
                            WHEN COALESCE(send_alto, 0) = 1 OR COALESCE(send_critical, 0) = 1 THEN 1
                            ELSE 0
                        END
                        """
                    )

            if "send_medio" in target_cols and "send_medium" in target_cols:
                cur.execute(
                    """
                    UPDATE telegram_targets
                    SET send_medio = CASE
                        WHEN COALESCE(send_medio, 0) = 1 OR COALESCE(send_medium, 0) = 1 THEN 1
                        ELSE 0
                    END
                    """
                )

            if "send_bajo" in target_cols and "send_low" in target_cols:
                cur.execute(
                    """
                    UPDATE telegram_targets
                    SET send_bajo = CASE
                        WHEN COALESCE(send_bajo, 0) = 1 OR COALESCE(send_low, 0) = 1 THEN 1
                        ELSE 0
                    END
                    """
                )
        except Exception:
            pass

        # Inicializa columnas de reportes con valores útiles si estaban vacías.
        try:
            if "send_alerts" in target_cols:
                cur.execute(
                    """
                    UPDATE telegram_targets
                    SET send_alerts = CASE
                        WHEN send_alerts IS NULL THEN COALESCE(enabled, 1)
                        ELSE send_alerts
                    END
                    """
                )
            if "send_report_manual" in target_cols:
                cur.execute(
                    """
                    UPDATE telegram_targets
                    SET send_report_manual = CASE
                        WHEN send_report_manual IS NULL THEN COALESCE(enabled, 1)
                        ELSE send_report_manual
                    END
                    """
                )
            if "send_report_manual_alto" in target_cols and "send_report_manual_medio" in target_cols and "send_report_manual_bajo" in target_cols:
                cur.execute(
                    """
                    UPDATE telegram_targets
                    SET
                        send_report_manual_alto = CASE WHEN send_report_manual_alto IS NULL THEN 1 ELSE send_report_manual_alto END,
                        send_report_manual_medio = CASE WHEN send_report_manual_medio IS NULL THEN 1 ELSE send_report_manual_medio END,
                        send_report_manual_bajo = CASE WHEN send_report_manual_bajo IS NULL THEN 1 ELSE send_report_manual_bajo END
                    """
                )
            if "send_report_auto" in target_cols:
                cur.execute(
                    """
                    UPDATE telegram_targets
                    SET send_report_auto = CASE
                        WHEN send_report_auto IS NULL THEN 0
                        ELSE send_report_auto
                    END
                    """
                )
            if "send_report_auto_alto" in target_cols and "send_report_auto_medio" in target_cols and "send_report_auto_bajo" in target_cols:
                cur.execute(
                    """
                    UPDATE telegram_targets
                    SET
                        send_report_auto_alto = CASE WHEN send_report_auto_alto IS NULL THEN 1 ELSE send_report_auto_alto END,
                        send_report_auto_medio = CASE WHEN send_report_auto_medio IS NULL THEN 1 ELSE send_report_auto_medio END,
                        send_report_auto_bajo = CASE WHEN send_report_auto_bajo IS NULL THEN 1 ELSE send_report_auto_bajo END
                    """
                )
        except Exception:
            pass

        # Bootstrap: si no hay destinos en la tabla nueva, migrar configuración legacy.
        try:
            cur.execute("SELECT COUNT(1) FROM telegram_targets")
            targets_count = int((cur.fetchone() or [0])[0] or 0)
        except Exception:
            targets_count = 0

        if targets_count == 0:
            try:
                cur.execute("SELECT value FROM configs WHERE key = ?",
                            ("monitor_config",))
                row = cur.fetchone()
                cfg = json.loads(row[0]) if row and row[0] else {}
                if isinstance(cfg, dict):
                    legacy_chat = str(
                        cfg.get("telegram_target_chat") or "").strip()
                    legacy_alerts = _to_bool(cfg.get("telegram_alerts"), False)
                    if legacy_chat:
                        cur.execute(
                            """
                            INSERT OR IGNORE INTO telegram_targets
                            (chat_id, label, enabled, send_alerts, send_alto, send_medio, send_bajo, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                legacy_chat,
                                "Principal",
                                1 if legacy_alerts else 0,
                                1 if legacy_alerts else 0,
                                1 if legacy_alerts else 0,
                                0,
                                0,
                                now_mx_iso(),
                            ),
                        )
            except Exception:
                pass


_ensure_schema()


def _normalize_ingested_by(raw_ingested_by: str | None, origin: str | None, source: str | None) -> str:
    val = (raw_ingested_by or "").strip().lower()
    if val in ("monitor", "telegram_search", "telegram_add"):
        return val

    orig_l = (origin or "").strip().lower()
    src_l = (source or "").strip().lower()

    if "telegram_search" in orig_l:
        return "telegram_search"
    if "telegram_manual" in orig_l or "telegram_add" in orig_l:
        return "telegram_add"
    if src_l == "telegram":
        return "telegram_add"
    if "monitor" in orig_l:
        return "monitor"
    return "monitor"


def find_existing_item_id(url: str | None = None, title: str | None = None) -> int | None:
    u = str(url or "").strip()
    t = str(title or "").strip()
    if not u and not t:
        return None
    try:
        with _connect() as conn:
            cur = conn.cursor()
            if u:
                cur.execute(
                    "SELECT id FROM items WHERE url = ? ORDER BY id DESC LIMIT 1", (u,))
            else:
                cur.execute(
                    "SELECT id FROM items WHERE title = ? ORDER BY id DESC LIMIT 1", (t,))
            row = cur.fetchone()
            return int(row[0]) if row else None
    except Exception:
        return None


def append_item(
    source: str | dict,
    channel: str | None = None,
    title: str | None = None,
    url: str | None = None,
    summary: str | None = None,
    published_at: str | None = None,
    keyword: str | None = None,
    extracted_at: str | None = None,
    level: str | None = None,
    emoji: str | None = None,
    color: str | None = None,
    origin: str | None = None,
    ingested_by: str | None = None,
    meta: str | None = None,
) -> int:

    if isinstance(source, dict):
        item = source
        src = item.get("source", "") or ""
        ch = item.get("channel", "") or ""
        t = item.get("title", "") or ""
        u = item.get("url", "") or ""
        s = item.get("summary", "") or ""
        p = item.get("published_at", "") or ""
        k = item.get("keyword", "") or ""
        extracted = item.get("extracted_at", "") or ""
        lvl = item.get("level", "") or ""
        emj = item.get("emoji", "") or ""
        col = item.get("color", "") or ""
        orig = item.get("origin", "") or ""
        ing_by = item.get("ingested_by", "") or ""
        meta_v = item.get("meta", "") or ""
    else:
        src = source or ""
        ch = channel or ""
        t = title or ""
        u = url or ""
        s = summary or ""
        p = published_at or ""
        k = keyword or ""
        extracted = extracted_at or ""
        lvl = level or ""
        emj = emoji or ""
        col = color or ""
        orig = origin or ""
        ing_by = ingested_by or ""
        meta_v = meta or ""

    if not extracted:
        try:
            extracted = now_mx_iso()
        except Exception:
            extracted = ""
    existing_id = find_existing_item_id(u, t)
    if existing_id is not None:
        return int(existing_id)

    ingested_by_norm = _normalize_ingested_by(ing_by, orig, src)

    with _connect() as conn:
        cur = conn.cursor()
        created_at = now_mx_iso()
        cur.execute(
            "INSERT INTO items (created_at, source, title, url, summary, published_at, keyword, extracted_at, level, emoji, color, origin, ingested_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (created_at, src, t, u, s, p, k, extracted, lvl, emj, col, orig, ingested_by_norm),
        )
        conn.commit()
        return int(cur.lastrowid or 0)


def read_items(limit: int = 200) -> list[dict[str, Any]]:
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, created_at, source, title, url, summary, published_at, keyword, extracted_at, level, emoji, color, origin, ingested_by, tg_message_id, views_count, shares_count FROM items ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        out = [dict(zip(cols, row)) for row in rows]
        return out


def get_latest_id() -> int:
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM items ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        return int(row[0]) if row else 0


def store_search_results(chat_id: str, results: list[dict[str, Any]]) -> int:
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO search_results (chat_id, results, created_at) VALUES (?, ?, ?)",
            (str(chat_id), json.dumps(results, ensure_ascii=False), now_mx_iso()),
        )
        conn.commit()
        return int(cur.lastrowid or 0)


def load_search_results(chat_id: str) -> list[dict[str, Any]] | None:
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT results FROM search_results WHERE chat_id = ? ORDER BY id DESC LIMIT 1", (str(chat_id),))
        row = cur.fetchone()
        if not row:
            return None
        return json.loads(row[0])


def set_state(chat_id: str, state: dict[str, Any]) -> None:
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO chat_states (chat_id, state, updated_at) VALUES (?, ?, ?)",
            (str(chat_id), json.dumps(state, ensure_ascii=False), now_mx_iso()),
        )
        conn.commit()


def get_state(chat_id: str) -> dict[str, Any] | None:
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT state FROM chat_states WHERE chat_id = ?", (str(chat_id),))
        row = cur.fetchone()
        if not row:
            return None
        return json.loads(row[0])


def clear_state(chat_id: str) -> None:
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM chat_states WHERE chat_id = ?",
                    (str(chat_id),))
        conn.commit()


def log_duplicate(signature: str, item_id: int | None = None) -> int:
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO duplicates (signature, item_id, created_at) VALUES (?, ?, ?)",
            (signature, item_id, now_mx_iso()),
        )
        conn.commit()
        return int(cur.lastrowid or 0)


def set_config(key: str, value: Any) -> None:
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO configs (key, value, updated_at) VALUES (?, ?, ?)",
            (key, json.dumps(value, ensure_ascii=False), now_mx_iso()),
        )
        conn.commit()


def set_tg_message_id(item_id: int, message_id: Any) -> None:
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE items SET tg_message_id = ? WHERE id = ?",
                    (str(message_id), int(item_id)))
        conn.commit()


def increment_item_engagement(item_id: int, action: str) -> None:
    action_norm = str(action or "").strip().lower()
    if action_norm not in ("view", "share"):
        return

    column = "views_count" if action_norm == "view" else "shares_count"
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE items SET {column} = COALESCE({column}, 0) + 1 WHERE id = ?",
            (int(item_id),),
        )
        conn.commit()


def record_item_telegram_message(item_id: int, chat_id: str, message_id: Any) -> int:
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM item_telegram_messages WHERE item_id = ? AND chat_id = ?",
            (int(item_id), str(chat_id or "").strip()),
        )
        cur.execute(
            "INSERT INTO item_telegram_messages (item_id, chat_id, message_id, sent_at) VALUES (?, ?, ?, ?)",
            (int(item_id), str(chat_id or "").strip(), str(
                message_id) if message_id is not None else "", now_mx_iso()),
        )
        conn.commit()
        return int(cur.lastrowid or 0)


def list_telegram_targets(include_disabled: bool = True) -> list[dict[str, Any]]:
    with _connect() as conn:
        cur = conn.cursor()
        if include_disabled:
            cur.execute(
                """
                SELECT id, chat_id, label, enabled, send_alerts, send_alto, send_medio, send_bajo,
                       send_report_manual, send_report_manual_alto, send_report_manual_medio, send_report_manual_bajo,
                       send_report_auto, send_report_auto_alto, send_report_auto_medio, send_report_auto_bajo
                FROM telegram_targets
                ORDER BY id ASC
                """
            )
        else:
            cur.execute(
                """
                SELECT id, chat_id, label, enabled, send_alerts, send_alto, send_medio, send_bajo,
                       send_report_manual, send_report_manual_alto, send_report_manual_medio, send_report_manual_bajo,
                       send_report_auto, send_report_auto_alto, send_report_auto_medio, send_report_auto_bajo
                FROM telegram_targets
                WHERE COALESCE(enabled, 1) = 1
                ORDER BY id ASC
                """
            )

        rows = cur.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            send_alerts = _to_bool(row[4], True)
            send_alto = _to_bool(row[5], True)
            send_medio = _to_bool(row[6], False)
            send_bajo = _to_bool(row[7], False)
            send_report_manual = _to_bool(row[8], True)
            send_report_manual_alto = _to_bool(row[9], True)
            send_report_manual_medio = _to_bool(row[10], True)
            send_report_manual_bajo = _to_bool(row[11], True)
            send_report_auto = _to_bool(row[12], False)
            send_report_auto_alto = _to_bool(row[13], True)
            send_report_auto_medio = _to_bool(row[14], True)
            send_report_auto_bajo = _to_bool(row[15], True)
            out.append(
                {
                    "id": int(row[0]),
                    "chat_id": str(row[1] or "").strip(),
                    "label": str(row[2] or "").strip(),
                    "enabled": _to_bool(row[3], True),
                    "send_alerts": send_alerts,
                    "send_alto": send_alto,
                    "send_medio": send_medio,
                    "send_bajo": send_bajo,
                    "send_report_manual": send_report_manual,
                    "send_report_manual_alto": send_report_manual_alto,
                    "send_report_manual_medio": send_report_manual_medio,
                    "send_report_manual_bajo": send_report_manual_bajo,
                    "send_report_auto": send_report_auto,
                    "send_report_auto_alto": send_report_auto_alto,
                    "send_report_auto_medio": send_report_auto_medio,
                    "send_report_auto_bajo": send_report_auto_bajo,
                }
            )
        return out


def replace_telegram_targets(targets: list[dict[str, Any]] | None) -> int:
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()

    for raw in (targets or []):
        if not isinstance(raw, dict):
            continue
        chat_id = str(raw.get("chat_id") or "").strip()
        if not chat_id:
            continue

        key = chat_id.lower()
        if key in seen:
            continue
        seen.add(key)

        normalized.append(
            {
                "chat_id": chat_id,
                "label": str(raw.get("label") or "").strip(),
                "enabled": _to_bool(raw.get("enabled"), True),
                "send_alerts": _to_bool(raw.get("send_alerts"), True),
                "send_alto": _to_bool(raw.get("send_alto"), True),
                "send_medio": _to_bool(raw.get("send_medio"), False),
                "send_bajo": _to_bool(raw.get("send_bajo"), False),
                "send_report_manual": _to_bool(raw.get("send_report_manual"), True),
                "send_report_manual_alto": _to_bool(raw.get("send_report_manual_alto"), True),
                "send_report_manual_medio": _to_bool(raw.get("send_report_manual_medio"), True),
                "send_report_manual_bajo": _to_bool(raw.get("send_report_manual_bajo"), True),
                "send_report_auto": _to_bool(raw.get("send_report_auto"), False),
                "send_report_auto_alto": _to_bool(raw.get("send_report_auto_alto"), True),
                "send_report_auto_medio": _to_bool(raw.get("send_report_auto_medio"), True),
                "send_report_auto_bajo": _to_bool(raw.get("send_report_auto_bajo"), True),
            }
        )

    with _connect() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM telegram_targets")
        for t in normalized:
            cur.execute(
                """
                INSERT INTO telegram_targets
                (chat_id, label, enabled, send_alerts, send_alto, send_medio, send_bajo,
                 send_report_manual, send_report_manual_alto, send_report_manual_medio, send_report_manual_bajo,
                 send_report_auto, send_report_auto_alto, send_report_auto_medio, send_report_auto_bajo,
                 updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    t["chat_id"],
                    t["label"],
                    1 if t["enabled"] else 0,
                    1 if t["send_alerts"] else 0,
                    1 if t["send_alto"] else 0,
                    1 if t["send_medio"] else 0,
                    1 if t["send_bajo"] else 0,
                    1 if t["send_report_manual"] else 0,
                    1 if t["send_report_manual_alto"] else 0,
                    1 if t["send_report_manual_medio"] else 0,
                    1 if t["send_report_manual_bajo"] else 0,
                    1 if t["send_report_auto"] else 0,
                    1 if t["send_report_auto_alto"] else 0,
                    1 if t["send_report_auto_medio"] else 0,
                    1 if t["send_report_auto_bajo"] else 0,
                    now_mx_iso(),
                ),
            )
        conn.commit()

    return len(normalized)


def get_config(key: str) -> Any | None:
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT value FROM configs WHERE key = ?", (key,))
        row = cur.fetchone()
        if not row:
            return None
        return json.loads(row[0])


def get_mentions_count() -> int:
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM items")
        row = cur.fetchone()
        return int(row[0] or 0)


__all__ = [
    "append_item",
    "read_items",
    "get_latest_id",
    "store_search_results",
    "load_search_results",
    "set_state",
    "get_state",
    "clear_state",
    "log_duplicate",
    "set_config",
    "get_config",
    "get_mentions_count",
    "set_tg_message_id",
    "increment_item_engagement",
    "find_existing_item_id",
    "record_item_telegram_message",
    "list_telegram_targets",
    "replace_telegram_targets",
]
