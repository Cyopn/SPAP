from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

DB_PATH = "pa_feed.db"


def _connect():
    return sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)


def _ensure_schema():
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source TEXT,
            channel TEXT,
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
        }
        for col, typ in extra_cols.items():
            if col not in existing_cols:
                cur.execute(f"ALTER TABLE items ADD COLUMN {col} {typ}")

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


_ensure_schema()


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
        meta_v = meta or ""

    if not extracted:
        try:
            extracted = datetime.now(timezone.utc).isoformat()
        except Exception:
            extracted = ""
    try:
        with _connect() as conn:
            cur = conn.cursor()
            if u:
                cur.execute(
                    "SELECT id FROM items WHERE url = ? ORDER BY id DESC LIMIT 1", (u,))
                row = cur.fetchone()
                if row:
                    return int(row[0])
            else:
                cur.execute(
                    "SELECT id FROM items WHERE title = ? ORDER BY id DESC LIMIT 1", (t,))
                row = cur.fetchone()
                if row:
                    return int(row[0])
    except Exception:
        pass

    try:
        orig_l = (orig or "").lower()
        if any(k in orig_l for k in ("search", "bot", "crawler", "finder", "newsapi")):
            ingested_by = "search"
        else:
            ingested_by = "monitor"
    except Exception:
        ingested_by = "monitor"

    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO items (source, title, url, summary, published_at, keyword, extracted_at, level, emoji, color, origin, ingested_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (src, t, u, s, p, k, extracted, lvl, emj, col, orig, ingested_by),
        )
        conn.commit()
        return int(cur.lastrowid or 0)


def read_items(limit: int = 200) -> list[dict[str, Any]]:
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, created_at, source, title, url, summary, published_at, keyword, extracted_at, level, emoji, color, origin, ingested_by, tg_message_id FROM items ORDER BY id DESC LIMIT ?",
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
        cur.execute("INSERT INTO search_results (chat_id, results) VALUES (?, ?)", (str(
            chat_id), json.dumps(results, ensure_ascii=False)))
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
        cur.execute("INSERT OR REPLACE INTO chat_states (chat_id, state, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)", (str(
            chat_id), json.dumps(state, ensure_ascii=False)))
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
            "INSERT INTO duplicates (signature, item_id) VALUES (?, ?)", (signature, item_id))
        conn.commit()
        return int(cur.lastrowid or 0)


def set_config(key: str, value: Any) -> None:
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO configs (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                    (key, json.dumps(value, ensure_ascii=False)))
        conn.commit()


def set_tg_message_id(item_id: int, message_id: Any) -> None:
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE items SET tg_message_id = ? WHERE id = ?",
                    (str(message_id), int(item_id)))
        conn.commit()


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
]
