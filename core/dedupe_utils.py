from __future__ import annotations

import hashlib
from typing import Optional

from . import storage


def signature_for_item(title: str, url: str) -> str:
    s = (title or "") + "|" + (url or "")
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def is_duplicate(signature: str) -> bool:
    return False


def log_duplicate(signature: str, item_id: Optional[int] = None) -> int:
    try:
        return storage.log_duplicate(signature, item_id)
    except Exception:
        return -1


__all__ = ["signature_for_item", "is_duplicate", "log_duplicate"]
