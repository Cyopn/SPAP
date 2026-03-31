from __future__ import annotations

from core import storage
from core.logger import log


def generate_simple_report(limit: int = 100) -> str:
    items = storage.read_items(limit)
    lines = [
        f"ID {i['id']} | {i['published_at'] or i['created_at']} | {i['title']}" for i in items]
    return "\n".join(lines)


if __name__ == "__main__":
    log(generate_simple_report(50), "INFO")
