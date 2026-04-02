from __future__ import annotations

from datetime import datetime, timedelta, timezone

# Zona horaria fija solicitada: UTC-6 (Ciudad de Mexico / centro de Mexico).
MX_TZ = timezone(timedelta(hours=-6), name="UTC-6")


def now_mx() -> datetime:
    return datetime.now(MX_TZ)


def now_mx_iso() -> str:
    return now_mx().isoformat()


def to_mx(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=MX_TZ)
    return dt.astimezone(MX_TZ)
