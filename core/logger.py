from __future__ import annotations

from datetime import datetime
import traceback
from typing import Optional


def _now_formatted() -> str:
    try:
        return datetime.now().strftime("%H:%M:%S - %d/%m/%Y")
    except Exception:
        return datetime.utcnow().strftime("%H:%M:%S - %d/%m/%Y")


def log(message: str, level: str = "INFO") -> None:
    try:
        import re

        ts_pattern = re.compile(r"^\d{2}:\d{2}:\d{2} - \d{2}/\d{2}/\d{4}")
        if isinstance(message, str) and ts_pattern.match(message):
            try:
                import sys

                sys.stdout.write(message + "\n")
            except Exception:
                pass
            return

        ts = _now_formatted()
        try:
            import sys

            sys.stdout.write(f"{ts} [{level}] {message}\n")
        except Exception:
            pass
    except Exception:
        try:
            import sys
            sys.stdout.write(f"{message}\n")
        except Exception:
            pass


def log_exc(message: str, exc: Optional[BaseException] = None) -> None:
    try:
        import re
        ts_pattern = re.compile(r"^\d{2}:\d{2}:\d{2} - \d{2}/\d{2}/\d{4}")
        if isinstance(message, str) and ts_pattern.match(message):
            try:
                import sys

                sys.stdout.write(message + "\n")
            except Exception:
                pass
        else:
            ts = _now_formatted()
            try:
                import sys

                sys.stdout.write(f"{ts} [ERROR] {message}\n")
            except Exception:
                pass

        if exc is not None:
            traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stdout)
        else:
            traceback.print_stack(file=sys.stdout)
    except Exception:
        pass


def now() -> str:
    return _now_formatted()
