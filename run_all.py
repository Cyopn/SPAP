from __future__ import annotations
import os
import sys
import subprocess
import threading
import time
import re
from pathlib import Path
from dotenv import load_dotenv
from core.logger import log, now


def _stream_reader(pipe, prefix: str):
    ts_pattern = re.compile(r'^(?P<ts>\d{2}:\d{2}:\d{2} - \d{2}/\d{2}/\d{4})(?:\s*(?P<level>\[.*?\]))?\s*(?P<rest>.*)$')
    try:
        for line in iter(pipe.readline, ''):
            if not line:
                break
            raw = line.rstrip()
            m = ts_pattern.match(raw)
            if m:
                inner_ts = m.group('ts')
                level = m.group('level') or ''
                rest = m.group('rest') or ''
                if level:
                    out = f"{inner_ts} [{prefix}] {level} {rest}".rstrip()
                else:
                    out = f"{inner_ts} [{prefix}] {rest}".rstrip()
            else:
                ts = now()
                out = f"{ts} [{prefix}] {raw}"
            try:
                log(out, "INFO")
            except Exception:
                try:
                    sys.stdout.write(out + "\n")
                except Exception:
                    pass
    except Exception:
        pass


def start_process(python_exe: str, script: Path, env: dict, name: str) -> subprocess.Popen:
    proc = subprocess.Popen(
        [python_exe, str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )
    t = threading.Thread(target=_stream_reader, args=(
        proc.stdout, name), daemon=True)
    t.start()
    return proc


def main():
    base = Path(__file__).parent
    load_dotenv(base / ".env")

    python_exe = sys.executable or "python"
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")

    scripts = [
        (None, "python", ["-m", "web.app"], "flask"),
        (None, "python", ["-m", "bots.telegram_bot"], "bot"),
        (None, "python", ["-m", "monitors.monitor"], "monitor"),
    ]

    procs = []
    try:
        for entry in scripts:
            _, _, args, name = entry
            log(f"[launcher] Iniciando {name} -> python {' '.join(args)}")
            proc = subprocess.Popen(
                [python_exe] + args,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
            )
            t = threading.Thread(target=_stream_reader, args=(
                proc.stdout, name), daemon=True)
            t.start()
            procs.append((proc, name))

        while True:
            alive = any(p.poll() is None for p, _ in procs)
            if not alive:
                log("[launcher] Todos los procesos terminaron.")
                break
            time.sleep(0.5)

    except KeyboardInterrupt:
        log("[launcher] Interrupción recibida, deteniendo procesos...")
    finally:
        for p, name in procs:
            if p.poll() is None:
                try:
                    log(f"[launcher] Terminando {name} (pid={p.pid})...")
                    p.terminate()
                except Exception:
                    pass
        time.sleep(1)
        for p, name in procs:
            if p.poll() is None:
                try:
                    log(f"[launcher] Forzando kill {name} (pid={p.pid})...")
                    p.kill()
                except Exception:
                    pass


if __name__ == "__main__":
    main()
