"""Bounded restart policy shared by macOS/Linux and Windows launchers."""
import subprocess
import signal
import sys
import time
from collections import deque

WINDOW = 1800
MAX_RESTARTS = 5
DELAYS = (10, 30, 60, 120, 300)


def restart_delay(code: int, failures: list[float], now: float) -> int | None:
    if code in (0, 78, 130, -2, -15):
        return None
    recent = [t for t in failures if now - t < WINDOW]
    if len(recent) >= MAX_RESTARTS:
        return None
    return DELAYS[min(len(recent), len(DELAYS)-1)]


def main():
    failures = deque()
    while True:
        child = subprocess.Popen([sys.executable, '-c', 'from avito_bot import main; main()'])
        def stop(*_):
            child.terminate()
            try:
                child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
            raise SystemExit(0)
        signal.signal(signal.SIGTERM, stop)
        try:
            code = child.wait()
        except KeyboardInterrupt:
            child.terminate()
            try:
                child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
            return
        now = time.monotonic()
        while failures and now - failures[0] >= WINDOW:
            failures.popleft()
        delay = restart_delay(code, list(failures), now)
        if delay is None:
            print(f'Бот остановлен (код {code}). Автоматических перезапусков больше нет.', flush=True)
            raise SystemExit(code if code > 0 else 0)
        failures.append(now)
        print(f'Сбой (код {code}), перезапуск через {delay} с. Лимит: {MAX_RESTARTS} за 30 мин.', flush=True)
        try:
            time.sleep(delay)
        except KeyboardInterrupt:
            return
