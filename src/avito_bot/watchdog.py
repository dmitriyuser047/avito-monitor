"""Restart the process when a long-running loop stops showing signs of work.

Heartbeats use time.monotonic(): on macOS it does not advance while the computer sleeps, so waking
up is not mistaken for a hang. The supervisor starts the process again after a non-zero exit.
"""
import asyncio
import logging
import time
from collections.abc import Callable

log = logging.getLogger(__name__)

HUNG_EXIT_CODE = 75


class ComponentHung(RuntimeError):
    def __init__(self, names: list[str]):
        super().__init__('No heartbeat from: ' + ', '.join(names))
        self.names = names


class Watchdog:
    def __init__(self, limit: float, components: dict[str, Callable[[], float]]):
        self.limit, self.components = limit, components

    def stale(self, now: float | None = None) -> list[str]:
        now = time.monotonic() if now is None else now
        return [name for name, beat in self.components.items() if now - beat() > self.limit]

    async def run(self, interval: float = 30) -> None:
        while True:
            await asyncio.sleep(interval)
            stale = self.stale()
            if stale:
                log.error('Watchdog: no heartbeat for %ss from %s; restarting the process',
                          int(self.limit), ', '.join(stale))
                raise ComponentHung(stale)
