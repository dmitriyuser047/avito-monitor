"""Bounded per-process cache for reusing one listing photo across Telegram recipients.

Never caches failures or persists photos; clearing on restart is intentional.
"""
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable


class PhotoCache:
    def __init__(self, download: Callable[[str], Awaitable[bytes | None]],
                 max_entries: int = 16, max_bytes: int = 8_000_000, ttl_seconds: int = 300):
        self.download = download
        self.max_entries = max_entries
        self.max_bytes = max_bytes
        self.ttl_seconds = ttl_seconds
        self._data: OrderedDict[str, tuple[float, bytes]] = OrderedDict()
        self._size = 0

    def clear(self) -> None:
        self._data.clear()
        self._size = 0

    async def get(self, url: str) -> bytes | None:
        now = time.monotonic()
        for key, (expires, data) in list(self._data.items()):
            if expires <= now:
                self._size -= len(data)
                del self._data[key]
        cached = self._data.get(url)
        if cached is not None:
            self._data.move_to_end(url)
            return cached[1]
        photo = await self.download(url)
        if not photo or len(photo) > self.max_bytes or not self.max_entries:
            return photo
        while self._data and (len(self._data) >= self.max_entries or self._size + len(photo) > self.max_bytes):
            _, (_, old) = self._data.popitem(last=False)
            self._size -= len(old)
        self._data[url] = (now + self.ttl_seconds, photo)
        self._size += len(photo)
        return photo
