"""Scripted listings source for tests and demos; it never touches the network."""
import time
from collections import deque

from . import FetchResult, FetchStatus, Search, SourceHealth


class MockListingsSource:
    source_id = 'mock'

    def __init__(self, results: list[FetchResult] | None = None):
        self.results = deque(results or [])
        self.checks: list[Search] = []
        self.closed = False

    def push(self, result: FetchResult) -> None:
        self.results.append(result)

    async def check(self, search: Search) -> FetchResult:
        self.checks.append(search)
        if self.results:
            return self.results.popleft()
        return FetchResult(FetchStatus.NO_RESULTS, [], self.source_id, search.city, time.time(), 200)

    async def health(self) -> SourceHealth:
        return SourceHealth(started=not self.closed, closed=self.closed, detail={'queued': len(self.results)})

    async def close(self) -> None:
        self.closed = True
