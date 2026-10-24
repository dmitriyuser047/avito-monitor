"""Replaceable data source contract; no Telegram or matcher policy here."""
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol

from ..matcher import Item


class FetchStatus(StrEnum):
    OK = 'ok'
    NO_RESULTS = 'no_results'
    BLOCKED = 'blocked'
    CHALLENGE = 'challenge'
    RATE_LIMITED = 'rate_limited'
    PARSE_CHANGED = 'parse_changed'
    NETWORK_ERROR = 'network_error'


class SourceState(StrEnum):
    ACTIVE = 'ACTIVE'
    COOLDOWN = 'COOLDOWN'
    WAITING_FOR_USER = 'WAITING_FOR_USER'
    DEGRADED = 'DEGRADED'
    PAUSED = 'PAUSED'
    STOPPED = 'STOPPED'


@dataclass
class FetchResult:
    status: FetchStatus
    items: list[Item]
    source_id: str
    city: str
    observed_at: float
    http_status: int | None = None
    retry_after_sec: float | None = None
    diagnostics: dict = field(default_factory=dict)


class Source(Protocol):
    source_id: str
    @property
    def closed(self) -> bool: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def fetch(self, url: str) -> FetchResult: ...
