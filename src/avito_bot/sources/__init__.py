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
    """Availability of one source/city; independent of the service and of Telegram delivery."""
    AVAILABLE = 'AVAILABLE'
    CHECKING = 'CHECKING'
    RECOVERING = 'RECOVERING'
    RATE_LIMITED = 'RATE_LIMITED'
    VERIFICATION_REQUIRED = 'VERIFICATION_REQUIRED'
    ACCESS_DENIED = 'ACCESS_DENIED'
    PARSER_ERROR = 'PARSER_ERROR'
    NETWORK_UNAVAILABLE = 'NETWORK_UNAVAILABLE'


class ServiceState(StrEnum):
    RUNNING = 'RUNNING'
    RESTARTING = 'RESTARTING'
    FAILED = 'FAILED'


class DeliveryState(StrEnum):
    RUNNING = 'RUNNING'
    RETRYING = 'RETRYING'
    FAILED = 'FAILED'


# HTTP 429 answered with Avito's passable "access limited: IP problem" page: the user can lift it in
# the browser ("Continue" + check); otherwise it ends with its term like any rate limit.
IP_CHECK_REASON = 'HTTP 429 IP check'

# Waiting for a term: the scheduler itself resumes when next_allowed_at has passed.
TIMED_STATES = (SourceState.RECOVERING, SourceState.RATE_LIMITED,
                SourceState.PARSER_ERROR, SourceState.NETWORK_UNAVAILABLE)
# A person must pass a check or regain access; blind requests never lift these.
USER_STATES = (SourceState.VERIFICATION_REQUIRED, SourceState.ACCESS_DENIED)


def legacy_state(state: str, reason: str) -> str | None:
    """Map a source_state row written by releases before 2.0; None means drop the row."""
    if state in SourceState.__members__.values():
        return state
    if state in ('ACTIVE', 'PAUSED'):
        return SourceState.AVAILABLE
    if state == 'COOLDOWN':
        return SourceState.RATE_LIMITED if '429' in reason else SourceState.RECOVERING
    if state == 'WAITING_FOR_USER':
        # Repeated 429 no longer waits for /resume; its saved term stays binding.
        return {'challenge': SourceState.VERIFICATION_REQUIRED,
                'blocked': SourceState.ACCESS_DENIED}.get(reason, SourceState.RATE_LIMITED)
    if state == 'DEGRADED':
        return SourceState.PARSER_ERROR if reason == 'parse_changed' else SourceState.NETWORK_UNAVAILABLE
    return None  # STOPPED and other service-level rows are no longer source states


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


@dataclass(frozen=True)
class Search:
    """One feed check shared by every subscription and recipient of that city."""
    city: str
    feed: str
    url: str


@dataclass
class SourceHealth:
    started: bool
    closed: bool
    detail: dict = field(default_factory=dict)


class Source(Protocol):
    source_id: str
    @property
    def closed(self) -> bool: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def fetch(self, url: str) -> FetchResult: ...


class ListingsSource(Protocol):
    """Adapter contract for any provider: browser, an authorized feed, or test events."""
    source_id: str
    async def check(self, search: Search) -> FetchResult: ...
    async def health(self) -> SourceHealth: ...
    async def close(self) -> None: ...
