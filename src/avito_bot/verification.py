"""Confirms a passed check from the page already open in the browser; never makes a request.

The open document is trusted only when it is newer than the restriction: either it is the
restriction page itself (answered by a navigation), or the main frame navigated afterwards.
light_fetch parses answers off-page, so after a fetch() restriction the page may still show an
older, normal catalog; reading it would falsely "confirm" access.
"""
import logging
import time
from urllib.parse import urlparse

from .sources import FetchStatus, IP_CHECK_REASON, SourceState, USER_STATES

log = logging.getLogger(__name__)


class VerificationCoordinator:
    def __init__(self, monitor, interval: float):
        self.monitor, self.interval = monitor, interval
        # (city, since) -> (navigation generation at the restriction, the DOM shows the restriction)
        self._baseline: dict[tuple[str, float], tuple[int, bool]] = {}
        self._confirmed: set[tuple[str, float]] = set()
        self._next_read = 0.0

    def _generation(self) -> int:
        return getattr(self.monitor.scraper, 'navigation_generation', 0)

    def note_restriction(self, city: str, since: float, dom_shows_restriction: bool) -> None:
        self._baseline[(city, since)] = (self._generation(), dom_shows_restriction)

    def _eligible(self, state: dict) -> bool:
        # Restored from an earlier run: only a navigation in this run makes the DOM trustworthy.
        generation, dom_shows = self._baseline.get((state['city'], state['since']), (0, False))
        return dom_shows or self._generation() > generation

    def pending(self) -> list[dict]:
        return [s for s in self.monitor.db.source_states(self.monitor.source_id)
                if (s['state'] in USER_STATES
                    or (s['state'] == SourceState.RATE_LIMITED and s['reason'] == IP_CHECK_REASON))
                and (s['city'], s['since']) not in self._confirmed]

    def describe(self) -> str:
        if not self.pending():
            return 'не требуется'
        scraper = self.monitor.scraper
        page_url = getattr(getattr(scraper, 'page', None), 'url', '') or ''
        if (urlparse(page_url).hostname or '').removeprefix('www.') != 'avito.ru':
            return 'в окне Chrome не открыта страница Авито (обновите страницу или /unblock)'
        if not any(self._eligible(s) for s in self.pending()):
            return 'открыта старая страница; обновите её в окне Chrome, чтобы показать проверку'
        return f'страница читается локально раз в {self.interval:g} с, без запросов'

    async def tick(self) -> bool:
        """Read the open page at a bounded local rate; True when a pending check was confirmed."""
        now = time.monotonic()
        if now < self._next_read:
            return False
        pending = self.pending()
        if not pending:
            return False
        self._next_read = now + self.interval
        scraper = self.monitor.scraper
        if not hasattr(scraper, 'read_current') or scraper.closed:
            return False
        page_url = getattr(getattr(scraper, 'page', None), 'url', '') or ''
        if (urlparse(page_url).hostname or '').removeprefix('www.') != 'avito.ru':
            return False
        if not any(self._eligible(s) for s in pending):
            return False
        result = await scraper.read_current()
        if result.status not in (FetchStatus.OK, FetchStatus.NO_RESULTS):
            return False
        log.info('Verification confirmed from the loaded page: cities=%s', [s['city'] for s in pending])
        self._confirmed.update((s['city'], s['since']) for s in pending)
        await self.monitor.confirm_verification(result, page_url, pending)
        return True
