"""Confirms access from the page already open in the browser; never makes a request.

Any stop without a server-given term (a check, a block, the bot's own pause after 429 or an error)
ends as soon as the open Avito page shows a normal catalog: the source is plainly available.

The open document is trusted only when it is newer than the restriction: either it is the
restriction page itself (answered by a navigation), or the main frame navigated afterwards.
light_fetch parses answers off-page, so after a fetch() restriction the page may still show an
older, normal catalog; reading it would falsely "confirm" access.
"""
import logging
import time
from urllib.parse import urlparse

from .config import feed_url
from .listings_collector import same_search
from .sources import FetchStatus, IP_CHECK_REASON, TIMED_STATES, USER_STATES

log = logging.getLogger(__name__)


class VerificationCoordinator:
    def __init__(self, monitor, interval: float):
        self.monitor, self.interval = monitor, interval
        # (city, since) -> (navigation generation at the restriction, the DOM shows the restriction)
        self._baseline: dict[tuple[str, float], tuple[str, int, bool]] = {}
        self._confirmed: set[tuple[str, float]] = set()
        self._next_read = 0.0

    def _generation(self) -> int:
        return getattr(self.monitor.scraper, 'navigation_generation', 0)

    def note_restriction(self, city: str, since: float, dom_shows_restriction: bool) -> None:
        # A new answer may reuse `since` while RATE_LIMITED persists across a passed check.
        # It is a fresh restriction, not an already completed event; also bound 24/7 memory use.
        for key in [key for key in self._baseline if key[0] == city]:
            self._baseline.pop(key)
        self._confirmed = {key for key in self._confirmed if key[0] != city}
        self._baseline[(city, since)] = (getattr(self.monitor.scraper, 'run_id', ''),
                                          self._generation(), dom_shows_restriction)

    def _eligible(self, state: dict) -> bool:
        # Restored from an earlier run: only a navigation in this run makes the DOM trustworthy.
        epoch, generation, dom_shows = self._baseline.get((state['city'], state['since']), ('', 0, False))
        if epoch != getattr(self.monitor.scraper, 'run_id', ''):
            generation, dom_shows = 0, False
        return dom_shows or self._generation() > generation

    def pending(self) -> list[dict]:
        now = time.time()
        pending = []
        for s in self.monitor.db.source_states(self.monitor.source_id):
            if (s['city'], s['since']) in self._confirmed:
                continue
            if self.monitor.access.get(s['city']).verification_required or s['state'] in USER_STATES:
                pending.append(s)
            # The bot's own pause can end early; a Retry-After from Avito cannot. A passed IP check is
            # still acknowledged under Retry-After, and collection then starts when the term ends.
            elif (s['state'] in TIMED_STATES and (s['next_allowed_at'] or 0) > now
                  and (not (s.get('server_until') or 0) > now or s['reason'] == IP_CHECK_REASON)):
                pending.append(s)
        return pending

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
        matching = [s for s in pending if same_search(
            page_url, feed_url(s['city'], self.monitor.cfg.feed))]
        eligible = [s for s in matching if self._eligible(s)]
        attached = getattr(scraper, 'session_mode', 'managed') == 'cdp_attach'
        if not eligible and not (attached and matching):
            return False
        result = await scraper.read_current()
        if result.status not in (FetchStatus.OK, FetchStatus.NO_RESULTS):
            # On reconnect, observing the check itself establishes a trustworthy DOM baseline.
            if result.status in (FetchStatus.CHALLENGE, FetchStatus.BLOCKED):
                for s in matching:
                    self.note_restriction(s['city'], s['since'], True)
            return False
        # CDP can attach to an already loaded document. Its actual creation time must be newer
        # than the restriction when there is no navigation/check evidence in this connection.
        if not eligible:
            began = result.diagnostics.get('document_started_at')
            eligible = [s for s in matching if began is not None and began > s['since']]
        eligible = [s for s in eligible if s['city'] == result.city]
        if not eligible or not same_search(page_url, getattr(scraper.page, 'url', '')):
            return False
        log.info('Verification confirmed from the loaded page: cities=%s', [s['city'] for s in eligible])
        # Persist and commit before changing the in-memory completion marker. SQLite failure is retryable.
        await self.monitor.confirm_verification(result, page_url, eligible)
        self._confirmed.update((s['city'], s['since']) for s in eligible)
        return True
