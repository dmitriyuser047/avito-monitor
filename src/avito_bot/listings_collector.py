"""Read an already available search once, or perform one scheduled collection operation."""
import time
from urllib.parse import parse_qsl, urlsplit

from .sources import FetchStatus


def search_key(url: str):
    u = urlsplit(url)
    return (u.scheme, (u.hostname or '').removeprefix('www.'), u.port,
            u.path.rstrip('/'), tuple(sorted(parse_qsl(u.query, keep_blank_values=True))))


def same_search(left: str, right: str) -> bool:
    try:
        return search_key(left) == search_key(right)
    except ValueError:
        return False


class ListingsCollector:
    """The scheduler shares each result across subscribers. No time-based result cache."""
    def __init__(self, scraper, max_dom_age=120):
        self.scraper, self.max_dom_age = scraper, max_dom_age
        self._consumed = None

    def _document(self):
        return self.scraper.run_id, self.scraper.navigation_generation

    async def check(self, search):
        scraper = self.scraper
        document = self._document()
        # Managed startup stays about:blank. Attached Chrome can already contain the search.
        if (getattr(scraper, 'session_mode', 'managed') == 'cdp_attach'
                and document != self._consumed
                and same_search(getattr(scraper.page, 'url', ''), search.url)):
            result = await scraper.read_current()
            self._consumed = document
            began = result.diagnostics.get('document_started_at')
            if (result.status not in (FetchStatus.OK, FetchStatus.NO_RESULTS)
                    or began is not None and 0 <= time.time() - began <= self.max_dom_age):
                result.diagnostics.update(actual_mode='loaded_dom', is_warmup=False)
                return result
        result = await scraper.fetch(search.url)
        self._consumed = self._document()
        return result

    def consumed(self):
        self._consumed = self._document()
