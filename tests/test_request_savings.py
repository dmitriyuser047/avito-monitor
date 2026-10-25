"""Request-saving regressions: all tests are offline."""
import asyncio
from unittest.mock import AsyncMock, Mock
from types import SimpleNamespace

from avito_bot.photo_cache import PhotoCache
from avito_bot.scraper import Scraper
from avito_bot.sources import FetchStatus


def test_photo_cache_reuses_bytes_for_multiple_recipients():
    async def scenario():
        downloader = AsyncMock(return_value=b'jpeg')
        cache = PhotoCache(downloader)
        assert await cache.get('https://fixture.test/img1') == b'jpeg'
        assert await cache.get('https://fixture.test/img1') == b'jpeg'
        downloader.assert_awaited_once()
        assert await cache.get('https://fixture.test/img2') == b'jpeg'
        assert downloader.await_count == 2
    asyncio.run(scenario())


def test_photo_cache_bounded_and_no_failure_cache():
    async def scenario():
        downloader = AsyncMock(side_effect=[None, b'abc', b'def', b'ghi'])
        cache = PhotoCache(downloader, max_entries=1, max_bytes=3)
        assert await cache.get('a') is None
        assert await cache.get('a') == b'abc'
        assert await cache.get('b') == b'def'
        assert await cache.get('a') == b'ghi'
        assert downloader.await_count == 4
        cache.clear()
        assert cache._size == 0 and not cache._data
    asyncio.run(scenario())


def test_browser_waits_for_content_not_another_navigation(tmp_path):
    async def scenario():
        source = Scraper(tmp_path/'p', None, fetch_mode='page_navigation')
        response = SimpleNamespace(status=200, all_headers=AsyncMock(return_value={}))
        page = SimpleNamespace(
            goto=AsyncMock(return_value=response),
            wait_for_function=AsyncMock(),
            evaluate=AsyncMock(side_effect=[
                {'catalog': False, 'items': [], 'status': 200},
                {'catalog': True, 'empty': True, 'items': [], 'status': 200}
            ]),
            url='https://www.avito.ru/kazan/bytovaya_elektronika?s=104'
        )
        source.page = page
        result = await source.fetch(page.url)
        assert result.status is FetchStatus.NO_RESULTS
        page.goto.assert_awaited_once()
        page.wait_for_function.assert_awaited_once()
        assert page.evaluate.await_count == 2
    asyncio.run(scenario())
