import asyncio
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from playwright.async_api import async_playwright

from avito_bot.scraper import INSPECT_FN, LIGHT_JS, Scraper, parse_result, retry_after_seconds
from avito_bot.sources import FetchStatus

FIXTURES = Path(__file__).with_name('fixtures')


@pytest.mark.parametrize(('code','raw','expected'), [
    (403, {}, FetchStatus.BLOCKED),
    (429, {'retry_after':'120'}, FetchStatus.RATE_LIMITED),
    (439, {}, FetchStatus.CHALLENGE),
    (200, {'blocked': True}, FetchStatus.BLOCKED),
    (200, {'challenge': True}, FetchStatus.CHALLENGE),
    (200, {'catalog':True,'empty':True}, FetchStatus.NO_RESULTS),
    (200, {'catalog':True}, FetchStatus.PARSE_CHANGED),
    (200, {}, FetchStatus.PARSE_CHANGED),
    (500, {}, FetchStatus.NETWORK_ERROR),
])
def test_fetch_classification(code, raw, expected):
    result = parse_result(dict(status=code, **raw), 'test', 'kazan')
    assert result.status is expected and result.http_status == code
    if code == 429:
        assert result.retry_after_sec == 120


def test_retry_after_dates_invalid_and_nonfinite():
    now = datetime(2026,10,24,12,0,tzinfo=timezone.utc).timestamp()
    assert retry_after_seconds('Sat, 24 Oct 2026 12:02:00 GMT', now) == 120
    assert retry_after_seconds('-10', now) == 0
    assert retry_after_seconds('junk') is None
    assert retry_after_seconds('inf') is None


@pytest.mark.parametrize('code', [403,429,439,500])
def test_light_error_never_causes_navigation(tmp_path, code):
    async def scenario():
        scraper = Scraper(tmp_path/'profile',None)
        scraper.light_ok = True
        scraper.page = SimpleNamespace(evaluate=AsyncMock(return_value={'status':code, 'items':[], 'retry_after':'45'}))
        scraper.load = AsyncMock()
        result = await scraper.fetch('https://www.avito.ru/kazan/bytovaya_elektronika?s=104')
        assert result.http_status == code
        scraper.load.assert_not_awaited()
        scraper.page.evaluate.assert_awaited_once()
    asyncio.run(scenario())


def test_network_error_never_causes_navigation(tmp_path):
    async def scenario():
        scraper = Scraper(tmp_path/'profile',None)
        scraper.light_ok=True
        scraper.page=SimpleNamespace(evaluate=AsyncMock(side_effect=TimeoutError()))
        scraper.load=AsyncMock()
        assert (await scraper.fetch('https://www.avito.ru/kazan/x')).status is FetchStatus.NETWORK_ERROR
        scraper.load.assert_not_awaited()
    asyncio.run(scenario())


def test_closed_includes_page_not_just_context(tmp_path):
    scraper = Scraper(tmp_path/'profile',None)
    scraper.page = SimpleNamespace(is_closed=lambda:True)
    assert scraper.closed


def test_debug_html_opt_in_and_expiry(tmp_path):
    import os
    sample = tmp_path/'feed_sample.html'
    sample.write_text('legacy raw sensitive HTML')
    Scraper(tmp_path/'profile',None)
    assert not sample.exists()
    sample.write_text('old')
    os.utime(sample,(time.time()-90000,)*2)
    Scraper(tmp_path/'profile',None,save_debug_html=True)
    assert not sample.exists()


def test_dom_fixtures_and_light_fetch_offline():
    async def scenario():
        async with async_playwright() as pw:
            try:
                browser = await pw.chromium.launch(headless=True)
            except Exception:
                browser = await pw.chromium.launch(channel='chrome',headless=True)
            context = await browser.new_context()
            # Every request is intercepted. Fixtures never reach Avito or the internet.
            await context.route('**/*', lambda route: route.fulfill(status=200,body='<html></html>',content_type='text/html'))
            page=await context.new_page()
            expected = {'catalog':FetchStatus.OK,'empty':FetchStatus.NO_RESULTS,'blocked':FetchStatus.BLOCKED,
                        'challenge':FetchStatus.CHALLENGE,'changed':FetchStatus.PARSE_CHANGED,'malformed':FetchStatus.PARSE_CHANGED}
            for name,status in expected.items():
                await page.set_content((FIXTURES/f'{name}.html').read_text())
                raw=await page.evaluate(f'({INSPECT_FN})(document)')
                result=parse_result(dict(status=200,**raw),'test','kazan')
                assert result.status is status, (name,raw)
                if name=='catalog':
                    assert result.items[0].published == 1792830000
                    assert result.items[0].price == 18000
                    assert '?' not in result.items[0].url
            await page.goto('https://fixture.test/')
            await page.route('https://fixture.test/feed',lambda route: route.fulfill(
                status=200,body=(FIXTURES/'catalog.html').read_text(),content_type='text/html'))
            raw=await page.evaluate(LIGHT_JS,['https://fixture.test/feed',True])
            result=parse_result(raw,'test','kazan')
            assert result.status is FetchStatus.OK
            # textContent works even in a detached DOMParser document.
            assert 'Советский район' in result.items[0].location
            assert 'Описание видеокарты' in result.items[0].description
            assert '<script' not in raw['html'] and 'secret' not in raw['html']
            await browser.close()
    asyncio.run(scenario())
