"""Настоящий браузер (Playwright) с постоянным профилем: открывает ленты
Авито и достаёт объявления из страницы.

Headless Авито распознаёт, поэтому окно обычное, но свёрнутое. Когда Авито
показывает «Доступ ограничен», окно разворачивается — капчу проходит человек.

Предпочитаем установленный Google Chrome без флага --enable-automation: капча
(GeeTest/hCaptcha) в тестовом Chromium под автоматизацией может не запуститься.
Если Chrome нет — встроенный Chromium.
"""

import logging
from enum import Enum
from pathlib import Path

from playwright.async_api import BrowserContext, Page, async_playwright

from .matcher import Item

log = logging.getLogger(__name__)


# Достаёт объявления из документа: и из открытой страницы, и из HTML, полученного fetch
EXTRACT_FN = """(root) => [...root.querySelectorAll('[data-marker="catalog-serp"] [data-marker="item"]')].map(e => {
  const q = s => e.querySelector(s);
  const link = q('[data-marker="item-title"]') || q('a[itemprop="url"]');
  return {
    id: e.dataset.itemId,
    title: q('[itemprop="name"]')?.textContent?.trim() || '',
    price: q('[itemprop="price"]')?.getAttribute('content'),
    href: link?.getAttribute('href') || '',
    date: q('[data-marker="item-date"]')?.textContent?.trim() || '',
    image: q('img')?.getAttribute('src'),
    location: q('[data-marker="item-location"]')?.innerText?.replace(/\\s+/g, ' ').trim() || '',
    // описание — абзац в 4 строки; цена, район и продавец — другие абзацы
    description: (q('p[style*="max-lines-size:4"]') || q('[class*="description"]'))
      ?.textContent?.trim()?.slice(0, 600) || '',
  };
})"""

# Точное время публикации есть только в служебном JSON страницы (script type="mime/invalid"):
# у объявлений поле sortTimeStamp, мс. Ищем все объекты с id и sortTimeStamp.
TIMES_FN = """(root) => {
  const out = {};
  const walk = (o, depth) => {
    if (!o || typeof o !== 'object' || depth > 25) return;
    if (Array.isArray(o)) { for (const x of o) walk(x, depth + 1); return; }
    if (o.id && o.sortTimeStamp) out[o.id] = o.sortTimeStamp;
    for (const k in o) walk(o[k], depth + 1);
  };
  for (const s of root.querySelectorAll('script[type="mime/invalid"]')) {
    try { walk(JSON.parse(s.textContent), 0); } catch (e) {}
  }
  return out;
}"""

# Лёгкий опрос: один запрос HTML ленты из уже открытой страницы Авито (те же cookies и
# отпечаток настоящего браузера), без загрузки 350 картинок, скриптов и аналитики.
LIGHT_JS = f"""async ([url, wantHtml]) => {{
  const r = await fetch(url, {{credentials: 'include', cache: 'no-store',
                               headers: {{'accept': 'text/html,application/xhtml+xml'}}}});
  const html = await r.text();
  const doc = new DOMParser().parseFromString(html, 'text/html');
  const items = ({EXTRACT_FN})(doc), times = ({TIMES_FN})(doc);
  for (const i of items) i.ts = times[i.id] || null;
  return {{status: r.status, url: r.url, title: doc.title, items, html: wantHtml ? html : null}};
}}"""



class Status(Enum):
    OK = "ok"
    BLOCKED = "blocked"  # «Доступ ограничен» — нужна капча / пауза
    EMPTY = "empty"      # страница загрузилась, но объявлений нет — возможно, сменилась вёрстка
    ERROR = "error"


class Scraper:
    def __init__(self, profile_dir: Path, proxy_url: str | None, headless: bool = False):
        self.profile_dir = profile_dir
        self.proxy_url = proxy_url
        self.headless = headless
        self._pw = None
        self.ctx: BrowserContext | None = None
        self.page: Page | None = None
        self._final_url: dict[str, str] = {}  # адрес ленты после редиректа Авито
        self.light_ok = False  # открыта ли страница Авито, из которой можно делать лёгкие запросы
        self.closed = False  # окно браузера закрыли — работать дальше нельзя
        self.sample_path: Path | None = profile_dir.parent / "feed_sample.html"

    async def start(self) -> None:
        self._pw = await async_playwright().start()
        options = dict(
            headless=self.headless,
            proxy={"server": self.proxy_url} if self.proxy_url else None,
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            no_viewport=True,
            args=["--disable-blink-features=AutomationControlled", "--window-size=1400,900"],
            ignore_default_args=["--enable-automation"],
        )
        try:
            # у Chrome свой профиль: профиль от более новой версии Chromium он может не открыть
            self.ctx = await self._pw.chromium.launch_persistent_context(
                str(self.profile_dir.with_name(self.profile_dir.name + "-chrome")), channel="chrome", **options)
            log.info("Браузер: Google Chrome")
        except Exception as e:
            log.info("Google Chrome не найден (%s), использую встроенный Chromium", str(e).splitlines()[0])
            self.ctx = await self._pw.chromium.launch_persistent_context(
                str(self.profile_dir), channel="chromium", **options)
        self.page = self.ctx.pages[0] if self.ctx.pages else await self.ctx.new_page()
        self.ctx.on("close", lambda _: setattr(self, "closed", True))
        await self.set_window("minimized")

    async def stop(self) -> None:
        if self.ctx:
            await self.ctx.close()
        if self._pw:
            await self._pw.stop()

    async def set_window(self, state: str) -> None:
        """state: 'minimized' или 'normal'."""
        if self.headless or not self.page:
            return
        try:
            cdp = await self.ctx.new_cdp_session(self.page)
            win = await cdp.send("Browser.getWindowForTarget")
            await cdp.send("Browser.setWindowBounds",
                           {"windowId": win["windowId"], "bounds": {"windowState": state}})
            await cdp.detach()
        except Exception as e:
            log.debug("Не удалось изменить окно: %s", e)

    async def fetch(self, url: str) -> tuple[Status, list[Item]]:
        """Лента: лёгким запросом, а если Авито просит проверку — полной загрузкой страницы."""
        if self.light_ok:
            try:
                res = await self.page.evaluate(LIGHT_JS, [self._final_url.get(url, url), self.sample_path is not None])
                if res.get("html") and self.sample_path:
                    self.sample_path.write_text(res["html"], encoding="utf-8")  # один образец для диагностики
                    self.sample_path = None
                if res["status"] == 200 and "Доступ ограничен" not in res["title"]:
                    self._final_url[url] = res["url"]
                    items = [_to_item(r) for r in res["items"] if r.get("id")]
                    return (Status.OK if items else Status.EMPTY), items
                log.info("Лёгкий запрос: %s «%s» — открываю страницу целиком", res["status"], res["title"][:40])
            except Exception as e:
                # сеть моргнула — повторим в следующем цикле, без лишней полной загрузки
                log.warning("Лёгкий запрос не удался: %s", e)
                return Status.ERROR, []
        self.light_ok = False
        status, items = await self.load(url)
        if status is Status.OK:
            self.light_ok = True
            self._final_url[url] = self.page.url
        return status, items

    async def load(self, url: str) -> tuple[Status, list[Item]]:
        """Полная загрузка страницы — так проходит JS-проверка Авито и показывается капча."""
        try:
            await self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
            # первая загрузка может пройти через JS-проверку (код 439) и редирект
            try:
                await self.page.wait_for_selector('[data-marker="catalog-serp"]', timeout=12000)
            except Exception:
                pass
            status, items = await self.read_current()
            # «проверка безопасности» проходит сама за несколько секунд — ждём её
            for _ in range(10):
                if status is not Status.BLOCKED or "проверка безопасности" not in await self.page.title():
                    break
                await self.page.wait_for_timeout(2000)
                status, items = await self.read_current()
            return status, items
        except Exception as e:
            log.warning("Ошибка загрузки %s: %s", url, e)
            return Status.ERROR, []

    async def captcha_open(self) -> bool:
        """Открыто ли окно hCaptcha (человек нажал «Продолжить» и решает)."""
        try:
            for frame in self.page.frames:
                if "hcaptcha" in frame.url and "frame=challenge" in frame.url:
                    el = await frame.frame_element()
                    if await el.is_visible():
                        return True
        except Exception:
            pass
        return False

    async def download(self, url: str) -> bytes | None:
        """Скачать картинку тем же браузером (прокси, cookies) — Telegram сам её не достаёт."""
        try:
            r = await self.ctx.request.get(url, headers={"referer": "https://www.avito.ru/"}, timeout=10000)
            return await r.body() if r.ok else None
        except Exception as e:
            log.debug("Не удалось скачать %s: %s", url, e)
            return None

    async def read_current(self) -> tuple[Status, list[Item]]:
        """Разобрать то, что сейчас открыто, без перезагрузки."""
        try:
            title = await self.page.title()
            if "Доступ ограничен" in title:
                return Status.BLOCKED, []
            raw = await self.page.evaluate(
                f"(() => {{ const items = ({EXTRACT_FN})(document), times = ({TIMES_FN})(document);"
                f" for (const i of items) i.ts = times[i.id] || null; return items; }})()")
        except Exception as e:
            log.warning("Ошибка разбора страницы: %s", e)
            return Status.ERROR, []
        items = [_to_item(r) for r in raw if r.get("id")]
        if items:
            self.light_ok = True  # открыта нормальная страница Авито (в т.ч. после капчи)
        return (Status.OK if items else Status.EMPTY), items


def _to_item(r: dict) -> Item:
    href = r["href"].split("?")[0]
    parts = href.strip("/").split("/")  # kazan/tovary_dlya_kompyutera/videokarta_..._123
    price = r.get("price")
    return Item(
        id=int(r["id"]),
        title=r["title"],
        price=int(price) if price and price.isdigit() else None,
        url=f"https://www.avito.ru{href}" if href.startswith("/") else href,
        date=r["date"],
        image=r.get("image"),
        location=r["location"],
        description=r["description"],
        published=r["ts"] / 1000 if r.get("ts") else None,
        feed=parts[1] if len(parts) > 2 else "",  # раздел из адреса объявления
        city=parts[0] if len(parts) > 2 else "",
    )
