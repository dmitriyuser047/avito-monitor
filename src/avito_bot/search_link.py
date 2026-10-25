"""Turn a ready Avito search link into subscription filters, without opening the link."""
import re
from urllib.parse import parse_qs, urlparse

CITY = re.compile(r'[a-z][a-z_\-]*')


def parse_avito_link(text: str) -> tuple[dict | None, str | None]:
    """(filters, None) or (None, reason). Only parameters that can be read reliably are used."""
    parsed = urlparse(text.strip())
    host = (parsed.hostname or '').removeprefix('www.').removeprefix('m.')
    if parsed.scheme not in ('http', 'https') or host != 'avito.ru':
        return None, 'Это не ссылка на поиск Авито.'
    parts = [p for p in parsed.path.split('/') if p]
    if not parts or not CITY.fullmatch(parts[0]):
        return None, 'В ссылке нет города. Откройте поиск в нужном городе и скопируйте адрес.'
    if parts[0] in ('rossiya', 'all'):
        return None, 'Поиск по всей России не поддерживается: бот следит за лентами городов.'
    params = parse_qs(parsed.query)
    query = ' '.join(params.get('q', [''])[0].split())[:100]
    if not query:
        return None, 'В ссылке нет текста поиска (параметр q). Добавьте запрос через мастер /add.'
    def number(name):
        value = params.get(name, [''])[0]
        return int(value) if value.isdigit() else None
    pmin, pmax = number('pmin'), number('pmax')
    if pmin is not None and pmax is not None and pmin > pmax:
        pmin, pmax = pmax, pmin
    return dict(query=query, pmin=pmin, pmax=pmax, city=parts[0],
                section='/'.join(parts[1:]) or None), None
