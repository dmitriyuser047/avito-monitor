"""Поиск: к какому разделу относится объявление и подходит ли оно под запрос.

1. Название приводится к единому виду: «RTX3060Ti» → «rtx 3060 ti», «1660S» → «1660 super»,
   «16 ГБ» → «16gb», кириллица в моделях («ртх», «райзен») → латиница.
2. Объявление относится к одному или нескольким разделам (видеокарта, процессор, ПК…):
   сначала по явным словам («Видеокарта», «Блок питания»), иначе по моделям (rtx, ryzen, ddr4).
3. Запрос = раздел (выбран кнопкой или угадан по слову «видеокарта») + модель + цена + исключения.
   У готовых ПК модель ищется и в описании — там часто перечислена конфигурация.
"""

import re
from dataclasses import dataclass

from .db import Subscription

# Свежее — до 3 часов: объявление может появиться в поиске только после модерации,
# уже с пометкой «1 час назад». «Вчера» и старше — обычно поднятые старые.
FRESH_DATE = re.compile(r"только что|сек|мин|\b[1-3] час", re.I)

# Объявления о покупке («Куплю видеокарту») не присылаем никогда.
WANTED = re.compile(r"^(?:куплю|скупк|скупаю|выкуп|приму|ищу)|\bкуплю\b|\bскупка\b")

PC_FEED = "nastolnye_kompyutery"


@dataclass
class Item:
    id: int
    title: str
    price: int | None
    url: str
    date: str
    image: str | None
    location: str
    description: str
    feed: str = ""  # из какой ленты пришло
    city: str = ""  # город ленты, как в адресе Авито


@dataclass(frozen=True)
class Category:
    key: str
    name: str
    query_words: tuple[str, ...]  # начала слов в запросе, означающие раздел
    noun: str | None  # явное название в объявлении
    hint: str | None  # признаки модели, если явного названия нет


B = r"(?<![a-zа-я0-9])"  # граница слова
E = r"(?![a-zа-я])"

CATEGORIES = [
    Category("gpu", "Видеокарты", ("видеокарт", "видюх", "видях", "видюшк", "gpu"),
             r"видеокарт|видюх|видях",
             rf"{B}(?:gtx|rtx|gt|rx|hd|r[579]){E} \d{{3,4}}|{B}(?:rtx|gtx){E}|radeon|geforce|quadro|{B}cmp \d|{B}arc [ab]\d|{B}tesla{E}"),
    Category("cpu", "Процессоры", ("процессор", "проц", "cpu"),
             rf"процессор|{B}проц{E}",
             rf"ryzen|{B}core i\d|{B}i[3579]{E}|xeon|athlon|pentium|celeron|threadripper|{B}fx \d{{4}}|{B}e5 \d{{4}}"),
    Category("ram", "Оперативная память", ("оперативк", "оперативн", "озу", "оперативу", "ram"),
             rf"оперативн|оперативк|{B}озу{E}|модул\w* памят|{B}so ?dimm",
             rf"{B}ddr\d{E}"),
    Category("mobo", "Материнские платы", ("материнк", "материнск", "мать", "матплат"),
             rf"материнск|материнк|{B}мат плат|{B}мать{E}|{B}матплат",
             rf"{B}(?:lga ?\d{{3,4}}|am[345]){E}|{B}[abhxzq]\d{{2,3}}m?{E}(?! ?\d)"),
    Category("storage", "Накопители", ("ssd", "hdd", "накопител", "диск", "nvme", "жестк"),
             rf"{B}ssd|{B}hdd|nvme|накопител|жестк\w* диск|{B}m 2{E}",
             None),
    Category("psu", "Блоки питания", ("бп",),
             rf"блок\w* питани|{B}бп{E}",
             rf"{B}(?:[2-9]\d\d|1\d{{3}}) ?w{E}|80 ?plus"),
    Category("cooling", "Охлаждение", ("кулер", "куллер", "охлажден", "сжо", "вентилятор"),
             rf"кулер|куллер|охлажден|{B}сжо{E}|вентилятор|радиатор|термопаст|{B}aio{E}",
             rf"{B}fan{E}|{B}\d{{3}} ?mm{E}"),
    Category("case", "Корпуса", ("корпус",), r"корпус", None),
    Category("pc", "Компьютеры", ("компьютер", "комп", "пк", "системник", "системн", "сборк", "моноблок"),
             rf"системн\w* блок|системник|{B}пк{E}|компьютер|моноблок", None),
]
BY_KEY = {c.key: c for c in CATEGORIES}

# Слова, которые в запросе ничего не уточняют, если раздел уже понятен
STOPWORDS = {"блок", "питания", "игровой", "игровая", "игровую", "купить", "продам", "новый", "новая", "бу", "б", "у", "для"}

# Кириллица и сленг в моделях → как пишут латиницей
ALIASES = [
    (r"\bртх\b", "rtx"), (r"\bгтх\b", "gtx"), (r"\bрх\b", "rx"), (r"\bрадеон\w*", "radeon"),
    (r"\bджифорс\w*", "geforce"), (r"\bрайзен\w*", "ryzen"), (r"\b(?:razen|raizen|rizen|ryzon|ruzen)\b", "ryzen"), (r"\bрузен\w*", "ryzen"), (r"\bинтел\w*", "intel"),
    (r"\bксеон\w*", "xeon"), (r"\bсупер\b", "super"), (r"\bти\b", "ti"),
    (r"\bгб\b", "gb"), (r"\bтб\b", "tb"), (r"\bмгц\b", "mhz"), (r"\bвт\b", "w"),
]


def normalize(text: str) -> str:
    t = text.lower().replace("ё", "е")
    t = re.sub(r"[^\w]+|_", " ", t)
    for pat, repl in ALIASES:
        t = re.sub(pat, repl, t)
    t = re.sub(r"(\d)\s*(?:gb|гб|г)\b", r"\1gb", t)          # 16 гб → 16gb
    t = re.sub(r"(\d)\s*(?:tb|тб)\b", r"\1tb", t)
    t = re.sub(r"\b([a-z]{2,})(\d{3,})", r"\1 \2", t)         # rtx3060 → rtx 3060, rx580 → rx 580
    t = re.sub(r"(\d{3,4})(ti|super)\b", r"\1 \2", t)         # 3060ti → 3060 ti
    t = re.sub(r"\b(\d{4})s\b", r"\1 super", t)              # 1660s → 1660 super
    t = re.sub(r"\b(\d{3,4})xt\b", r"\1 xt", t)              # 5700xt → 5700 xt
    return re.sub(r"\s+", " ", t).strip()


def classify(item: Item) -> set[str]:
    """Разделы объявления. Комплект «мать + процессор» попадает в оба."""
    if PC_FEED in item.feed:
        return {"pc"}
    title = normalize(item.title)
    found = {c.key for c in CATEGORIES if c.noun and re.search(c.noun, title)}
    if found:
        return found
    hints = {c.key for c in CATEGORIES if c.hint and re.search(c.hint, title)}
    if "gpu" in hints:
        hints -= {"ram", "psu"}  # «8gb gddr6», «300w» у видеокарт
    return hints or {"other"}


def category_of(token: str) -> Category | None:
    for cat in CATEGORIES:
        if any(token.startswith(w) for w in cat.query_words):
            return cat
    return None


def split_query(sub: Subscription) -> tuple[Category | None, list[str]]:
    """Раздел запроса (выбран явно или угадан по словам) и оставшиеся слова модели."""
    cat = BY_KEY.get(sub.category)
    text = sub.query.lower()
    if cat is None and re.search(r"блок\w* питани", text):
        cat = BY_KEY["psu"]
    words = []
    for t in normalize(sub.query).split():
        c = category_of(t)
        if c:
            cat = cat or c
        else:
            words.append(t)
    if cat is not None:
        words = [w for w in words if w not in STOPWORDS]
    return cat, words


def _stem(word: str) -> str:
    """Грубое отсечение окончания: «компьютеры» → «компьюте», «видеокарты» → «видеокар»."""
    return word if len(word) <= 4 else word[: max(4, len(word) - 2)]


def word_in(word: str, tokens: list[str]) -> bool:
    if any(ch.isdigit() for ch in word):
        # модель: 5600 ↔ 5600x, 5600g; 12400 ↔ 12400f; но не 13060 и не 56000
        return any(t == word or (t.startswith(word) and t[len(word):].isalpha()) for t in tokens)
    if len(word) <= 3:
        return word in tokens
    stem = _stem(word)
    return any(t.startswith(stem) for t in tokens)


def minus_in(word: str, tokens: list[str]) -> bool:
    word = normalize(word)
    if not word:
        return False
    if " " in word:
        return f" {word} " in f" {' '.join(tokens)} "
    if any(ch.isdigit() for ch in word) or len(word) <= 3:
        return word in tokens
    stem = word if len(word) <= 5 else word[:-1]
    return any(t.startswith(stem) for t in tokens)


def matches(item: Item, sub: Subscription) -> bool:
    title_tokens = normalize(item.title).split()
    if WANTED.search(" ".join(title_tokens)):
        return False
    cat, words = split_query(sub)
    if cat is not None and cat.key not in classify(item):
        return False
    tokens = title_tokens
    if cat is not None and cat.key == "pc" and item.description:
        tokens = title_tokens + normalize(item.description).split()  # конфигурация ПК часто в описании
    if not all(word_in(w, tokens) for w in words):
        return False
    if any(minus_in(m, title_tokens) for m in sub.minus_words):
        return False
    if sub.min_price is not None and (item.price is None or item.price < sub.min_price):
        return False
    if sub.max_price is not None and (item.price is None or item.price > sub.max_price):
        return False
    return True


def is_fresh(item: Item) -> bool:
    return bool(FRESH_DATE.search(item.date))


def parse_add(text: str) -> tuple[str, int | None, int | None, list[str]]:
    """'1660 super | 8000-15000 | -ti -ноутбук' → (запрос, мин, макс, минус-слова).

    Цену и минус-слова можно не указывать; минус-слова можно писать и прямо в запросе.
    """
    query_words: list[str] = []
    minus: list[str] = []
    min_price = max_price = None
    for part in (p.strip() for p in text.split("|")):
        price = re.fullmatch(r"(\d*)\s*-\s*(\d*)", part.replace(" ", ""))
        if price and any(price.groups()):
            min_price = int(price.group(1)) if price.group(1) else None
            max_price = int(price.group(2)) if price.group(2) else None
            continue
        for word in part.split():
            if word.startswith("-") and len(word) > 1:
                minus.append(word[1:])
            else:
                query_words.append(word)
    return " ".join(query_words), min_price, max_price, minus
