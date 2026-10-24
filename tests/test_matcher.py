from avito_bot.db import Subscription
from avito_bot.matcher import Item, is_fresh, matches, parse_add

KOMPL = "tovary_dlya_kompyutera/komplektuyuschie"
PC = "nastolnye_kompyutery"


def item(title, price=10000, date="2 минуты назад", feed=KOMPL):
    return Item(1, title, price, "", date, None, "", "", feed)


def sub(text):
    q, a, b, minus = parse_add(text)
    return Subscription(1, q, a, b, minus)


def test_parse_add():
    assert parse_add("1660 super | 8000-15000 | -ti -ноутбук") == ("1660 super", 8000, 15000, ["ti", "ноутбук"])
    assert parse_add("rtx 3060 -ноутбук | -20000") == ("rtx 3060", None, 20000, ["ноутбук"])
    assert parse_add("ryzen 5600") == ("ryzen 5600", None, None, [])


def test_models():
    s = sub("1660 super")
    assert matches(item("Видеокарта GTX 1660 SUPER 6GB"), s)
    assert matches(item("Palit GeForce GTX1660 Super"), s)
    assert not matches(item("GTX 1660 Ti 6gb"), s)
    assert matches(item("Игровой пк. Core i5 – 6400/ GTX 1660 Super 6 GB", feed=PC), s)


def test_word_endings():
    assert matches(item("Процессоры intel"), sub("процессор"))
    assert matches(item("Новый кулер", feed=KOMPL), sub("кулеры"))


def test_categories():
    gpu = sub("видеокарта")
    assert matches(item("RTX 3090 galax 24G"), gpu)
    assert matches(item("Rx 5700xt sapphire nitro plus"), gpu)
    assert matches(item("Asus GeForce GTX 760 DC Mini 2gb"), gpu)
    assert not matches(item("Блок питания 300W GameMax GT-300 Б/у"), gpu)
    assert not matches(item("Игровой пк RTX 3060 i5 12400f", feed=PC), gpu)  # это компьютер

    cpu = sub("процессор")
    assert matches(item("AMD Ryzen 5 5500"), cpu)
    assert matches(item("Intel core i5 6400"), cpu)
    assert not matches(item("Оперативная память ddr4 16gb"), cpu)

    pc = sub("компьютеры")
    assert matches(item("Игровой пк", feed=PC), pc)
    assert matches(item("Системный блок", feed=PC), pc)
    assert not matches(item("Видеокарта rtx 3050"), pc)

    assert matches(item("Игровой пк RTX 3060 i5 12400f", feed=PC), sub("системный блок 3060"))
    assert matches(item("Видеокарта rtx 3060ti/3070"), sub("видеокарта 3060"))
    assert not matches(item("Игровой пк RTX 3060", feed=PC), sub("видеокарта 3060"))


def test_minus_and_price():
    s = sub("1660 super | 8000-15000 | -ноутбук")
    assert not matches(item("Игровой ноутбук 1660 super"), s)
    assert not matches(item("GTX 1660 Super", price=20000), s)
    assert not matches(item("GTX 1660 Super", price=None), s)
    assert matches(item("GTX 1660 Super", price=12000), s)
    assert not matches(item("Видеокарта нерабочая rx 580"), Subscription(1, "видеокарта", None, None, ["нерабоч"]))
    assert matches(item("Куплена в ДНС rx 580"), Subscription(1, "видеокарта", None, None, ["куплю"]))


def test_fresh():
    for d in ("Только что", "15 секунд назад", "3 минуты назад", "1 час назад", "3 часа назад"):
        assert is_fresh(item("x", date=d)), d
    for d in ("4 часа назад", "Вчера", "3 дня назад", "12 часов назад"):
        assert not is_fresh(item("x", date=d)), d


def test_normalize_models():
    from avito_bot.matcher import normalize
    assert normalize("RTX3060Ti") == "rtx 3060 ti"
    assert normalize("GTX1660S 6Гб") == "gtx 1660 super 6gb"
    assert normalize("ртх 3060 ти") == "rtx 3060 ti"
    assert normalize("Rx5700XT") == "rx 5700 xt"
    assert normalize("Райзен 5 5600X") == "ryzen 5 5600x"
    assert normalize("SSD 1 ТБ") == "ssd 1tb"


def test_model_spelling_variants():
    s = sub("1660 super")
    assert matches(item("Palit GTX1660S"), s)
    assert matches(item("ГТХ 1660 супер"), s)
    s = sub("rtx 3060 ti")
    assert matches(item("RTX3060Ti Palit"), s)
    assert not matches(item("RTX 3060 12gb"), s)


def test_model_numbers_exact():
    assert matches(item("AMD Ryzen 5 5600X"), sub("ryzen 5600"))
    assert matches(item("Intel Core i5-12400F"), sub("i5 12400"))
    assert not matches(item("Видеокарта RTX 3060"), sub("видеокарта 306"))
    assert not matches(item("Ryzen 5 3600"), sub("ryzen 360"))


def test_classify():
    from avito_bot.matcher import classify
    c = lambda t, feed=KOMPL: classify(item(t, feed=feed))
    assert c("RTX 3090 galax 24G") == {"gpu"}
    assert c("Rtx pro 96gb Max-Q Blackwell") == {"gpu"}
    assert c("Блок питания 300W GameMax GT-300 Б/у") == {"psu"}
    assert c("Deepcool PF600 600w") == {"psu"}
    assert c("Материнская плата + процессор +куллер") >= {"cpu", "mobo"}
    assert c("Asus prime z270-p") == {"mobo"}
    assert c("Ddr2 667 1gb") == {"ram"}
    assert c("Жёсткий диск 1тб 2.5 Toshiba") == {"storage"}
    assert c("Сжо Deepcool LE360 WH V2 белая") == {"cooling"}
    assert c("Корпус Zalman") == {"case"}
    assert c("Игровой системный блок") == {"pc"}
    assert c("Что угодно", feed=PC) == {"pc"}
    assert c("Звуковая карта Creative SB1550") == {"other"}


def test_explicit_category():
    gpus = Subscription(1, "", None, 10000, [], True, "gpu")
    assert matches(item("Rx 580 8gb", price=7000), gpus)
    assert not matches(item("Rx 580 8gb", price=12000), gpus)
    assert not matches(item("Процессор Ryzen 5 5500", price=7000), gpus)
    pc = Subscription(2, "3060", None, None, [], True, "pc")
    with_desc = Item(1, "Игровой пк", 50000, "", "1 минуту назад", None, "", "Ryzen 5 5600, RTX 3060 12gb, 16gb", PC)
    assert matches(with_desc, pc)
    assert not matches(item("Видеокарта RTX 3060"), pc)


def test_wanted_ads_skipped():
    assert not matches(item("Куплю видеокарту rtx 3060"), sub("rtx 3060"))
    assert not matches(item("Скупка видеокарт"), sub("видеокарта"))


def test_typos():
    from avito_bot.matcher import classify
    assert classify(item("Razen 7 9700x")) == {"cpu"}
