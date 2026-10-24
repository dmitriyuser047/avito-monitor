"""SQLite: подписки, уже виденные объявления, отправленные уведомления, настройки."""

import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass
class Subscription:
    id: int
    query: str
    min_price: int | None
    max_price: int | None
    minus_words: list[str]
    enabled: bool = True
    category: str = ""  # ключ раздела из matcher.CATEGORIES; пусто — угадать по словам запроса
    cities: list[str] = field(default_factory=list)  # как в адресе Авито (kazan); пусто — город из настроек


class DB:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query TEXT NOT NULL,
                min_price INTEGER,
                max_price INTEGER,
                minus_words TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS seen (
                item_id INTEGER PRIMARY KEY,
                first_seen REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS members (
                user_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                added_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS invites (
                code TEXT PRIMARY KEY,
                created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS notifications (
                item_id INTEGER NOT NULL,
                sub_id INTEGER NOT NULL,
                sent_at REAL NOT NULL
            );
            """
        )
        columns = {r[1] for r in self.conn.execute("PRAGMA table_info(subscriptions)")}
        if "enabled" not in columns:
            self.conn.execute("ALTER TABLE subscriptions ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1")
        if "category" not in columns:
            self.conn.execute("ALTER TABLE subscriptions ADD COLUMN category TEXT NOT NULL DEFAULT ''")
        if "city" not in columns:
            self.conn.execute("ALTER TABLE subscriptions ADD COLUMN city TEXT NOT NULL DEFAULT ''")
        self.conn.commit()

    # --- подписки ---

    def add_subscription(self, query: str, min_price: int | None, max_price: int | None,
                         minus_words: list[str], category: str = "", cities: list[str] | None = None) -> int:
        cur = self.conn.execute(
            "INSERT INTO subscriptions (query, min_price, max_price, minus_words, category, city)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (query, min_price, max_price, " ".join(minus_words), category, " ".join(cities or [])),
        )
        self.conn.commit()
        return cur.lastrowid

    def update_subscription(self, sub_id: int, **fields) -> None:
        if "minus_words" in fields:
            fields["minus_words"] = " ".join(fields["minus_words"])
        if "enabled" in fields:
            fields["enabled"] = int(fields["enabled"])
        if "cities" in fields:
            fields["city"] = " ".join(fields.pop("cities"))  # колонка city: города через пробел
        sets = ", ".join(f"{k} = ?" for k in fields)
        self.conn.execute(f"UPDATE subscriptions SET {sets} WHERE id = ?", (*fields.values(), sub_id))
        self.conn.commit()

    def delete_subscription(self, sub_id: int) -> bool:
        cur = self.conn.execute("DELETE FROM subscriptions WHERE id = ?", (sub_id,))
        self.conn.commit()
        return cur.rowcount > 0

    def subscriptions(self, only_enabled: bool = False) -> list[Subscription]:
        sql = "SELECT id, query, min_price, max_price, minus_words, enabled, category, city FROM subscriptions"
        if only_enabled:
            sql += " WHERE enabled = 1"
        return [_sub(r) for r in self.conn.execute(sql + " ORDER BY id")]

    def subscription(self, sub_id: int) -> Subscription | None:
        row = self.conn.execute(
            "SELECT id, query, min_price, max_price, minus_words, enabled, category, city FROM subscriptions WHERE id = ?",
            (sub_id,),
        ).fetchone()
        return _sub(row) if row else None

    # --- виденные объявления ---

    def unseen(self, item_ids: list[int]) -> set[int]:
        if not item_ids:
            return set()
        marks = ",".join("?" * len(item_ids))
        seen = {r[0] for r in self.conn.execute(f"SELECT item_id FROM seen WHERE item_id IN ({marks})", item_ids)}
        return set(item_ids) - seen

    def mark_seen(self, item_ids: list[int]) -> None:
        now = time.time()
        self.conn.executemany("INSERT OR IGNORE INTO seen VALUES (?, ?)", [(i, now) for i in item_ids])
        self.conn.commit()

    def seen_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM seen").fetchone()[0]

    def prune_seen(self, older_than_days: int = 30) -> None:
        self.conn.execute("DELETE FROM seen WHERE first_seen < ?", (time.time() - older_than_days * 86400,))
        self.conn.commit()

    # --- статистика уведомлений ---

    def record_notification(self, item_id: int, sub_ids: list[int]) -> None:
        now = time.time()
        self.conn.executemany("INSERT INTO notifications VALUES (?, ?, ?)", [(item_id, s, now) for s in sub_ids])
        self.conn.commit()

    def found_today(self) -> int:
        midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        return self.conn.execute(
            "SELECT COUNT(DISTINCT item_id) FROM notifications WHERE sent_at >= ?", (midnight,)
        ).fetchone()[0]

    def found_for(self, sub_id: int) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM notifications WHERE sub_id = ?", (sub_id,)).fetchone()[0]

    # --- участники и приглашения ---

    def members(self) -> list[tuple[int, str]]:
        return list(self.conn.execute("SELECT user_id, name FROM members ORDER BY added_at"))

    def add_member(self, user_id: int, name: str) -> None:
        self.conn.execute("INSERT OR REPLACE INTO members VALUES (?, ?, ?)", (user_id, name, time.time()))
        self.conn.commit()

    def remove_member(self, user_id: int) -> None:
        self.conn.execute("DELETE FROM members WHERE user_id = ?", (user_id,))
        self.conn.commit()

    def create_invite(self, code: str) -> None:
        self.conn.execute("INSERT INTO invites VALUES (?, ?)", (code, time.time()))
        self.conn.commit()

    def use_invite(self, code: str, ttl_hours: int = 24) -> bool:
        """Одноразовое приглашение: True, если код действителен (и он тут же гасится)."""
        cur = self.conn.execute("DELETE FROM invites WHERE code = ? AND created_at > ?",
                                (code, time.time() - ttl_hours * 3600))
        self.conn.execute("DELETE FROM invites WHERE created_at <= ?", (time.time() - ttl_hours * 3600,))
        self.conn.commit()
        return cur.rowcount > 0

    # --- настройки ---

    def get(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def set(self, key: str, value: str) -> None:
        self.conn.execute("INSERT OR REPLACE INTO settings VALUES (?, ?)", (key, value))
        self.conn.commit()


def _sub(r) -> Subscription:
    return Subscription(r[0], r[1], r[2], r[3], r[4].split(), bool(r[5]), r[6], r[7].split())
