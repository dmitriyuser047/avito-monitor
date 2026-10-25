"""SQLite: подписки, уже виденные объявления, отправленные уведомления, настройки."""

import json
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


# Additive migration: historical measurements stay unknown, never guessed.
FETCH_DIAGNOSTIC_COLUMNS = {
    'browser_engine': 'TEXT', 'browser_channel': 'TEXT', 'fetch_mode': 'TEXT',
    'actual_mode': 'TEXT', 'run_id': 'TEXT', 'duration_sec': 'REAL',
    'request_count': 'INTEGER', 'transferred_bytes': 'INTEGER',
    'access_limited': 'INTEGER', 'budget_rpm': 'REAL', 'comparison_context': 'TEXT',
    'operation': "TEXT NOT NULL DEFAULT 'automatic'",
    'is_warmup': 'INTEGER', 'restriction_reason': 'TEXT',
}
# Origin of a restriction: when it was first set, the HTTP answer and the server-given term.
SOURCE_STATE_COLUMNS = {'since': 'REAL', 'http_status': 'INTEGER', 'server_until': 'REAL'}


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
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.execute("PRAGMA foreign_keys=ON")
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
            CREATE TABLE IF NOT EXISTS items (
                item_id INTEGER PRIMARY KEY,
                payload_json TEXT NOT NULL,
                first_seen_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS notification_outbox (
                id INTEGER PRIMARY KEY,
                item_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL,
                lease_until REAL,
                sent_at REAL,
                last_error TEXT,
                UNIQUE(item_id, chat_id)
            );
            CREATE INDEX IF NOT EXISTS idx_outbox_pending
                ON notification_outbox(status, next_attempt_at);
            CREATE INDEX IF NOT EXISTS idx_notifications_item_sub ON notifications(item_id, sub_id);
            CREATE TABLE IF NOT EXISTS source_state (
                source_id TEXT NOT NULL,
                city TEXT NOT NULL,
                state TEXT NOT NULL,
                next_allowed_at REAL,
                reason TEXT NOT NULL DEFAULT '',
                updated_at REAL NOT NULL,
                PRIMARY KEY(source_id, city)
            );
            CREATE TABLE IF NOT EXISTS fetch_metrics (
                id INTEGER PRIMARY KEY,
                source_id TEXT NOT NULL,
                city TEXT NOT NULL,
                fetched_at REAL NOT NULL,
                status TEXT NOT NULL,
                http_status INTEGER,
                raw_count INTEGER NOT NULL,
                matched_count INTEGER NOT NULL,
                overlap_count INTEGER,
                interval_sec REAL,
                cooldown_until REAL,
                item_ids_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS source_observations (
                source_id TEXT NOT NULL,
                city TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                published_at REAL,
                first_seen_at REAL NOT NULL,
                last_seen_at REAL NOT NULL,
                matched INTEGER NOT NULL,
                baseline INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(source_id,city,item_id)
            );
            CREATE TABLE IF NOT EXISTS studies (
                id INTEGER PRIMARY KEY,
                source_id TEXT NOT NULL,
                started_at REAL NOT NULL,
                ends_at REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'running',
                filters_json TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_one_running_study
                ON studies(source_id) WHERE status='running';
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY,
                at REAL NOT NULL,
                kind TEXT NOT NULL,
                correlation_id TEXT,
                source_id TEXT,
                city TEXT,
                result TEXT,
                details_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_events_kind_at ON events(kind, at);
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY,
                item_id INTEGER NOT NULL,
                price INTEGER NOT NULL,
                seen_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_price_item ON price_history(item_id, id);
            CREATE TABLE IF NOT EXISTS study_samples (
                study_id INTEGER NOT NULL,
                sampled_at REAL NOT NULL,
                state TEXT NOT NULL,
                queue_json TEXT NOT NULL,
                PRIMARY KEY(study_id,sampled_at)
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
        metrics_columns = {r[1] for r in self.conn.execute('PRAGMA table_info(fetch_metrics)')}
        for name, declaration in FETCH_DIAGNOSTIC_COLUMNS.items():
            if name not in metrics_columns:
                self.conn.execute(f'ALTER TABLE fetch_metrics ADD COLUMN {name} {declaration}')
        state_columns = {r[1] for r in self.conn.execute('PRAGMA table_info(source_state)')}
        for name, declaration in SOURCE_STATE_COLUMNS.items():
            if name not in state_columns:
                self.conn.execute(f'ALTER TABLE source_state ADD COLUMN {name} {declaration}')
        # 2.0 state model: terms, codes and set times are kept; the user's pause and the
        # "wait for /resume" state are not carried over.
        from .sources import legacy_state  # sources imports matcher, which imports this module
        for source_id, city, state, reason in list(self.conn.execute(
                'SELECT source_id,city,state,reason FROM source_state')):
            mapped = legacy_state(state, reason)
            if mapped is None:
                self.conn.execute('DELETE FROM source_state WHERE source_id=? AND city=?', (source_id, city))
            elif mapped != state:
                self.conn.execute('UPDATE source_state SET state=? WHERE source_id=? AND city=?',
                                  (mapped, source_id, city))
        self.conn.execute("DELETE FROM settings WHERE key='paused'")
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
            "SELECT COUNT(DISTINCT item_id) FROM notifications WHERE sent_at >= ? AND item_id > 0", (midnight,)
        ).fetchone()[0]

    def found_for(self, sub_id: int) -> int:
        return self.conn.execute("SELECT COUNT(DISTINCT item_id) FROM notifications WHERE sub_id = ? AND item_id > 0",
                                 (sub_id,)).fetchone()[0]

    # Событие и задания каждому получателю фиксируются одной транзакцией.
    def stage_items(self, entries: list[tuple[int, dict, list[int]]], now: float) -> list[int]:
        inserted = []
        with self.conn:
            for item_id, payload, chat_ids in entries:
                cur = self.conn.execute("INSERT OR IGNORE INTO seen VALUES (?, ?)", (item_id, now))
                if not cur.rowcount:
                    continue
                encoded = json.dumps(payload, ensure_ascii=False)
                self.conn.execute("INSERT OR REPLACE INTO items VALUES (?, ?, ?)", (item_id, encoded, now))
                for chat_id in set(chat_ids):
                    self.conn.execute(
                        "INSERT OR IGNORE INTO notification_outbox"
                        " (item_id, chat_id, payload_json, next_attempt_at) VALUES (?, ?, ?, ?)",
                        (item_id, chat_id, encoded, now))
                inserted.append(item_id)
        return inserted

    # --- история цен ---

    def record_price(self, item_id: int, price: int, at: float) -> tuple[int, int | None] | None:
        """Store a price only when it changes: (history id, previous price) or None when unchanged."""
        last = self.conn.execute('SELECT price FROM price_history WHERE item_id=? ORDER BY id DESC LIMIT 1',
                                 (item_id,)).fetchone()
        if last and last[0] == price:
            return None
        with self.conn:
            cur = self.conn.execute('INSERT INTO price_history(item_id,price,seen_at) VALUES (?,?,?)',
                                    (item_id, price, at))
        return cur.lastrowid, last[0] if last else None

    def price_history(self, item_id: int, limit: int = 10) -> list[tuple[int, float]]:
        return list(self.conn.execute('SELECT price,seen_at FROM price_history WHERE item_id=? ORDER BY id DESC LIMIT ?',
                                      (item_id, limit)))

    def stage_price_drop(self, history_id: int, payload: dict, chat_ids: list[int], now: float) -> int:
        """One durable job per recipient and price change; the negative id never collides with a listing."""
        encoded = json.dumps(payload, ensure_ascii=False)
        with self.conn:
            return sum(self.conn.execute(
                'INSERT OR IGNORE INTO notification_outbox (item_id, chat_id, payload_json, next_attempt_at)'
                ' VALUES (?, ?, ?, ?)', (-history_id, chat_id, encoded, now)).rowcount for chat_id in set(chat_ids))

    def recent_items(self, limit: int = 10) -> list[tuple[int, dict, float]]:
        """Latest listings that matched a subscription (unmatched observations are skipped)."""
        found = []
        for item_id, payload, seen in self.conn.execute(
                'SELECT item_id,payload_json,first_seen_at FROM items ORDER BY first_seen_at DESC LIMIT ?',
                (limit * 20,)):
            data = json.loads(payload)
            if data.get('subs'):
                found.append((item_id, data, seen))
                if len(found) == limit:
                    break
        return found

    def recent_events(self, kinds: tuple[str, ...], limit: int = 15) -> list[dict]:
        marks = ','.join('?' * len(kinds))
        cur = self.conn.execute(f'SELECT at,kind,city,result,details_json FROM events WHERE kind IN ({marks})'
                                ' ORDER BY id DESC LIMIT ?', (*kinds, limit))
        return [dict(at=at, kind=kind, city=city, result=result, details=json.loads(details))
                for at, kind, city, result, details in cur]

    def claim_delivery(self, now: float, lease_seconds: float = 120) -> dict | None:
        with self.conn:
            self.conn.execute("UPDATE notification_outbox SET status='retry', lease_until=NULL"
                              " WHERE status='leased' AND lease_until <= ?", (now,))
            row = self.conn.execute(
                "UPDATE notification_outbox SET status='leased', attempts=attempts+1, lease_until=?"
                " WHERE id=(SELECT id FROM notification_outbox WHERE status IN ('pending','retry')"
                " AND next_attempt_at <= ? ORDER BY next_attempt_at,id LIMIT 1) RETURNING *",
                (now + lease_seconds, now))
            names = [c[0] for c in row.description]
            values = row.fetchone()
        return dict(zip(names, values)) if values else None

    def ack_delivery(self, job_id: int, now: float) -> None:
        with self.conn:
            row = self.conn.execute("SELECT item_id,payload_json FROM notification_outbox"
                                    " WHERE id=? AND status='leased'", (job_id,)).fetchone()
            if not row:
                return
            self.conn.execute("UPDATE notification_outbox SET status='sent',sent_at=?,lease_until=NULL,"
                              "last_error=NULL WHERE id=?", (now, job_id))
            for sub in json.loads(row[1])["subs"]:
                self.conn.execute("INSERT INTO notifications SELECT ?,?,? WHERE NOT EXISTS"
                                  " (SELECT 1 FROM notifications WHERE item_id=? AND sub_id=?)",
                                  (row[0], sub["id"], now, row[0], sub["id"]))

    def fail_delivery(self, job_id: int, error: str, next_at: float, terminal: bool = False) -> None:
        with self.conn:
            self.conn.execute("UPDATE notification_outbox SET status=?,last_error=?,next_attempt_at=?,"
                              "lease_until=NULL WHERE id=? AND status='leased'",
                              ('terminal' if terminal else 'retry', error[:300], next_at, job_id))

    def queue_counts(self) -> dict[str, int]:
        return dict(self.conn.execute("SELECT status,COUNT(*) FROM notification_outbox GROUP BY status"))

    def source_states(self, source_id: str) -> list[dict]:
        cur = self.conn.execute("SELECT city,state,next_allowed_at,reason,updated_at,"
                                "COALESCE(since,updated_at) AS since,http_status,server_until FROM source_state"
                                " WHERE source_id=? ORDER BY city", (source_id,))
        names = [c[0] for c in cur.description]
        return [dict(zip(names, row)) for row in cur]

    def save_source_state(self, source_id: str, city: str, state: str,
                          next_allowed_at: float | None = None, reason: str = '',
                          http_status: int | None = None, server_until: float | None = None,
                          since: float | None = None) -> bool:
        old = self.conn.execute("SELECT state,COALESCE(since,updated_at) FROM source_state"
                                " WHERE source_id=? AND city=?", (source_id, city)).fetchone()
        now = time.time()
        # Repeating the same state keeps the moment the restriction was first set.
        if since is None:
            since = old[1] if old and old[0] == state else now
        with self.conn:
            self.conn.execute("INSERT OR REPLACE INTO source_state (source_id,city,state,next_allowed_at,reason,"
                              "updated_at,since,http_status,server_until) VALUES (?,?,?,?,?,?,?,?,?)",
                              (source_id, city, state, next_allowed_at, reason, now, since,
                               http_status, server_until))
        return not old or old[0] != state

    def record_event(self, kind: str, *, correlation_id: str | None = None, source_id: str | None = None,
                     city: str | None = None, result: str | None = None, at: float | None = None,
                     **details) -> None:
        """Diagnostic event; callers pass only safe fields (no cookies, tokens, chats or page text)."""
        with self.conn:
            self.conn.execute('INSERT INTO events(at,kind,correlation_id,source_id,city,result,details_json)'
                              ' VALUES (?,?,?,?,?,?,?)',
                              (time.time() if at is None else at, kind, correlation_id, source_id, city, result,
                               json.dumps(details, ensure_ascii=False, sort_keys=True)))

    def prune_events(self, older_than_days: int = 30) -> None:
        with self.conn:
            self.conn.execute('DELETE FROM events WHERE at < ?', (time.time() - older_than_days * 86400,))

    def latest_snapshot(self, source_id: str, city: str) -> tuple[set[int], float] | None:
        row = self.conn.execute("SELECT item_ids_json,fetched_at FROM fetch_metrics"
                                " WHERE source_id=? AND city=? AND status IN ('ok','no_results')"
                                " ORDER BY id DESC LIMIT 1", (source_id, city)).fetchone()
        return (set(json.loads(row[0])), row[1]) if row else None

    def record_fetch(self, **values) -> None:
        for name in FETCH_DIAGNOSTIC_COLUMNS:
            values.setdefault(name, 'automatic' if name == 'operation' else None)
        extra = ','.join(FETCH_DIAGNOSTIC_COLUMNS)
        params = ','.join(':' + name for name in FETCH_DIAGNOSTIC_COLUMNS)
        with self.conn:
            self.conn.execute("INSERT INTO fetch_metrics (source_id,city,fetched_at,status,http_status,"
                              f"raw_count,matched_count,overlap_count,interval_sec,cooldown_until,item_ids_json,{extra})"
                              " VALUES (:source_id,:city,:fetched_at,:status,:http_status,:raw_count,"
                              f":matched_count,:overlap_count,:interval_sec,:cooldown_until,:item_ids_json,{params})", values)

    def latency_stats(self) -> dict:
        rows = self.conn.execute("SELECT o.sent_at,i.first_seen_at,i.payload_json FROM notification_outbox o"
                                 " JOIN items i USING(item_id) WHERE o.status='sent' ORDER BY o.id DESC LIMIT 5000")
        delivery, publish = [], []
        for sent, seen, payload in rows:
            delivery.append(max(0, sent - seen))
            pub = json.loads(payload)['item'].get('published')
            if pub:
                publish.append(max(0, sent - pub))
        def summary(samples):
            samples.sort()
            if not samples:
                return None
            import math
            return {'n': len(samples), 'p50': samples[math.ceil(len(samples)*.5)-1],
                    'p95': samples[math.ceil(len(samples)*.95)-1],
                    'within_60': sum(s <= 60 for s in samples) / len(samples)}
        return {'delivery': summary(delivery), 'publish': summary(publish)}

    def close(self) -> None:
        self.conn.close()

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
