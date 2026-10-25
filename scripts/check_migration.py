"""Migrate a backup copy of a live database and compare user data before and after.

uv run python scripts/check_migration.py data/bot.sqlite3 /tmp/bot-copy.sqlite3
The live database is opened read-only; only the copy is migrated.
"""
import sqlite3, sys, json
src, copy = sys.argv[1], sys.argv[2]
with sqlite3.connect(f'file:{src}?mode=ro', uri=True) as live, sqlite3.connect(copy) as dst:
    live.backup(dst)
TABLES = ['subscriptions', 'seen', 'items', 'notification_outbox', 'notifications', 'members',
          'fetch_metrics', 'source_observations', 'studies', 'study_samples']
def snapshot(conn):
    out = {t: conn.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0] for t in TABLES}
    out['subscriptions_rows'] = conn.execute('SELECT id,query,min_price,max_price,minus_words,enabled,category,city FROM subscriptions ORDER BY id').fetchall()
    out['outbox_status'] = dict(conn.execute('SELECT status,COUNT(*) FROM notification_outbox GROUP BY status').fetchall())
    out['source_state'] = conn.execute('SELECT city,state,reason,next_allowed_at,since,http_status,server_until FROM source_state ORDER BY city').fetchall()
    out['paused'] = conn.execute("SELECT value FROM settings WHERE key='paused'").fetchone()
    return out
with sqlite3.connect(copy) as conn:
    before = snapshot(conn)
from avito_bot.db import DB
db = DB(__import__('pathlib').Path(copy)); db.close()
db = DB(__import__('pathlib').Path(copy)); db.close()  # idempotent second open
with sqlite3.connect(copy) as conn:
    after = snapshot(conn)
    integrity = conn.execute('PRAGMA integrity_check').fetchone()[0]
for key in TABLES + ['subscriptions_rows', 'outbox_status']:
    print(f"{key}: {'OK' if before[key] == after[key] else 'CHANGED'} {before[key] if not isinstance(before[key], list) else len(before[key])}")
print('source_state before:', before['source_state'])
print('source_state after: ', after['source_state'])
print('paused before/after:', before['paused'], after['paused'])
print('integrity:', integrity)
