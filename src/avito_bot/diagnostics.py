"""Read-only startup report. Does not start Chrome, open a proxy or contact any source."""
import argparse
import json
import sqlite3
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from .config import load_config


def _value(value):
    if value is None:
        return 'unknown'
    if isinstance(value, bool):
        return 'yes' if value else 'no'
    return str(value)


def _at(value):
    return datetime.fromtimestamp(value, ZoneInfo('Europe/Moscow')).isoformat() if value else 'unknown'


def startup_report(cfg):
    sample = {}
    path = cfg.data_dir / 'startup-diagnostics.json'
    if path.exists():
        try:
            sample = json.loads(path.read_text())
        except (OSError, ValueError):
            pass
    rows, access, last, success, next_poll = [], [], None, None, None
    path = cfg.data_dir / 'bot.sqlite3'
    if path.exists():
        # Opening the report must not migrate a production DB or modify saved restrictions.
        conn = sqlite3.connect(f'{path.resolve().as_uri()}?mode=ro', uri=True)
        try:
            conn.row_factory = sqlite3.Row
            rows = [dict(r) for r in conn.execute('SELECT * FROM source_state WHERE source_id=?', ('local_browser',))]
            last = conn.execute('SELECT http_status FROM fetch_metrics WHERE source_id=? AND http_status IS NOT NULL '
                                'ORDER BY id DESC LIMIT 1', ('local_browser',)).fetchone()
            success = conn.execute("SELECT MAX(fetched_at) FROM fetch_metrics WHERE source_id=? AND status IN ('ok','no_results')",
                                   ('local_browser',)).fetchone()[0]
            next_poll = conn.execute('SELECT value FROM settings WHERE key=?', ('next_poll:local_browser',)).fetchone()
            if conn.execute("SELECT 1 FROM sqlite_master WHERE name='access_status'").fetchone():
                access = [json.loads(r[0]) for r in conn.execute('SELECT data_json FROM access_status WHERE source_id=?',
                                                               ('local_browser',))]
        except sqlite3.Error:
            pass
        finally:
            conn.close()
    current = sample and time.time() - sample.get('at', 0) < 15
    lines = [f'Browser session: {cfg.browser_session_mode}', f'Browser engine: {"chrome_extension" if cfg.browser_session_mode == "chrome_extension" else cfg.browser_engine}',
             'Chrome connection: ' + (sample.get('connection', 'unknown') if current else 'unknown'),
             'Profile: persistent',
             'Proxy: ' + ('unknown' if cfg.browser_session_mode in ('cdp_attach', 'chrome_extension')
                          else 'enabled' if cfg.direct_interface else 'disabled'),
             'Network route: ' + (sample.get('route', 'unknown') if current else 'unknown'),
             'External IP: unknown',
             'Route mismatch: ' + ('DirectProxy configuration does not control attached Chrome'
                                  if cfg.browser_session_mode in ('cdp_attach', 'chrome_extension') and cfg.direct_interface else 'unknown'),
             'Last HTTP status: ' + _value(last[0] if last else None),
             'Last successful catalog: ' + _at(success),
             'Previous restriction restored: ' + _value(sample.get('previous_restriction_restored')),
             'First navigation: ' + (_at(sample['first_navigation'].get('at'))
                                    if sample.get('first_navigation') else 'unknown'),
             'Requests before navigation: ' + _value(sample.get('requests_before_navigation')),
             'Observed requests before navigation: ' + _value(sample.get('observed_requests_before_navigation')),
             'Requests during navigation: ' + _value(sample.get('requests_during_navigation')),
             'Browser page: ' + (sample.get('browser_page', 'unknown') if current else 'unknown'),
             'Verification required: ' + _value(any(s['verification_required'] for s in access) if access else None),
             'Verification confirmed: ' + _value(any(s['verification_confirmed'] for s in access) if access else None)]
    now = time.time()
    server = max((s.get('server_until') or 0 for s in rows), default=0)
    local = max((s.get('next_allowed_at') or 0 for s in rows if not s.get('server_until')), default=0)
    lines += ['Server Retry-After: ' + (_at(server) if server else 'unknown'),
              'Local recovery delay: ' + (f'{max(0, local - now):.1f} sec' if local else 'unknown')]
    if any(s['state'] in ('VERIFICATION_REQUIRED', 'ACCESS_DENIED') for s in rows):
        next_check = 'unknown (waiting for verified catalog)'
    elif next_poll or server or local:
        next_check = _at(max(now, server, local, float(next_poll[0]) if next_poll else 0))
    else:
        next_check = 'unknown'
    lines.append('Next allowed scheduled check: ' + next_check)
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--startup', action='store_true', required=True)
    parser.parse_args()
    print(startup_report(load_config()))
