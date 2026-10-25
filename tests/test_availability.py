"""Stage 4: the bot running and the source being available are measured separately."""
import json
import subprocess
import sys
import time

from avito_bot.db import DB
from avito_bot.metrics import availability_report, service_uptime


def test_uptime_counts_clean_stops_and_crashes_by_last_event(tmp_path):
    db = DB(tmp_path/'x.sqlite3')
    for at, kind in [(0, 'service_started'), (100, 'source_checked'), (200, 'service_stopped'),
                     (300, 'service_started'), (350, 'source_checked'),  # crash: no stop event
                     (500, 'service_started')]:
        db.record_event(kind, at=at)
    uptime = service_uptime(db.conn, 0, 1000)
    assert uptime['seconds'] == 200 + 50 + 500 and uptime['starts'] == 3 and uptime['share'] == .75
    db.close()


def test_report_separates_source_restrictions_and_delivery(tmp_path):
    db = DB(tmp_path/'x.sqlite3')
    db.record_event('service_started', at=0)
    for at, result, code in [(10, 'ok', 200), (20, 'rate_limited', 429), (30, 'blocked', 403), (40, 'ok', 200)]:
        db.record_event('source_checked', at=at, result=result, http_status=code, source_id='s')
    db.record_event('verification_required', at=30)
    db.record_event('source_recovered', at=40, city='kazan', recovery_sec=10)
    db.record_event('listing_discovered', at=40, result='7')
    db.record_event('listing_discovered', at=41, result='7')
    db.record_fetch(source_id='s', city='kazan', fetched_at=40, status='ok', http_status=200, raw_count=1,
                    matched_count=0, overlap_count=None, interval_sec=None, cooldown_until=None,
                    item_ids_json='[7]', request_count=12, transferred_bytes=3000)
    report = availability_report(db.conn, 0, 100)
    assert report['service']['share'] == 1
    assert report['source']['checks'] == 4 and report['source']['success_share'] == .5
    assert report['source']['http_429'] == 1 and report['source']['http_403'] == 1
    assert report['source']['verification_required'] == 1 and report['source']['recovery_sec']['p50'] == 10
    assert report['listings']['unique_discovered'] == 1
    assert report['traffic'] == dict(measured_checks=1, requests=12, measured_byte_checks=1, transferred_bytes=3000)
    db.close()


def test_availability_cli_is_read_only_json(tmp_path):
    db = DB(tmp_path/'x.sqlite3')
    db.record_event('service_started', at=time.time() - 60)
    db.close()
    out = subprocess.run([sys.executable, '-m', 'avito_bot.metrics', '--availability', '--db', str(tmp_path/'x.sqlite3')],
                         capture_output=True, text=True, check=True).stdout
    report = json.loads(out)
    assert report['service']['starts'] == 1 and report['source']['checks'] == 0
