"""Read-only fetch, comparison and delivery reports. Never starts a browser."""
import argparse
import csv
import json
import math
from pathlib import Path
import sqlite3
import sys

from .config import load_config

ENGINES = ('playwright', 'patchright')
MODES = ('light_fetch', 'page_navigation')
RESTRICTED = {'blocked', 'challenge', 'rate_limited'}
SUCCESS = {'ok', 'no_results'}


def distribution(values):
    values = sorted(values)
    if not values:
        return None
    return {'n': len(values), 'p50': values[math.ceil(len(values) * .5) - 1],
            'p95': values[math.ceil(len(values) * .95) - 1]}


def detection_events(conn, since=0, until=1e20):
    """One event per delivery state, with the original detection's attribution."""
    for item_id, encoded, seen, sent, status in conn.execute(
            'SELECT i.item_id,i.payload_json,i.first_seen_at,MIN(o.sent_at),o.status FROM items i '
            'LEFT JOIN notification_outbox o USING(item_id) WHERE i.first_seen_at>=? AND i.first_seen_at<? '
            'GROUP BY i.item_id,o.status ORDER BY i.first_seen_at', (since, until)):
        payload = json.loads(encoded)
        item, collection = payload['item'], payload.get('collection', {})
        yield dict(item_id=item_id, source=payload.get('source_id'), city=item['city'],
                   browser_engine=collection.get('browser_engine'), fetch_mode=collection.get('fetch_mode'),
                   run_id=collection.get('run_id'), comparison_context=collection.get('comparison_context'),
                   operation=collection.get('operation'),
                   published_at=item.get('published'), first_seen_at=seen, delivered_at=sent, status=status)


def comparison_report(conn, since=0, until=1e20):
    cursor = conn.cursor()
    cursor.row_factory = sqlite3.Row
    rows = [dict(row) for row in cursor.execute(
        "SELECT * FROM fetch_metrics WHERE fetched_at>=? AND fetched_at<? AND operation='automatic' ORDER BY id",
        (since, until))]
    # Attribute late Telegram acknowledgements to the detecting mode, regardless of the current mode.
    deliveries = []
    for encoded, seen, sent in conn.execute(
            "SELECT i.payload_json,i.first_seen_at,o.sent_at FROM items i JOIN notification_outbox o USING(item_id) "
            "WHERE o.status='sent' AND i.first_seen_at>=? AND i.first_seen_at<?", (since, until)):
        payload = json.loads(encoded)
        deliveries.append((payload.get('collection', {}), payload['item'].get('published'), seen, sent))
    groups = []
    for engine in ENGINES:
        for mode in MODES:
            samples = [r for r in rows if r['browser_engine'] == engine and r['fetch_mode'] == mode]
            ids = set()
            runs = {}
            for r in samples:
                ids.update(json.loads(r['item_ids_json']))
                key = (r['run_id'], r['comparison_context'], r['budget_rpm'], r['browser_channel'])
                runs.setdefault(key, []).append(r)
            windows = []
            for (run, context, budget, channel), attempts in runs.items():
                first = attempts[0]['fetched_at'] - (attempts[0]['duration_sec'] or 0)
                restricted = next((r for r in attempts if r['status'] in RESTRICTED), None)
                last = attempts[-1]
                windows.append(dict(run_id=run, comparison_context=context, budget_rpm=budget,
                                    browser_channel=channel, started_at=first,
                                    cities=sorted({r['city'] for r in attempts}),
                                    observed_until=last['fetched_at'],
                                    first_restriction_at=restricted['fetched_at'] if restricted else None,
                                    seconds_to_first_restriction=(restricted['fetched_at']-first) if restricted else None,
                                    observed_seconds=max(0, last['fetched_at']-first),
                                    censored=restricted is None, checks=len(attempts),
                                    success_share=sum(r['status'] in SUCCESS for r in attempts)/len(attempts),
                                    restrictions=sum(r['status'] in RESTRICTED for r in attempts),
                                    listing_observations=sum(r['raw_count'] for r in attempts)))
            detection_latency, publication_latency = [], []
            for collection, published, seen, sent in deliveries:
                if (collection.get('browser_engine') == engine and collection.get('fetch_mode') == mode
                        and collection.get('operation', 'automatic') == 'automatic'):
                    detection_latency.append(max(0, sent-seen))
                    if published is not None:
                        publication_latency.append(max(0, sent-published))
            def total(field):
                measured = [r[field] for r in samples if r[field] is not None]
                return {'measured_checks': len(measured), 'total': sum(measured) if measured else None}
            groups.append(dict(browser_engine=engine, fetch_mode=mode, checks=len(samples),
                               success_share=sum(r['status'] in SUCCESS for r in samples)/len(samples) if samples else None,
                               restrictions=sum(r['status'] in RESTRICTED for r in samples),
                               listing_observations=sum(r['raw_count'] for r in samples), unique_listings=len(ids),
                               duration_sec=distribution(r['duration_sec'] for r in samples if r['duration_sec'] is not None),
                               requests=total('request_count'), transferred_bytes=total('transferred_bytes'),
                               detection_to_telegram_sec=distribution(detection_latency),
                               publication_to_telegram_sec=distribution(publication_latency), windows=windows))
    return dict(groups=groups, unattributed_checks=sum(r['browser_engine'] not in ENGINES or r['fetch_mode'] not in MODES
                                                     for r in rows),
                notes=['Manual verification is excluded from fetch comparison.',
                       'Windows are observed fetch spans, not proof of uptime. Windows without a restriction are right-censored.',
                       'Compare matching context, budget, channel, cities and time windows; sequential runs are not randomized.',
                       'Network counters cover page traffic during collection only; completed encoded bytes are approximate.',
                       'Delivery quantiles count recipients; items keep their original detection attribution.'])


def main():
    parser = argparse.ArgumentParser()
    output = parser.add_mutually_exclusive_group()
    output.add_argument('--events', action='store_true', help='Export detection/delivery rows as CSV')
    output.add_argument('--compare', action='store_true', help='Aggregate four sequential engine/mode combinations')
    parser.add_argument('--since', type=float, default=0, help='Detection/fetch start, Unix seconds inclusive')
    parser.add_argument('--until', type=float, default=1e20, help='Detection/fetch end, Unix seconds exclusive')
    parser.add_argument('--db', type=Path, help='SQLite path (read-only)')
    args = parser.parse_args()
    if not math.isfinite(args.since) or not math.isfinite(args.until) or args.until <= args.since:
        parser.error('--since/--until must be finite and increasing')
    path = (args.db or load_config().data_dir/'bot.sqlite3').resolve()
    with sqlite3.connect(path.as_uri()+'?mode=ro', uri=True) as conn:
        columns = {r[1] for r in conn.execute('PRAGMA table_info(fetch_metrics)')}
        if args.compare and 'browser_engine' not in columns:
            parser.error('Database has no browser measurements yet; start the updated application to migrate it')
        if args.events:
            writer = csv.DictWriter(sys.stdout, fieldnames=[
                'item_id', 'source', 'city', 'browser_engine', 'fetch_mode', 'run_id', 'comparison_context', 'operation',
                'published_at', 'first_seen_at', 'delivered_at', 'status'])
            writer.writeheader()
            writer.writerows(detection_events(conn, args.since, args.until))
        elif args.compare:
            print(json.dumps(comparison_report(conn, args.since, args.until), indent=2))
        else:
            conn.row_factory = sqlite3.Row
            # Whitelist fields: no tokens, payloads, cookies, query text or HTML.
            fields = ['source_id', 'city', 'fetched_at', 'status', 'http_status', 'raw_count', 'matched_count',
                      'overlap_count', 'interval_sec', 'cooldown_until', 'browser_engine', 'browser_channel',
                      'fetch_mode', 'actual_mode', 'run_id', 'duration_sec', 'request_count', 'transferred_bytes',
                      'access_limited', 'budget_rpm', 'comparison_context', 'operation']
            select = ','.join(name for name in fields if name in columns)
            print(json.dumps([dict(row) for row in conn.execute(
                f'SELECT {select} FROM fetch_metrics WHERE fetched_at>=? AND fetched_at<? ORDER BY id DESC LIMIT 1000',
                (args.since, args.until))], indent=2))
