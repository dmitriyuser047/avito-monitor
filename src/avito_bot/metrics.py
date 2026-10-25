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
                   operation=collection.get('operation'), actual_mode=collection.get('actual_mode'),
                   is_warmup=collection.get('is_warmup'),
                   published_at=item.get('published'), first_seen_at=seen, delivered_at=sent, status=status)


def fetch_summary(samples):
    """Unknown measurements never become zero-valued observations."""
    ids = set()
    for row in samples:
        ids.update(json.loads(row['item_ids_json']))
    def total(field):
        measured = [r[field] for r in samples if valid_measurement(r.get(field))]
        return {'measured_checks': len(measured), 'total': sum(measured) if measured else None}
    return dict(checks=len(samples),
                success_share=sum(r['status'] in SUCCESS for r in samples)/len(samples) if samples else None,
                restrictions=sum(r['status'] in RESTRICTED for r in samples),
                listing_observations=sum(r['raw_count'] for r in samples), unique_listings=len(ids),
                duration_sec=distribution(r['duration_sec'] for r in samples if valid_measurement(r.get('duration_sec'))),
                requests=total('request_count'), transferred_bytes=total('transferred_bytes'))


def valid_measurement(value):
    return isinstance(value, (int, float)) and math.isfinite(value) and value >= 0


def phase_summary(samples):
    regular = [r for r in samples if r.get('is_warmup') == 0]
    return dict(configuration_totals=fetch_summary(samples),
                warmup=fetch_summary([r for r in samples if r.get('is_warmup') == 1]),
                regular_by_actual_mode={mode: fetch_summary([r for r in regular if r.get('actual_mode') == mode])
                                        for mode in MODES},
                unknown_phase=fetch_summary([r for r in samples if r.get('is_warmup') is None]),
                unknown_actual_mode_checks=sum(r.get('actual_mode') not in MODES for r in regular))


def observation_windows(history, selected_ids):
    # Keep contiguous contexts separate (A -> B -> A is three observations).
    segments, current = [], {}
    for row in history:
        key = tuple(row.get(name) for name in ('source_id', 'browser_engine', 'fetch_mode', 'run_id',
                                              'comparison_context', 'budget_rpm', 'browser_channel'))
        previous = current.get(row['source_id'])
        if previous is None or previous[0] != key:
            attempts = []
            segments.append(attempts)
            current[row['source_id']] = key, attempts
        else:
            attempts = previous[1]
        attempts.append(row)
    windows = []
    for attempts in segments:
        selected = [r for r in attempts if r['id'] in selected_ids]
        if not selected:
            continue
        first, last = attempts[0], attempts[-1]
        restricted = next((r for r in attempts if r['status'] in RESTRICTED), None)
        # Do not rebase time-to-restriction onto --since or substitute missing duration with zero.
        start = (first['fetched_at']-first['duration_sec']
                 if first.get('run_id') and valid_measurement(first.get('duration_sec')) else None)
        elapsed = restricted['fetched_at']-start if restricted and start is not None else None
        windows.append(dict(
            browser_engine=first.get('browser_engine'), fetch_mode=first.get('fetch_mode'),
            run_id=first.get('run_id'), comparison_context=first.get('comparison_context'),
            budget_rpm=first.get('budget_rpm'), browser_channel=first.get('browser_channel'),
            cities=sorted({r['city'] for r in selected}),
            started_at=start, first_observed_at=first['fetched_at'], observed_until=last['fetched_at'],
            first_restriction_at=restricted['fetched_at'] if restricted else None,
            seconds_to_first_restriction=elapsed if valid_measurement(elapsed) else None,
            censored=restricted is None, origin='first_observed_check_in_context',
            selected_checks=fetch_summary(selected)))
    return windows


def delivery_summary(deliveries, engine, fetch_mode=None, actual_mode=None, regular_only=False):
    detection, publication = [], []
    invalid = 0
    for collection, published, seen, sent in deliveries:
        if collection.get('browser_engine') != engine or collection.get('operation', 'automatic') != 'automatic':
            continue
        if fetch_mode is not None and collection.get('fetch_mode') != fetch_mode:
            continue
        if actual_mode is not None and collection.get('actual_mode') != actual_mode:
            continue
        if regular_only and collection.get('is_warmup') != 0:
            continue
        if not valid_measurement(sent) or not valid_measurement(seen) or not valid_measurement(sent-seen):
            invalid += 1
            continue
        detection.append(sent-seen)
        if valid_measurement(published) and valid_measurement(sent-published):
            publication.append(sent-published)
    return dict(detection_to_telegram_sec=distribution(detection), publication_to_telegram_sec=distribution(publication),
                invalid_delivery_timestamps=invalid)


def comparison_report(conn, since=0, until=1e20):
    cursor = conn.cursor()
    cursor.row_factory = sqlite3.Row
    # Reading earlier history preserves run/context origins when the export starts mid-run.
    history = [dict(row) for row in cursor.execute(
        'SELECT * FROM fetch_metrics WHERE fetched_at<? ORDER BY id', (until,))]
    rows = [r for r in history if r['fetched_at'] >= since]
    automatic = [r for r in rows if r['operation'] == 'automatic']
    windows = observation_windows([r for r in history if r['operation'] == 'automatic'],
                                  {r['id'] for r in automatic})
    deliveries = []
    for encoded, seen, sent in conn.execute(
            "SELECT i.payload_json,i.first_seen_at,o.sent_at FROM items i JOIN notification_outbox o USING(item_id) "
            "WHERE o.status='sent' AND i.first_seen_at>=? AND i.first_seen_at<?", (since, until)):
        payload = json.loads(encoded)
        deliveries.append((payload.get('collection', {}), payload['item'].get('published'), seen, sent))
    groups = []
    for engine in ENGINES:
        for mode in MODES:
            samples = [r for r in automatic if r['browser_engine'] == engine and r['fetch_mode'] == mode]
            groups.append(dict(browser_engine=engine, fetch_mode=mode, **phase_summary(samples),
                               **delivery_summary(deliveries, engine, fetch_mode=mode),
                               windows=[w for w in windows if w['browser_engine']==engine and w['fetch_mode']==mode]))
    mechanisms = []
    for engine in ENGINES:
        for mode in MODES:
            regular = [r for r in automatic if r['browser_engine']==engine and r.get('actual_mode')==mode
                       and r.get('is_warmup')==0]
            cohorts = {}
            for row in regular:
                key = (row.get('comparison_context'), row.get('budget_rpm'), row.get('browser_channel'), row['city'])
                cohorts.setdefault(key, []).append(row)
            mechanisms.append(dict(browser_engine=engine, actual_mode=mode, totals=fetch_summary(regular),
                **delivery_summary(deliveries, engine, actual_mode=mode, regular_only=True),
                cohort_count=len(cohorts), mixed_contexts=len(cohorts)>1,
                cohorts=[dict(comparison_context=context, budget_rpm=budget, browser_channel=channel, city=city,
                              context_complete=all(v is not None for v in (context, budget, channel)),
                              configured_modes=sorted({r['fetch_mode'] for r in checks}), **fetch_summary(checks))
                         for (context, budget, channel, city), checks in cohorts.items()]))
    return dict(schema_version=2, groups=groups, mechanisms=mechanisms,
                total_load=fetch_summary(rows), automatic_load=fetch_summary(automatic),
                manual_load=fetch_summary([r for r in rows if r['operation']=='manual']),
                unattributed_checks=sum(r['browser_engine'] not in ENGINES or r['fetch_mode'] not in MODES for r in automatic),
                notes=['Configuration totals include warm-up and unknown historical phases; mechanisms exclude both.',
                       'Manual verification is separate; total_load includes every recorded operation.',
                       'Match cohort context, budget, channel, city and actual network before comparing mechanisms.',
                       'Observation windows use earlier history, not --since as a new run origin; they do not prove uptime.',
                       'Windows with no observed restriction are right-censored; small samples do not prove stability.',
                       'Network counters cover collection-time page traffic; encoded bytes are approximate.',
                       'Delivery quantiles count recipients and retain original detection attribution.'])


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
                'item_id', 'source', 'city', 'browser_engine', 'fetch_mode', 'run_id', 'comparison_context', 'operation', 'actual_mode', 'is_warmup',
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
                      'access_limited', 'budget_rpm', 'comparison_context', 'operation', 'is_warmup', 'restriction_reason']
            select = ','.join(name for name in fields if name in columns)
            print(json.dumps([dict(row) for row in conn.execute(
                f'SELECT {select} FROM fetch_metrics WHERE fetched_at>=? AND fetched_at<? ORDER BY id DESC LIMIT 1000',
                (args.since, args.until))], indent=2))
