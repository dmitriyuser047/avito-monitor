"""Read-only export for the source comparison experiment and delivery latency report."""
import argparse
import csv
import json
import sqlite3
import sys

from .config import load_config


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--events',action='store_true',help='Export detection/delivery rows as CSV')
    args=parser.parse_args()
    path=load_config().data_dir/'bot.sqlite3'
    with sqlite3.connect(f'file:{path}?mode=ro',uri=True) as conn:
        if args.events:
            writer=csv.writer(sys.stdout)
            writer.writerow(['item_id','source','city','published_at','first_seen_at','delivered_at','status'])
            for item_id,payload,seen,sent,status in conn.execute(
                'SELECT i.item_id,i.payload_json,i.first_seen_at,MIN(o.sent_at),o.status FROM items i '
                'LEFT JOIN notification_outbox o USING(item_id) GROUP BY i.item_id,o.status ORDER BY i.first_seen_at'):
                payload=json.loads(payload); item=payload['item']
                writer.writerow([item_id,payload.get('source_id'),item['city'],item.get('published'),seen,sent,status])
        else:
            conn.row_factory=sqlite3.Row
            print(json.dumps([dict(row) for row in conn.execute(
                'SELECT source_id,city,fetched_at,status,http_status,raw_count,matched_count,overlap_count,'
                'interval_sec,cooldown_until FROM fetch_metrics ORDER BY id DESC LIMIT 1000')],indent=2))
