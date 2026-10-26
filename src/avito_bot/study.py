"""Passive local study: bounded time window, no additional network requests."""
import argparse
from collections import Counter
from dataclasses import asdict
from datetime import datetime
import json
import math
from pathlib import Path
import sqlite3
import time

from .config import load_config
from .db import DB

SAMPLE_INTERVAL = 60
MAX_SAMPLE_COVERAGE = 120


def filters_for(cfg, db):
    return {'city': cfg.city, 'sections': cfg.sections,
            'subscriptions': [asdict(s) for s in db.subscriptions(only_enabled=True)]}


def begin_study(db: DB, cfg, hours: float = 24, now: float | None = None) -> int:
    if not math.isfinite(hours) or hours <= 0:
        raise ValueError('Study duration must be positive')
    now = time.time() if now is None else now
    if db.conn.execute("SELECT 1 FROM studies WHERE source_id=? AND status='running'", (cfg.source_id,)).fetchone():
        raise ValueError('A local study is already running; use avito-study report')
    if not db.subscriptions(only_enabled=True):
        raise ValueError('At least one active subscription is required')
    with db.conn:
        cur = db.conn.execute('INSERT INTO studies(source_id,started_at,ends_at,filters_json) VALUES (?,?,?,?)',
                              (cfg.source_id, now, now + hours * 3600, json.dumps(filters_for(cfg,db),ensure_ascii=False)))
    return cur.lastrowid


def snapshot_state(monitor, now):
    if monitor.source_down or monitor.scraper.closed:
        return 'RESTARTING'
    if not monitor.db.subscriptions(only_enabled=True):
        return 'IDLE'
    states = monitor.db.source_states(monitor.source_id)
    for state in ('VERIFICATION_REQUIRED', 'ACCESS_DENIED'):
        if any(s['state']==state for s in states):
            return state
    for state in ('RATE_LIMITED', 'PARSER_ERROR', 'NETWORK_UNAVAILABLE', 'RECOVERING'):
        if any(s['state']==state and (s['next_allowed_at'] or 0)>now for s in states):
            return state
    return 'AVAILABLE'


class StudyRecorder:
    def __init__(self, db, data_dir):
        self.db, self.data_dir = db, data_dir

    def tick(self, monitor, now: float | None = None):
        now = time.time() if now is None else now
        studies = self.db.conn.execute("SELECT id,ends_at,filters_json FROM studies"
                                       " WHERE source_id=? AND status='running'", (monitor.source_id,)).fetchall()
        for study_id, ends_at, filters_json in studies:
            if now >= ends_at:
                report = make_report(self.db.conn,study_id,now)
                write_report(report,self.data_dir/'studies'/str(study_id))
                with self.db.conn:
                    self.db.conn.execute("UPDATE studies SET status='complete' WHERE id=?",(study_id,))
                continue
            state = snapshot_state(monitor,now)
            if filters_for(monitor.cfg,self.db) != json.loads(filters_json):
                state = 'FILTERS_CHANGED'
            last = self.db.conn.execute('SELECT sampled_at,state FROM study_samples WHERE study_id=?'
                                        ' ORDER BY sampled_at DESC LIMIT 1',(study_id,)).fetchone()
            if last and now-last[0] < SAMPLE_INTERVAL and state==last[1]:
                continue
            with self.db.conn:
                self.db.conn.execute('INSERT OR IGNORE INTO study_samples VALUES (?,?,?,?)',
                                      (study_id,now,state,json.dumps(self.db.queue_counts())))


def distribution(values):
    values=sorted(v for v in values if v is not None and math.isfinite(v) and v>=0)
    if not values:
        return {'n':0,'p50_sec':None,'p95_sec':None,'share_within_60_sec':None}
    return {'n':len(values),'p50_sec':values[math.ceil(len(values)*.5)-1],
            'p95_sec':values[math.ceil(len(values)*.95)-1],
            'share_within_60_sec':sum(v<=60 for v in values)/len(values)}


def make_report(conn, study_id, now=None):
    now = time.time() if now is None else now
    row = conn.execute('SELECT source_id,started_at,ends_at,status,filters_json FROM studies WHERE id=?',
                       (study_id,)).fetchone()
    if not row:
        raise ValueError('Study not found')
    source, started, ends, _, filters_json = row
    until = min(ends,max(started,now))
    duration = until-started
    samples=conn.execute('SELECT sampled_at,state FROM study_samples WHERE study_id=? AND sampled_at<=?'
                         ' ORDER BY sampled_at',(study_id,until)).fetchall()
    durations=Counter()
    for index,(sampled,state) in enumerate(samples):
        next_at=samples[index+1][0] if index+1<len(samples) else until
        durations[state]+=max(0,min(next_at,until,sampled+MAX_SAMPLE_COVERAGE)-max(started,sampled))
    observed=sum(durations.values())
    durations['UNOBSERVED']=max(0,duration-observed)
    fetches=conn.execute('SELECT status,http_status,raw_count,matched_count,overlap_count FROM fetch_metrics'
                         ' WHERE source_id=? AND fetched_at>=? AND fetched_at<=? ORDER BY id',
                         (source,started,until)).fetchall()
    observations=conn.execute('SELECT item_id,published_at,first_seen_at,matched,baseline FROM source_observations'
                              ' WHERE source_id=? AND first_seen_at>=? AND first_seen_at<=?',
                              (source,started,until)).fetchall()
    all_ids={r[0] for r in observations}
    matched_ids={r[0] for r in observations if r[3]}
    new_observations=[r for r in observations if not r[4]]
    new_ids={r[0] for r in new_observations}
    delivery, publication=[],[]
    sent_ids=set()
    for item_id,sent,payload in conn.execute('SELECT item_id,MIN(sent_at),payload_json FROM notification_outbox'
                                            " WHERE status='sent' AND sent_at>=? AND sent_at<=? GROUP BY item_id",
                                            (started,until)):
        payload=json.loads(payload)
        if payload.get('source_id')!=source or item_id not in new_ids:
            continue
        item=payload['item']
        first=item.get('found_at')
        if first is not None and first>=started:
            delivery.append(sent-first)
            if item.get('published') is not None:
                publication.append(sent-item['published'])
            sent_ids.add(item_id)
    successful=sum(r[0] in ('ok','no_results') for r in fetches)
    warnings=[]
    if now<ends:
        warnings.append('Наблюдение ещё не завершено; суточных выводов делать нельзя.')
    if not successful:
        warnings.append('Нет успешных снимков: скорость обнаружения и полноту оценить нельзя.')
    if durations['UNOBSERVED']>duration*.05:
        warnings.append('Есть периоды без наблюдателя; они не считаются работой источника.')
    if durations.get('FILTERS_CHANGED',0) or any(s=='FILTERS_CHANGED' for _,s in samples):
        warnings.append('Во время измерения изменились фильтры; выборка несопоставима.')
    if len(delivery)<100:
        warnings.append('Менее 100 подтверждённых объявлений: оценка p95 предварительная.')
    warnings.append('Первый снимок каждого города исключён из задержек: время его появления ранее неизвестно.')
    warnings.append('ID-overlap выявляет возможные пропуски, но не доказывает полноту выдачи без контрольного канала.')
    report={'study_id':study_id,'source':source,'started_at':started,'ends_at':ends,'measured_until':until,
            'status':'complete' if now>=ends else 'running',
            'requested_hours':(ends-started)/3600,'elapsed_hours':duration/3600,
            'filters':json.loads(filters_json),'sample_count':len(samples),
            'sampled_state_seconds':dict(durations),'observer_coverage':observed/duration if duration else 0,
            'fetch_count':len(fetches),'fetch_status_counts':dict(Counter(r[0] for r in fetches)),
            'http_status_counts':dict(Counter(str(r[1]) for r in fetches if r[1] is not None)),
            'successful_fetches':successful,'potential_coverage_gaps':sum(r[4]==0 and r[2]>0 for r in fetches),
            'observed_unique_ids':len(all_ids),'matched_unique_ids':len(matched_ids),
            'new_unique_ids':len({r[0] for r in new_observations}),
            'matched_new_unique_ids':len({r[0] for r in new_observations if r[3]}),
            'baseline_unique_ids':len({r[0] for r in observations if r[4]}),
            'delivered_unique_ids':len(sent_ids),
            'publish_to_discovery':distribution([r[2]-r[1] for r in new_observations if r[3] and r[1] is not None]),
            'discovery_to_telegram':distribution(delivery),'publish_to_telegram':distribution(publication),
            'warnings':warnings}
    return report


def render_markdown(report):
    stamp=lambda t:datetime.fromtimestamp(t).strftime('%d.%m.%Y %H:%M:%S')
    lines=['# Наблюдение локального канала Авито','',
           f"Источник: {report['source']}. Состояние: {report['status']}.",
           f"Начало: {stamp(report['started_at'])}. Плановый конец: {stamp(report['ends_at'])}.",
           f"Собрано {report['elapsed_hours']:.2f} из {report['requested_hours']:.2f} ч. "
           f"Покрытие наблюдателем: {report['observer_coverage']:.1%}.",'',
           f"Опросов: {report['fetch_count']}, успешных: {report['successful_fetches']}. "
           f"Возможных разрывов выдачи: {report['potential_coverage_gaps']}.",
           f"ID: всего {report['observed_unique_ids']}, новых {report['new_unique_ids']}, "
           f"подходящих {report['matched_unique_ids']}, доставленных {report['delivered_unique_ids']}.",'',
           '## Состояния по пассивным замерам','', '| Состояние | Минуты |','|---|---:|']
    for state,seconds in report['sampled_state_seconds'].items():
        lines.append(f'| {state} | {seconds/60:.1f} |')
    lines+=['','Замер состояния выполняется раз в минуту и при изменении. '
            'Длинные периоды без замеров помечаются UNOBSERVED.','',
            '## Задержки','', '| Метрика | n | p50, с | p95, с | ≤60 с |','|---|---:|---:|---:|---:|']
    for key,label in [('publish_to_discovery','Публикация → обнаружение'),
                      ('discovery_to_telegram','Обнаружение → Telegram'),('publish_to_telegram','Публикация → Telegram')]:
        s=report[key]
        if s['n']:
            lines.append(f"| {label} | {s['n']} | {s['p50_sec']:.1f} | {s['p95_sec']:.1f} | {s['share_within_60_sec']:.1%} |")
        else:
            lines.append(f'| {label} | 0 | — | — | — |')
    lines+=['','## Ограничения вывода','']+[f'- {w}' for w in report['warnings']]
    return '\n'.join(lines)+'\n'


def write_report(report, directory):
    directory.mkdir(parents=True,exist_ok=True,mode=0o700)
    for name,content in [('report.json',json.dumps(report,ensure_ascii=False,indent=2)+'\n'),
                         ('report.md',render_markdown(report))]:
        temp=directory/(name+'.tmp')
        temp.write_text(content,encoding='utf-8')
        temp.replace(directory/name)


def main():
    parser=argparse.ArgumentParser(description='Passive local source study; makes no network requests')
    sub=parser.add_subparsers(dest='action',required=True)
    start=sub.add_parser('start');start.add_argument('--hours',type=float,default=24)
    report=sub.add_parser('report');report.add_argument('--id',type=int);report.add_argument('--json',action='store_true')
    args=parser.parse_args();cfg=load_config();path=cfg.data_dir/'bot.sqlite3'
    if args.action=='start':
        db=DB(path)
        try:
            study_id=begin_study(db,cfg,args.hours)
            print(f'Study {study_id} started. Report: avito-study report --id {study_id}')
        except ValueError as exc:
            parser.error(str(exc))
        finally:
            db.close()
    else:
        with sqlite3.connect(f'file:{path}?mode=ro',uri=True) as conn:
            study_id=args.id
            if study_id is None:
                row=conn.execute('SELECT MAX(id) FROM studies').fetchone()
                study_id=row[0] if row else None
            try:
                result=make_report(conn,study_id)
            except ValueError as exc:
                parser.error(str(exc))
            write_report(result,cfg.data_dir/'studies'/str(study_id))
            print(json.dumps(result,ensure_ascii=False,indent=2) if args.json else render_markdown(result))
