"""Passive, bounded startup telemetry. Never records URLs, headers, bodies or credentials."""
import json
import os
import time
from collections import deque
from pathlib import Path
from urllib.parse import urlsplit


def domain(url: str) -> str:
    try:
        return urlsplit(url).hostname or 'unknown'
    except ValueError:
        return 'unknown'


class StartupDiagnostics:
    def __init__(self, path: Path, enabled: bool = False):
        self.path, self.enabled = path, enabled
        self.events = deque(maxlen=2000)
        self.pending = {}
        self.summary = {'connection': 'unknown', 'first_navigation': None,
                        'requests_before_navigation': None, 'requests_during_navigation': 0,
                        'route': 'unknown', 'external_ip': 'unknown'}
        self.navigating = False

    def update(self, **fields) -> None:
        self.summary.update(fields)
        self.flush()

    def begin_navigation(self, url: str) -> None:
        self.navigating = True
        if self.summary['first_navigation'] is None:
            self.summary['first_navigation'] = {'at': time.time(), 'domain': domain(url)}
        self.flush()

    def end_navigation(self) -> None:
        self.navigating = False
        self.flush()

    def request(self, event: dict) -> None:
        if not self.enabled:
            return
        rid = event.get('requestId')
        now = time.monotonic()
        if len(self.pending) >= 2000:
            self.pending.pop(next(iter(self.pending)))
        self.pending[rid] = now
        phase = 'during' if self.navigating else 'after' if self.summary['first_navigation'] else 'before'
        if phase == 'during':
            self.summary['requests_during_navigation'] += 1
        # Before observer installation Chrome traffic is unknown, so never claim a zero count.
        self.summary['observed_requests_before_navigation'] = (
            self.summary.get('observed_requests_before_navigation', 0) + int(phase == 'before'))
        self.events.append(dict(at=time.time(), kind='request', domain=domain(event.get('request', {}).get('url', '')),
                                type=event.get('type', 'unknown'), phase=phase,
                                redirect_status=event.get('redirectResponse', {}).get('status'),
                                redirect_domain=domain(event.get('redirectResponse', {}).get('url', '')),
                                route=self.summary['route']))

    def response(self, event: dict) -> None:
        if not self.enabled:
            return
        r = event.get('response', {})
        self.events.append(dict(at=time.time(), kind='response', domain=domain(r.get('url', '')),
                                status=r.get('status'), type=event.get('type', 'unknown'),
                                route=self.summary['route']))

    def finished(self, event: dict) -> None:
        started = self.pending.pop(event.get('requestId'), None)
        if self.enabled and started is not None:
            self.events.append(dict(at=time.time(), kind='finished', duration_sec=time.monotonic() - started))

    def flush(self) -> None:
        if not self.enabled:
            return
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            temporary = self.path.with_suffix('.tmp')
            fd = os.open(temporary, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
            with os.fdopen(fd, 'w') as file:
                json.dump(dict(at=time.time(), **self.summary, events=list(self.events)), file)
            temporary.replace(self.path)
        except OSError:
            # Diagnostics cannot change source classification or interrupt collection.
            pass
