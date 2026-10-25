"""Orthogonal access facts and the compatible scheduling projection, persisted atomically."""
import json
import time
from dataclasses import asdict, dataclass

from .sources import FetchStatus, IP_CHECK_REASON, SourceState


@dataclass
class AccessStatus:
    http_status: int | None = None
    rate_limit_until: float | None = None
    rate_limit_is_server_defined: bool = False
    verification_required: bool = False
    verification_confirmed: bool = False
    access_denied: bool = False
    last_success_at: float | None = None
    last_restriction_at: float | None = None

    def source_state(self, fallback=SourceState.AVAILABLE, now=None):
        now = time.time() if now is None else now
        if self.verification_required and self.http_status != 429:
            return SourceState.VERIFICATION_REQUIRED
        if (self.rate_limit_until or 0) > now:
            return SourceState.RATE_LIMITED
        if self.verification_required:
            return SourceState.VERIFICATION_REQUIRED
        if self.access_denied:
            return SourceState.ACCESS_DENIED
        return fallback


class AccessCoordinator:
    def __init__(self, db, source_id):
        self.db, self.source_id = db, source_id
        # Additive backfill preserves all legacy server terms. Historical unknown flags stay conservative.
        with db.atomic():
            for row in db.source_states(source_id):
                if self._stored(row['city']) is None:
                    self._save(row['city'], AccessStatus(
                        http_status=row['http_status'],
                        rate_limit_until=row['server_until'] or (row['next_allowed_at']
                            if row['state'] == SourceState.RATE_LIMITED else None),
                        rate_limit_is_server_defined=row['server_until'] is not None,
                        verification_required=row['state'] == SourceState.VERIFICATION_REQUIRED
                            or row['reason'] == IP_CHECK_REASON,
                        access_denied=row['state'] == SourceState.ACCESS_DENIED,
                        last_restriction_at=row['since'] if row['state'] != SourceState.AVAILABLE else None))

    def _stored(self, city):
        row = self.db.conn.execute('SELECT data_json FROM access_status WHERE source_id=? AND city=?',
                                   (self.source_id, city)).fetchone()
        return AccessStatus(**json.loads(row[0])) if row else None

    def get(self, city):
        return self._stored(city) or AccessStatus()

    def _save(self, city, status):
        self.db.conn.execute('INSERT OR REPLACE INTO access_status VALUES (?,?,?)',
                             (self.source_id, city, json.dumps(asdict(status))))

    def record(self, result, state, next_allowed_at=None, reason='', http_status=None,
               server_until=None):
        """Persist facts and scheduling together, never shorten an existing server deadline."""
        now = time.time()
        old = self.get(result.city)
        facts = AccessStatus(**asdict(old))
        if result.http_status is not None:
            facts.http_status = result.http_status
        if result.status in (FetchStatus.OK, FetchStatus.NO_RESULTS):
            facts.last_success_at = result.observed_at
            facts.access_denied = False
            facts.verification_required = False
            # A successful observation cannot authorize a request inside an unexpired server term.
            if not (old.rate_limit_is_server_defined and (old.rate_limit_until or 0) > now):
                facts.rate_limit_until = None
                facts.rate_limit_is_server_defined = False
        else:
            facts.last_restriction_at = now
            facts.verification_confirmed = False
            facts.verification_required = (result.status == FetchStatus.CHALLENGE
                                           or bool(result.diagnostics.get('verification_page')))
            facts.access_denied = result.status == FetchStatus.BLOCKED
            facts.rate_limit_until = server_until or (next_allowed_at if result.status == FetchStatus.RATE_LIMITED else None)
            facts.rate_limit_is_server_defined = server_until is not None
        if old.rate_limit_is_server_defined and (old.rate_limit_until or 0) > now:
            server_until = max(server_until or 0, old.rate_limit_until)
            facts.rate_limit_until = max(facts.rate_limit_until or 0, server_until)
            facts.rate_limit_is_server_defined = True
        if server_until and server_until > now:
            next_allowed_at = max(next_allowed_at or 0, server_until)
        # Keep the established RATE_LIMITED projection for 429 + an available manual check.
        projected = facts.source_state(state, now)
        if state in (SourceState.PARSER_ERROR, SourceState.NETWORK_UNAVAILABLE) and not server_until:
            projected = state
        with self.db.atomic():
            changed = self.db.save_source_state(self.source_id, result.city, projected, next_allowed_at,
                                                reason, facts.http_status, server_until)
            self._save(result.city, facts)
        return changed

    def confirm(self, city, now):
        """Call inside the same transaction as DOM ingestion and the confirmation event."""
        facts = self.get(city)
        facts.verification_required = False
        facts.verification_confirmed = True
        facts.access_denied = False
        facts.last_success_at = now
        active_server = facts.rate_limit_is_server_defined and (facts.rate_limit_until or 0) > now
        if not active_server:
            facts.rate_limit_until = None
            facts.rate_limit_is_server_defined = False
        self._save(city, facts)
        self.db.save_source_state(self.source_id, city,
            SourceState.RATE_LIMITED if active_server else SourceState.AVAILABLE,
            facts.rate_limit_until if active_server else None,
            'verification passed; Retry-After' if active_server else '', facts.http_status,
            facts.rate_limit_until if active_server else None)
        return facts
