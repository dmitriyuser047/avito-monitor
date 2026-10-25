# Browser session audit — 26 October 2026

Baseline and current `origin/main`: `e1577f62e71c8e3334aaa931ae5c6838361c470d` (fetched before work).
No production navigation or live Avito experiment was performed.

## Startup sequence at baseline

`run → DB migrations/restored restrictions → Telegram + DeliveryWorker → DirectProxy.start
→ Scraper.start → launch_persistent_context → select first tab → passive CDP counters
→ Monitor._poll_once → saved restrictions + next_poll gate → Scraper.fetch
→ first load/page.goto → classify → state + metric writes`.

`Scraper.start()` itself contains no catalog navigation. A first `light_fetch` calls `load()`;
subsequent successful checks use one browser fetch. An HTTP restriction switches the next
**allowed** check to navigation. The existing HTTP 429 IP check reads the navigated DOM once;
it does not immediately retry. Repeated 429 already uses bounded exponential backoff, not `/resume`.

## Findings and limits of evidence

1. Browser lifecycle, collection and metrics share Scraper; startup failure and source restriction
   need separate health information. First-tab selection is unsafe for user-owned CDP Chrome.
2. `_confirmed` is updated before SQLite writes. A failed write can permanently suppress confirmation.
3. `confirm_verification` and `/unblock` clear every pending city on the evidence of one catalog.
4. Confirmation performs several independent commits and can temporarily erase an active server term.
5. A 429 is classified first by HTTP status; check presence is available only as an auxiliary flag.
   Independent persisted access facts are needed to preserve both the term and manual verification.
6. Main-frame generation protects against stale light-fetch DOM, but it needs a browser connection
   epoch and exact catalog/query match for reconnect and multi-city recovery.
7. Passive counters are installed after launch. Requests before instrumentation, startup tab restores,
   redirect chains, and the origin of the first failure cannot be reconstructed from these counters.
8. Managed profiles already persist and differ by engine/channel. There is no automatic profile fallback.
   Browser restart must retain SQLite restrictions and the same profile.
9. DirectProxy binds CONNECT sockets, but its DoH client follows the OS/environment route. Therefore
   the baseline's claim that all traffic bypasses a VPN is stronger than the evidence.
10. DirectProxy has no active tunnel registry/connection ceiling; stop closes only listener and DoH.
    Slow tunnels and outstanding DNS tasks can survive cleanup. DNS/CONNECT failures correctly become
    transport failures/502, not synthetic 429, and must continue to do so.
11. CDP traffic belongs to user Chrome. A configured DirectProxy proves neither its route nor external IP.
    No public-IP lookup is added; equality with ordinary Chrome's external IP remains **unknown**.
12. Navigation success already extracts the DOM without an extra fetch. The new collector must preserve
    this and only reuse matching documents once, without indefinitely caching a live feed.
13. Telegram polling and durable delivery already start independently of Chrome; watchdog/supervisor and
    LaunchAgent require no replacement. Existing WAL, deduplication and Outbox transactions must remain.

The on-disk database was inspected read-only for technical timestamps/statuses only. Historical
HTTP 429 and restored restrictions exist; missing launch-level instrumentation means it is impossible
to prove that an observed 429 was the first Chrome navigation or to attribute it to a particular IP.
Diagnostic fields without measurements must say `unknown`. Startup instrumentation never contacts Avito.

## References used for design (no source code copied)

- [domovoyproj/avito-parser](https://github.com/domovoyproj/avito-parser): recognize challenges and inspect
  the browser page after a person's check. Do not adopt automated challenge solving.
- [AlexButiev/avito-personal-mcp](https://github.com/AlexButiev/avito-personal-mcp): explicitly authorized
  browser session through CDP.
- [Duff89/parser_avito](https://github.com/Duff89/parser_avito): centralize network/session failure handling;
  do not adopt proxy rotation or anti-restriction retries.
- [Chrome 136 remote debugging change](https://developer.chrome.com/blog/remote-debugging-port): remote
  debugging requires a separate non-default user data directory.
