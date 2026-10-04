# DESIGN

## Billing periods

"Monthly" means calendar month, UTC. A period is identified by `YYYY-MM`: it starts at
00:00 UTC on the 1st and ends (exclusive) at 00:00 UTC on the 1st of the next month.

The period id is part of every counter key, so a new month writes to new keys:
there is no reset job, so nothing can run late or run twice.

Naive datetimes are rejected rather than assumed UTC: a wrong guess near midnight on the
last day of a month would charge usage to the wrong month.

Rejected: rolling 30 days (needs per-event history, expensive, confusing to customers);
per-org anchor dates (only needed if billing cycles differ; `period_for()` is the single
function to change).

## Key layout and Redis Cluster

All state for one (org, feature) lives under keys sharing the hash tag `{org:feature}`
(see `quota/keys.py`). Redis Cluster hashes only the tag, so every key a Lua script
touches is in one slot and the script stays atomic and cluster-safe without changes.

- The limit key has no period: it is configuration, not usage.
- Usage, holds and done-markers are period-scoped, so a new month starts on fresh keys.
- Idempotency keys are not period-scoped and live 1 hour. Each stores the reservation id,
  its units and a fingerprint of the request payload. Consequence: a committed request
  retried after the month boundary is not recognised as a duplicate (accepted; see limits).
- Org and feature ids are validated: `{`, `}`, `:`, `|` and spaces are rejected, because
  they would break the hash tag, the tag separator or the reservation token format.

Trade-off: every operation for one (org, feature) serialises on one slot. That is the
point (it gives atomicity), and it scales across orgs, but one hot counter cannot be split.

## Integration shape: in-process library + Redis

Each service instance embeds `QuotaClient` (`quota/client.py`) and talks to Redis directly.
One quota operation = one `EVALSHA` round trip (plus a one-off `SCRIPT LOAD` the first time a
connection uses each script: 82 in the first load test).

Rejected: a separate quota microservice. It adds a network hop and a fleet to scale and
operate, for no correctness gain: correctness comes from Redis atomicity, not from
centralising code. The cost of the library choice is that every consumer must be Python
(or port four small Lua-calling methods), and upgrades roll out with each service.

## Concurrent correctness: one Lua script per operation

Check and deduct happen inside a single Lua script (`quota/scripts.py`). Redis executes
scripts serially, so no other client can run between the comparison
`units <= limit - used - reserved` and the `HINCRBY`. There is no read-then-write window.

| Option | Why rejected |
|---|---|
| GET, compare, INCRBY | Race window between read and write; over-serves under contention (measured: 818–1,633 units used on a limit of 500 across 3 runs, see proof below) |
| WATCH/MULTI (optimistic CAS) | Correct, but bursts on one org cause retry storms exactly on hot keys; unbounded latency |
| DECRBY then refund if negative | Counter goes briefly negative; concurrent requests are wrongly rejected; violates "never negative" |
| Postgres `SELECT ... FOR UPDATE` | Correct, but row-lock queueing on hot orgs and ms-level latency per op; more load on the primary DB |
| Per-instance counters / leases | Instances come and go; a crashed instance strands its share |

### Proof: multi-process test against real Redis

`tests/test_concurrency.py`: 8 OS processes (stand-ins for service instances), 50 concurrent
requests each, released at the same instant by a barrier, against one counter in real Redis.

- 400 one-unit requests vs limit 100: exactly 100 granted, `remaining == 0`.
- 400 one-unit requests vs limit 400: all granted, `remaining == 0`.
- Mixed batches (1–50 units) vs limit 500: `granted == used <= 500`, leftover smaller than
  the largest batch.
- Reserve/commit/release with 20% downstream failures: `granted == used`, `reserved == 0`.
- **Control:** the same harness against a naive GET-then-INCRBY implementation must
  over-serve, or the test fails. This proves the harness creates real contention, so the
  passing tests are meaningful rather than lucky. Measured: naive used 818–1,633 units on a
  limit of 500 across 3 runs.

## Batch policy: all-or-nothing

A request for N units with M < N left is rejected whole; the caller gets `remaining`.

Why: partial fulfilment makes the caller decide *which* 60 of 100 containers were served,
breaks idempotency (a retry would get a different partial amount), and turns one error path
into many. Callers that can split work can retry with `remaining` units. Cost: a large batch
can be refused while smaller ones keep succeeding (no fairness/queueing).
`commit(actual_units)` covers the opposite case: reserve 100, find 3 invalid, charge 97.

## Failure and retries: reserve → commit / release, idempotency keys

- `reserve` holds units (counted against remaining) with a 30 s TTL.
- Downstream succeeds → `commit` moves held → used. Fails → `release` returns them.
  `hold()` wraps this as an async context manager.
- Instance dies mid-request → nobody commits; the hold expires. Expiry is **lazy**: every
  script first sweeps expired holds for that counter (max 100 per call). No background job
  to fall behind or die.
- Late commit after expiry (work outlived TTL): charge only if capacity is still free
  (`LATE_COMMITTED`), otherwise `EXPIRED_UNCHARGED`. We prefer under-charging to
  over-serving the counter. `hold()` raises `HoldExpired` in the second case, so the caller
  knows the work was served but not charged. Work that may outlast the TTL calls
  `extend()` to renew its hold; the demo service instead caps downstream time at 10 s.
- `commit`/`release` are idempotent (per-reservation `done` marker: c committed,
  r released, x expired, u expired and uncharged), so the client retries each once on a
  Redis error. A commit for an id this counter never issued returns `UNKNOWN` and charges
  nothing.
- Client retries: an idempotency key maps to the reservation and a fingerprint of the
  payload. Retry while held → `DuplicateInFlight` (`hold()` and `/track`; `consume()`
  never leaves a hold). Retry after commit → `DUPLICATE`, not charged, reporting the
  original units. Same key with a different payload → `IdempotencyKeyReused`. Retry after
  release/expiry → a fresh attempt (the first left no charge). Works across instances
  because the key lives in Redis.
- Lost reply: if a reserve times out, the script may still have run. The client releases
  that reservation by its id (best effort), so a retry is not blocked by an orphaned hold.
- `consume()` = reserve + commit in one call, for work that cannot fail after the check.
- A reservation is charged to the period it was made in, even if committed after midnight.
- Time comes from the Redis server clock inside every script (`TIME`), for hold expiry and
  for the billing month: an instance whose clock is wrong is told the real time and
  retries against the right month's keys. Tests inject a clock instead.
- Redis errors surface as `QuotaUnavailable`; the caller chooses fail-open or fail-closed.

## Demo service and HTTP contract

`service/app.py` is one consumer of the library: a FastAPI app run as 3 replicas behind
nginx (`docker-compose.yml`). Replicas share nothing but Redis.

| Situation | Status | Body `error` |
|---|---|---|
| Success | 200 | — (`tracked`, `remaining`; `replayed: true` on an idempotent retry) |
| Bad identifier (e.g. org contains `:`) | 400 | `invalid_request` |
| `PUT` without the admin token, when `ADMIN_TOKEN` is set | 401 | `unauthorized` |
| Feature has no limit for this org | 403 | `feature_not_enabled` |
| Same `Idempotency-Key` still in progress (`/track` only), with `Retry-After: 1` | 409 | `request_in_progress` |
| Body fails validation (e.g. more than 1,000 containers) | 422 | FastAPI's `detail` |
| Same `Idempotency-Key` reused with a different body | 422 | `idempotency_key_reused` |
| Not enough quota (all-or-nothing) | 429 | `quota_exceeded` (+ `requested`, `remaining`) |
| Downstream work failed; hold released | 502 | `downstream_failed` |
| Redis unreachable, feature fails closed | 503 | `quota_unavailable` |

- **Metered flow** (`POST /v1/containers/track`): reserve → do the work → commit, or
  release on failure. `GET /v1/schedules/search` cannot fail after the check, so it uses
  one-shot `consume()`.
- **Fail policy**: closed (503) by default; per feature via `FAIL_OPEN_FEATURES`. Failing
  open serves unmetered and logs a warning, for features where availability matters more
  than exact billing.
- **Downstream time is capped** at `DOWNSTREAM_TIMEOUT_S` (10 s, checked at startup to
  leave 5 s of the 30 s hold). A timeout counts as a failure: the hold is released and the
  caller gets 502. So work cannot finish after its hold expired; if it ever does (a long
  process pause), the response says `metered: false` and an error is logged.
- **Commit fails after the work succeeded**: the library retries the commit once. If Redis
  is still unreachable the caller gets 503. The commit may or may not have landed (a
  timeout can fire after the script ran); a retry with the same `Idempotency-Key` replays
  if it did, and does the work again if it did not.
- **Limits** are seeded from `quotas.yaml` with `SET NX`, so a restart never overwrites a
  limit changed through `PUT /v1/quota/{org}/{feature}`. Production would keep limits in
  Postgres behind an admin API, writing through to Redis.
- **Redis socket timeout is 50 ms**: a slow Redis turns into a fast 503 rather than
  stalling every request. Ambiguous outcomes from it are handled as above.
- **Admin endpoint**: open in the demo; set `ADMIN_TOKEN` to require a bearer token.

## Load test

Two tools, both ending in a correctness audit, so a fast-but-wrong run fails:

- `loadtest/bench.py` (`make bench`): the library alone. N processes × M concurrent loops,
  skewed bursty traffic (20% of orgs get 80% of requests). Audit per org touched:
  `granted == used <= limit` and `reserved == 0`.
- `loadtest/loadgen.py` (`make load`): HTTP through nginx to 3 replicas, bursty per org,
  5% downstream failures, client retries reusing the `Idempotency-Key` (always on a lost
  response, plus 5% of delivered ones). Audit per org touched: `used <= limit`, and
  `used` equals units clients were told succeeded. A request whose every attempt died in
  transit has an unknown outcome, so `used` may lie anywhere in
  `[known, known + unknown]`; anything outside that range fails the run.

Measured on an Apple M3 laptop (8 cores: 4 performance + 4 efficiency, 8 GB RAM, macOS 26.5)
with Docker Desktop 28.1.1 limited to 8 CPUs and 4 GiB. Redis, the 3 API replicas, nginx and
the load generators all ran on that one machine:

| Test | Result |
|---|---|
| Redis server time per script (`INFO commandstats`, `usec_per_call` for evalsha) | 13.71 µs per `EVALSHA` (1,103,722 calls over bench + load). An A/B run of the same workload, 3 rounds each, measured the scripts at about 1.5 µs more than before the Redis-clock and period-check change (13.3 → 14.8 µs), about 1 µs of it the `TIME` call |
| Library, `make bench` (4 procs × 32 conc, 5,000 orgs) | 53,707 ops/s; p50 2.278 ms, p99 4.822 ms; 0 invariant violations across 4,985 orgs. Earlier runs before that change gave 56,354 and 56,989 ops/s. The gap is within run-to-run noise (one version alone varied 15% across rounds), and the bench is limited by its Python client processes rather than Redis, so the 1.5 µs barely registers. No request was rejected: the 20,000 per-org limit was not reached, so this measures the grant path |
| HTTP, `make load` (3 replicas, target 300 req/s, 60 s) | 251 req/s achieved (target 300); quota overhead p50 0.563 ms, p99 1.161 ms (end to end p50 9.34 ms, p99 14.78 ms); 200: 14,290, 502: 713 (simulated downstream failures), 429: 53; audit passed: 0 over-limit and 0 mismatches across 200 orgs, 0 unknown-outcome units |
| HTTP with faults (same load; SIGKILL one API replica at 15 s, `docker compose restart redis` at 30 s) | 241 req/s; 200: 13,587, 502: 709, 503: 109 (fail closed while Redis restarted), 429: 91. The killed replica stopped at 1,141 requests and the other two took its traffic. Audit passed: 0 over-limit, 0 mismatches, 0 unknown-outcome units. The Redis restart was graceful (AOF flushed on shutdown); a hard Redis crash was not tested |

## Where it falls over

- **Single Redis node:** scripts run serially; at 13.71 µs each one node is
  bounded at roughly 72,939 ops/s (computed, not measured: no run pushed Redis that far). Beyond that, or for HA, use Redis
  Cluster (keys are already hash-tagged per org+feature).
- **Redis failure:** with AOF `everysec`, a crash can lose ~1 s of deductions. Redis then
  believes less was used, so it **can over-serve by up to that ~1 s of traffic**. Failover to
  a replica can lose un-replicated writes the same way. Closing that gap needs an event log
  (see below) to reconcile from. Fail-closed by default (503), configurable per feature.
- **Clock skew:** hold expiry and the billing month use the Redis server clock, so instance
  skew no longer matters. A Redis failover to a node whose clock differs would still shift
  both by that difference.
- **Exactly-once is bounded by the hold:** a retry after a hold expired, while the original
  is somehow still working, starts a second attempt. The 10 s downstream cap (or `extend()`)
  is what keeps the original from outliving its hold.
- **Idempotency window is 1 hour:** a later retry is treated as a new request. At 2k keyed
  req/s a 24 h window is ~170M keys; 1 h is ~7M.
- **Idempotency across a month boundary:** a committed key retried next month is not
  recognised (its done-marker lives in last month's keys).
- **Hot single counter:** all ops for one (org, feature) serialise on one slot.
- **Sweep is bounded:** at most 100 expired holds cleared per call. After a mass crash,
  `reserved` stays too high until later calls clear the rest, so some requests may be
  rejected that should have been granted (under-serving, never over-serving).
- **The load generator itself:** one Python process. Runs reached ~250 of 300 req/s
  target; the cause was not measured, but Python's sleep-based pacing is the likely limit.

## Getting to 50,000 orgs

Memory is not the issue (~1.5M counters, a few hundred MB). What changes:
1. Redis Cluster (3+ primaries with replicas); the `{org:feature}` hash tag already makes it
   cluster-safe.
2. Limits in Postgres with write-through + a small in-process cache.
3. Async usage-event stream (Redis Stream or Kafka) into Postgres for audit, reconciliation
   after a Redis loss, and billing.
4. Metrics: per-script latency, rejection rate, expired-hold rate, fail-open count.
5. Load test on production-like hardware with a distributed generator (k6/Locust).
