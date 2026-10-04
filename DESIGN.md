# DESIGN

## Billing periods

"Monthly" means calendar month, UTC. A period is identified by `YYYY-MM`: it starts at
00:00 UTC on the 1st and ends (exclusive) at 00:00 UTC on the 1st of the next month.

The period id will be part of every counter key, so a new month writes to new keys:
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
- Idempotency keys are not period-scoped and live 1 hour. Consequence: a committed request
  retried after the month boundary is not recognised as a duplicate (accepted; see limits).
- Org and feature ids are validated: `{`, `}`, `:`, `|` and spaces are rejected, because
  they would break the hash tag, the tag separator or the reservation token format.

Trade-off: every operation for one (org, feature) serialises on one slot. That is the
point (it gives atomicity), and it scales across orgs, but one hot counter cannot be split.

## Integration shape: in-process library + Redis

Each service instance embeds `QuotaClient` (`quota/client.py`) and talks to Redis directly.
One quota operation = one `EVALSHA` round trip.

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
- Late commit after expiry (work outlived TTL): charge only if capacity is still free,
  otherwise `EXPIRED_UNCHARGED`. We prefer under-charging to over-serving.
- `commit`/`release` are idempotent (per-reservation `done` marker: c / r / x).
- Client retries: an idempotency key maps to the reservation. Retry while held →
  `DuplicateInFlight`. Retry after commit → `DUPLICATE`, not charged. Retry after
  release/expiry → a fresh attempt (the first left no charge). Works across instances
  because the key lives in Redis.
- `consume()` = reserve + commit in one call, for work that cannot fail after the check.
- A reservation is charged to the period it was made in, even if committed after midnight.
- Redis errors surface as `QuotaUnavailable`; the caller chooses fail-open or fail-closed.

## Demo service and HTTP contract

`service/app.py` is one consumer of the library: a FastAPI app run as 3 replicas behind
nginx (`docker-compose.yml`). Replicas share nothing but Redis.

| Situation | Status | Body `error` |
|---|---|---|
| Success | 200 | — (`tracked`, `remaining`; `replayed: true` on an idempotent retry) |
| Bad identifier (e.g. org contains `:`) | 400 | `invalid_request` |
| Feature has no limit for this org | 403 | `feature_not_enabled` |
| Same `Idempotency-Key` still in progress | 409 | `request_in_progress` |
| Not enough quota (all-or-nothing) | 429 | `quota_exceeded` (+ `requested`, `remaining`) |
| Downstream work failed; hold released | 502 | `downstream_failed` |
| Redis unreachable, feature fails closed | 503 | `quota_unavailable` |

- **Metered flow** (`POST /v1/containers/track`): reserve → do the work → commit, or
  release on failure. `GET /v1/schedules/search` cannot fail after the check, so it uses
  one-shot `consume()`.
- **Fail policy**: closed (503) by default; per feature via `FAIL_OPEN_FEATURES`. Failing
  open serves unmetered and logs a warning, for features where availability matters more
  than exact billing.
- **Commit fails after the work succeeded** (Redis drops between the two calls): the caller
  gets 503 and the hold expires uncharged. Under-charging is preferred to double work.
- **Limits** are seeded from `quotas.yaml` with `SET NX`, so a restart never overwrites a
  limit changed through `PUT /v1/quota/{org}/{feature}`. Production would keep limits in
  Postgres behind an admin API, writing through to Redis.
- **Redis socket timeout is 50 ms**: a slow Redis turns into a fast 503 rather than
  stalling every request.

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
| Redis server time per script (`INFO commandstats`, `usec_per_call` for evalsha) | 12.74 µs per `EVALSHA` (1,156,656 calls over both runs) |
| Library, `make bench` (4 procs × 32 conc, 5,000 orgs) | 56,354 ops/s; p50 2.136 ms, p99 4.714 ms; 0 invariant violations across 4,992 orgs. No request was rejected: the 20,000 per-org limit was never reached, so this measures the grant path only |
| HTTP, `make load` (3 replicas, target 300 req/s, 60 s) | 250 req/s achieved (target 300; see "The load generator itself" below); quota overhead p50 0.539 ms, p99 1.521 ms (end to end p50 9.46 ms, p99 15.22 ms); 200: 14,260, 502: 706 (simulated downstream failures), 429: 47; audit passed: 0 over-limit and 0 mismatches across 200 orgs, 0 unknown-outcome units |

## Where it falls over

- **Single Redis node:** scripts run serially; at 12.74 µs each one node is
  bounded at roughly 78,492 ops/s. Beyond that, or for HA, use Redis
  Cluster (keys are already hash-tagged per org+feature).
- **Redis failure:** with AOF `everysec`, a crash can lose ~1 s of deductions (under-count,
  never over-serve beyond that window). Failover to a replica can lose un-replicated writes
  the same way. Fail-closed by default (503), configurable per feature.
- **Clock skew:** periods and hold expiry use the instance clock (NTP). A few seconds of skew
  can put a request at the month boundary in the neighbouring period or expire holds early/late.
- **Idempotency window is 1 hour:** a later retry is treated as a new request. At 2k keyed
  req/s a 24 h window is ~170M keys; 1 h is ~7M.
- **Idempotency across a month boundary:** a committed key retried next month is not
  recognised (its done-marker lives in last month's keys).
- **Hot single counter:** all ops for one (org, feature) serialise on one slot.
- **Sweep is bounded:** at most 100 expired holds cleared per call; after a mass crash the
  rest clear on later calls (expiry stays correct; reporting lags slightly).
- **The load generator itself:** one Python process; if achieved rps is below target, the
  client is the bottleneck, not the service.

## Getting to 50,000 orgs

Memory is not the issue (~1.5M counters, a few hundred MB). What changes:
1. Redis Cluster (3+ primaries with replicas); the `{org:feature}` hash tag already makes it
   cluster-safe.
2. Limits in Postgres with write-through + a small in-process cache.
3. Async usage-event stream (Redis Stream or Kafka) into Postgres for audit, reconciliation
   after a Redis loss, and billing.
4. Metrics: per-script latency, rejection rate, expired-hold rate, fail-open count.
5. Load test on production-like hardware with a distributed generator (k6/Locust).
