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
