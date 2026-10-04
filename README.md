# Quota Metering

Per-customer, per-feature monthly quota metering in Python 3.12, backed by Redis and embedded
as an async library in a FastAPI service. Each metered request reserves units, does its work,
then commits or releases them. Quota is never over-served and never goes negative, even when
many requests hit the same counter at once from several service instances.

## Quick start

Requires Docker with Compose v2.

```bash
make load    # builds and starts Redis + 3 API replicas + nginx, then runs a 60 s HTTP load test
make down    # stops the stack and deletes its data
```

`make load` prints one JSON report. The run **passes** if the command exits 0 and the report
shows `"over_limit_orgs": 0` and `"granted_vs_used_mismatches": 0`. If either is non-zero, it
prints `AUDIT FAILED` and exits non-zero. Some `502` responses are expected: the demo
simulates a 5% downstream failure rate (`DOWNSTREAM_FAILURE_RATE`), and those holds are
released. The stack stays up on http://localhost:8080 until `make down`.

## Running tests

```bash
make redis-up   # Redis 7.4 in Docker on localhost:6379
make test       # pytest on the host; expect 64 passed
```

Without Docker: start any Redis on port 6379 (`redis-server`), then run `pytest`. Tests use
`REDIS_URL` (default `redis://localhost:6379/15`) and **flush that DB**, so never point it at
real data. `make check` runs black, ruff, mypy and pytest, the same checks CI runs.

`tests/test_concurrency.py` starts 8 OS processes with 50 concurrent requests each against one
counter, and checks that no more than the limit is ever granted. Its control test runs the
same load against a naive GET-then-INCRBY version and **requires** that version to
over-serve. That proves the harness really creates contention.

## Benchmarks

```bash
make bench   # library only: 4 processes x 32 concurrent loops, 5,000 orgs, 20 s (Redis DB 14, flushed first)
```

It prints JSON with `throughput_ops_s`, `latency_ms` (p50/p95/p99/max/mean),
`rejected_requests`, `orgs_touched` and `invariant_violations`. For every org touched it checks
`granted == used <= limit` and `reserved == 0`, and exits non-zero (`INVARIANT VIOLATED`) if
any org fails. To run it without Docker: `python -m loadtest.bench --procs 4 --conc 64 --seconds 15`.
Measured numbers are in [DESIGN.md](DESIGN.md#load-test).

## How it works

- Each service instance embeds `QuotaClient` and talks to Redis directly; there is no separate
  quota service: [quota/client.py](quota/client.py).
- Every operation (reserve/consume, commit, release, usage) is one Lua script, so the check
  and the write happen together with nothing in between: [quota/scripts.py](quota/scripts.py).
- `reserve` holds units for 30 s, then `commit` charges them or `release` returns them. Holds
  that are never resolved expire, and the next script call on that counter clears them.
- An `Idempotency-Key` maps a retry to the original reservation, so a retry after a commit
  is not charged again.
- Periods are calendar months in UTC ([quota/periods.py](quota/periods.py)). The period id is
  part of every counter key, so a new month starts on fresh keys and no reset job is needed:
  [quota/keys.py](quota/keys.py).
- All keys for one (org, feature) share the hash tag `{org:feature}`, so scripts also work on
  Redis Cluster.

## API

Served by [service/app.py](service/app.py). Through nginx, it's on http://localhost:8080.

| Method | Path | Metered | Status codes |
|---|---|---|---|
| POST | `/v1/containers/track` | Yes: 1 unit per container; reserve, then commit or release | 200, 400, 403, 409, 422, 429, 502, 503 |
| GET | `/v1/schedules/search?org=` | Yes: 1 unit, one-shot `consume()` | 200, 400, 403, 422, 429, 503 |
| GET | `/v1/quota/{org}/{feature}` | No: usage for the current month | 200, 400, 403, 503 |
| PUT | `/v1/quota/{org}/{feature}` | No: set the limit, body `{"limit": n}` | 200, 400, 422, 503 |
| GET | `/healthz` | No | 200, 503 |

What each code means:
- `400`: invalid org or feature id.
- `403`: no limit is configured for this org and feature.
- `409`: the same `Idempotency-Key` is still in progress.
- `422`: the request body or query fails validation.
- `429`: not enough quota. Batches are all-or-nothing.
- `502`: the downstream work failed, and the reserved quota was returned.
- `503`: Redis is unreachable.

A retry of a committed `/track` request returns `200` with `"replayed": true`. `/track`
responses with status 200 or 502 carry the `X-Quota-Ms` and `X-Instance` headers.

## Using the library in another service

```python
from redis.asyncio import Redis
from quota import QuotaClient

quota = QuotaClient(Redis.from_url("redis://localhost:6379/0"))

async def track(org: str, containers: list[str], request_id: str) -> None:
    async with quota.hold(org, "container-tracking", len(containers), request_id) as res:
        if res.status == "DUPLICATE":
            return  # committed by an earlier attempt: don't redo the work
        await do_tracking(containers)  # returns normally -> commit; raises -> release
```

`hold()` raises these errors:
- `QuotaExceeded`: not enough quota. It carries `remaining`.
- `QuotaNotConfigured`: no limit is set.
- `DuplicateInFlight`: the same key is still being processed.
- `QuotaUnavailable`: Redis is unreachable. The caller decides whether to fail open or closed.

## Configuration

| Variable | Read by | Default | Purpose |
|---|---|---|---|
| `REDIS_URL` | service | `redis://localhost:6379/0` | Redis to use; compose sets `redis://redis:6379/0` |
| `SEED_FILE` | service | unset (no seeding) | Path to `quotas.yaml`; compose sets `/app/quotas.yaml` |
| `FAIL_OPEN_FEATURES` | service | empty | Comma-separated features that serve unmetered when Redis is down, instead of returning 503 |
| `DOWNSTREAM_FAILURE_RATE` | service | `0.05` | Share of simulated downstream failures in `/track` |
| `HOSTNAME` | service | `local` | Instance id returned in `X-Instance` and `/healthz`; set by Docker |
| `REDIS_URL` | tests / bench | `.../15` / `.../14` | Database that tests and bench **flush** |

**How limits are seeded:** [quotas.yaml](quotas.yaml) sets a `default` limit per feature,
generates orgs `org-0001` to `org-NNNN` from `orgs.count`, and applies per-org `overrides`.
At startup the service checks that every limit is an integer >= 0, and fails to start
otherwise. It then writes each limit with `SET NX`. A restart therefore never overwrites a
limit that was changed later through `PUT /v1/quota/{org}/{feature}`.

## Development

```bash
python3 -m venv .venv && source .venv/bin/activate
make install-dev   # dev tools + the git pre-commit hook (black, ruff, mypy, file checks)
make format        # auto-fix lint issues and reformat
make check         # what CI runs (.github/workflows/ci.yml), with tests against Redis 7.4
```

## Repository layout

```
quota/              the library
  periods.py        calendar-month UTC periods
  keys.py           Redis key layout and identifier validation
  scripts.py        Lua scripts: reserve/consume, commit, release, usage
  client.py         async QuotaClient, hold(), error types
service/app.py      FastAPI demo service that uses the library
loadtest/
  bench.py          library benchmark with invariant audit
  loadgen.py        HTTP load generator with client/Redis audit
tests/              64 tests (unit, real Redis, multi-process, HTTP, audit logic)
quotas.yaml         seed limits
docker-compose.yml  Redis, 3 API replicas, nginx; loadgen/bench under profiles
Dockerfile          multi-stage: api and loadtest images
nginx.conf          round-robin across the API replicas
Makefile            up, down, load, bench, redis-up, test, check
DESIGN.md           decisions, measured numbers, limits
```

## Further reading

[DESIGN.md](DESIGN.md) explains why the system is built this way. It covers the options that
were rejected, the concurrency proof, measured load-test numbers, where it falls over, and
what would change to reach 50,000 orgs.
