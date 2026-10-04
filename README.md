# Quota Metering

A per-customer, per-feature monthly quota metering component in Python 3, backed by Redis
and embedded as an async library in a FastAPI service. Each metered call reserves quota
atomically in Redis, then commits or releases it. Quota is never over-served and never goes
negative under concurrent requests, including across multiple service instances.

> Status: initial repository setup. The sections marked TODO are filled in as the code lands.

## Quick start

<!-- TODO: single command to bring up the stack and run the load test (docker-compose.yml / Makefile),
     expected output, and how to tell pass from fail (exit code, audit fields). -->

## Running tests

<!-- TODO: docker command, local alternative (redis-server + pytest), expected test count,
     what the concurrency test and the naive control test prove. -->

## Benchmarks

<!-- TODO: how to run loadtest/bench.py and what it reports. Numbers live in DESIGN.md, not here. -->

## How it works

<!-- TODO: library + Redis, one atomic Lua script per operation, reserve -> commit/release,
     idempotency keys, calendar-month UTC periods in key names. One line per item, each
     linking to the implementing file. -->

## API

<!-- TODO: table of every endpoint in service/app.py: method, path, metered or not, status codes. -->

## Using the library in another service

<!-- TODO: minimal hold() example. -->

## Configuration

<!-- TODO: every environment variable the service reads, and quotas.yaml seeding (SET NX). -->

## Development

Requires Python 3.12+.

```bash
python3 -m venv .venv && source .venv/bin/activate
make install-dev      # installs dev tools and the git pre-commit hook
```

| Command          | What it does                                          |
|------------------|-------------------------------------------------------|
| `make format`    | Auto-fix lint issues (ruff) and format code (black)   |
| `make lint`      | Check formatting and lint rules without changing files |
| `make typecheck` | Run mypy in strict mode                               |
| `make test`      | Run pytest                                            |
| `make check`     | Run everything CI runs                                |

Code standards, configured in [pyproject.toml](pyproject.toml):

- **black** formats code (line length 100).
- **ruff** handles linting and import sorting (pycodestyle, pyflakes, isort, bugbear,
  pyupgrade, simplify, async, ruff rules).
- **mypy** runs in strict mode. `tests/` and `loadtest/` may leave functions unannotated.
- **pre-commit** runs the checks above, plus whitespace, YAML/TOML, merge-marker and
  private-key checks, on every commit. To run it on every file: `pre-commit run --all-files`.

CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)) runs on every pull request and on
every push to `main`. It has two jobs:

1. **Lint**: pre-commit hooks, `black --check`, `ruff check`, `mypy`.
2. **Tests**: `pytest` against a Redis 7 service container (`REDIS_URL=redis://localhost:6379/0`).

## Repository layout

```
.github/workflows/ci.yml   CI: lint, format, type check, tests
.pre-commit-config.yaml    Git hooks run on every commit
.editorconfig              Editor whitespace and indent rules
pyproject.toml             black, ruff, mypy and pytest configuration
requirements-dev.txt       Pinned dev and CI tooling
Makefile                   Development shortcuts
```

<!-- TODO: add quota/, service/, tests/, loadtest/, docker-compose.yml, quotas.yaml, requirements.txt. -->

## Further reading

<!-- TODO: link DESIGN.md (decisions, load-test numbers, limits) and AI_USAGE.md once they exist. -->
