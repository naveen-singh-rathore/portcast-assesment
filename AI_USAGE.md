# AI usage

I built this assessment with AI assistance, and this file says how. In short: **Claude Code
wrote most of the code and documentation. I set the requirements, made the design
decisions, decided what counted as proof, and reviewed and ran everything.** Every claim in
the README and DESIGN.md is backed by a test or a measured run that you can repeat
(`make check`, `make load`, `make bench`).

## Tools

| Tool | Used for |
|---|---|
| Claude Code (in VS Code and the terminal) | Writing code, tests and docs; running tests, load tests and benchmarks; reviewing the repo |
| Claude.ai chat | Talking through design options before building them |

No other AI tools were used.

## Who did what

| Phase | My part | AI's part |
|---|---|---|
| Design (4 Oct) | Chose Redis with atomic Lua scripts; reserve, then commit or release; an in-process library rather than a quota service; calendar months in UTC | Laid out the alternatives and their trade-offs (DESIGN.md's "rejected" tables), and wrote DESIGN.md |
| Core library, tests, demo service, load tests (4 Oct) | Set the requirements and the bar for proof, reviewed each PR, ran the suites | Wrote the key layout, Lua scripts, `QuotaClient`, the FastAPI service, the benchmark and the load generator |
| Review round (4 Oct, commit `bc080e7`) | Asked for a review of the whole codebase and chose what to fix | Found and fixed issues in commit, idempotency, time and failure handling |
| Burst limit (5 Oct) | Asked for a fixed-window burst limit per org | Designed it within the existing Lua script, implemented it, wrote 29 tests, documented the trade-offs |
| Final review and docs (5 Oct) | Asked for a clean check of the whole repo, setup steps for a new machine, and the architecture section | Ran every check from a clean state and from a fresh clone, fixed what it found, wrote the README sections |
| Repo workflow | Branch per feature, PRs, CI, pre-commit hooks, commit conventions | Followed them |

Git history: some commits carry a `Co-Authored-By: Claude` line and some don't, even though
AI wrote code in most of them. Treat this file, not the trailers, as the record.

## How AI output was checked

I didn't accept "it works" from the AI. Each kind of claim had to be backed by a test or a
measured run:

- **Never over-serves under concurrency.** 8 OS processes × 50 requests hit one counter in
  real Redis. A control test runs the same load against a naive GET-then-INCRBY version and
  **must** over-serve, which proves the test really creates contention
  (`tests/test_concurrency.py`). The burst window has the same multi-process proof.
- **Correct under real HTTP traffic and failures.** `make load` audits every org after the
  run: no org over its limit, and Redis usage matches what clients were told. It was also run
  with an API replica killed and Redis restarted mid-test.
- **Lint, types and tests in CI.** black, ruff, mypy in strict mode, and pytest against
  Redis 7.4, on every PR.
- **Reproducible by a stranger.** The setup steps were run on a fresh clone from GitHub
  before being written into the README.
- **Performance claims.** Measured, not estimated, except where the docs say "computed".
  When the burst check looked slower, its cost was measured with an A/B against the
  previous commit, using Redis's own per-script timings.

## What the AI got wrong, and how it was caught

From the 5 October session:

1. **The load-test audit compared the wrong numbers.** It checked this run's grants against
   the whole month's usage, so a second `make load` in the same month failed with false
   mismatches. The AI's first answer was to tell users to reset the stack first. My second
   failed run pushed for a real fix: the audit now records usage before the run and checks
   the growth in usage during it.
2. **An admin endpoint wrote before failing.** Setting a burst limit on a feature with no
   monthly limit saved the setting and then returned 403. This was found in the final review,
   fixed, and covered by a regression test.
3. **Test runs skewed the performance numbers.** The AI ran the test suite on the same Redis
   during a load test, which inflated the measured latency. It threw those numbers out, reran
   with nothing else running, and recorded only the clean run.
4. **The setup instructions would have built the wrong environment.** The README said
   `python3 -m venv`, but on a machine where `python3` is 3.11 that quietly builds an
   environment for the wrong Python version (the project needs 3.12). The fresh-clone run
   caught it; the docs now say `python3.12`.

In the AWS deployment section, anything not built or tested is labelled as such (for
example, Redis cluster mode), rather than presented as working.

## What I'd want a reviewer to take from this

The AI did most of the typing. The decisions, the standard of proof, and the call on when
something was done were mine. Every result above can be repeated with the commands in the
README.
