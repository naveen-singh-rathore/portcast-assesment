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
