"""The load-test audit is what turns a load run into a correctness check, so test it."""

from loadtest.loadgen import Outcome, audit


def test_clean_run_passes() -> None:
    outcomes = [Outcome("a", 5, succeeded=True), Outcome("a", 3), Outcome("b", 1, succeeded=True)]
    r = audit(outcomes, {"a": (5, 10), "b": (1, 10)})
    assert r["over_limit_orgs"] == 0 and r["granted_vs_used_mismatches"] == 0


def test_over_limit_is_flagged() -> None:
    r = audit([Outcome("a", 11, succeeded=True)], {"a": (11, 10)})
    assert r["over_limit_orgs"] == 1


def test_charged_but_not_reported_is_a_mismatch() -> None:
    r = audit([Outcome("a", 5, succeeded=True)], {"a": (9, 10)})
    assert r["granted_vs_used_mismatches"] == 1


def test_reported_but_not_charged_is_a_mismatch() -> None:
    r = audit([Outcome("a", 5, succeeded=True)], {"a": (0, 10)})
    assert r["granted_vs_used_mismatches"] == 1


def test_lost_response_may_or_may_not_have_been_charged() -> None:
    # Every attempt died in transit: the server may have charged it or not.
    outcomes = [Outcome("a", 5, succeeded=True), Outcome("a", 4, unknown=True)]
    for used in (5, 9):
        assert audit(outcomes, {"a": (used, 10)})["granted_vs_used_mismatches"] == 0
    assert audit(outcomes, {"a": (10, 10)})["granted_vs_used_mismatches"] == 1


def test_usage_from_before_the_run_is_not_a_mismatch() -> None:
    # An earlier run this month left 500 used; this run granted 5 more.
    outcomes = [Outcome("a", 5, succeeded=True)]
    assert audit(outcomes, {"a": (505, 1000)}, {"a": 500})["granted_vs_used_mismatches"] == 0
    assert audit(outcomes, {"a": (505, 1000)})["granted_vs_used_mismatches"] == 1  # no baseline
    assert audit(outcomes, {"a": (509, 1000)}, {"a": 500})["granted_vs_used_mismatches"] == 1


def test_over_limit_counts_usage_from_before_the_run() -> None:
    outcomes = [Outcome("a", 5, succeeded=True)]
    assert audit(outcomes, {"a": (505, 500)}, {"a": 500})["over_limit_orgs"] == 1


def test_unknown_then_succeeded_counts_as_known() -> None:
    o = Outcome("a", 5, succeeded=True, unknown=True)  # first attempt lost, retry replayed
    assert audit([o], {"a": (5, 10)})["granted_vs_used_mismatches"] == 0
    assert audit([o], {"a": (0, 10)})["granted_vs_used_mismatches"] == 1
