import pytest

from quota.keys import keys_for


def hash_tag(key: str) -> str:
    """The part of a key Redis Cluster hashes: text between the first "{" and the next "}"."""
    start = key.index("{")
    return key[start + 1 : key.index("}", start)]


def test_layout() -> None:
    k = keys_for("org-1", "container-tracking", "2026-10")
    assert k.limit == "q:{org-1:container-tracking}:limit"
    assert k.state == "q:{org-1:container-tracking}:2026-10"
    assert k.res == "q:{org-1:container-tracking}:2026-10:res"
    assert k.resunits == "q:{org-1:container-tracking}:2026-10:resunits"
    assert k.done == "q:{org-1:container-tracking}:2026-10:done"
    assert k.idem("abc") == "q:{org-1:container-tracking}:idem:abc"
    assert k.burst == "q:{org-1:container-tracking}:burst"
    assert k.window == "q:{org-1:container-tracking}:win"


def test_all_keys_share_one_hash_tag() -> None:
    # Same slot in Redis Cluster, so one Lua script may touch them all.
    k = keys_for("org-1", "container-tracking", "2026-10")
    tags = {hash_tag(key) for key in k.all_for_script("retry-key-1")}
    assert tags == {"org-1:container-tracking"}


def test_idempotency_key_cannot_escape_the_tag() -> None:
    # Client-supplied text after the tag does not change which slot is hashed.
    k = keys_for("org-1", "container-tracking", "2026-10")
    assert hash_tag(k.idem("{evil}")) == "org-1:container-tracking"


def test_new_period_uses_new_counter_keys_but_same_limit() -> None:
    oct_ = keys_for("org-1", "f", "2026-10")
    nov = keys_for("org-1", "f", "2026-11")
    assert oct_.limit == nov.limit
    assert {oct_.state, oct_.res, oct_.resunits, oct_.done}.isdisjoint(
        {nov.state, nov.res, nov.resunits, nov.done}
    )


def test_different_orgs_and_features_do_not_collide() -> None:
    a = keys_for("org-1", "f", "2026-10").all_for_script()
    b = keys_for("org-2", "f", "2026-10").all_for_script()
    c = keys_for("org-1", "g", "2026-10").all_for_script()
    assert set(a).isdisjoint(b)
    assert set(a).isdisjoint(c)


def test_script_keys_order_and_placeholder() -> None:
    k = keys_for("o", "f", "2026-10")
    assert k.all_for_script() == [
        k.limit,
        k.state,
        k.res,
        k.resunits,
        k.done,
        k.idem("_none"),
        k.burst,
        k.window,
    ]
    assert k.all_for_script("x")[5] == k.idem("x")


@pytest.mark.parametrize("bad", ["", "a{b", "a}b", "a:b", "a|b", "a b"])
def test_invalid_identifiers_rejected(bad: str) -> None:
    with pytest.raises(ValueError):
        keys_for(bad, "f", "2026-10")
    with pytest.raises(ValueError):
        keys_for("o", bad, "2026-10")
