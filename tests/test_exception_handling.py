"""Tests for granular exception handling in Snapper iterable processing."""

import pytest
from pathlib import Path

from snapperable import Snapper
from snapperable.storage.pickle_storage import PickleSnapshotStorage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class ItemError(Exception):
    """Simulates a per-item data error."""


class FatalError(Exception):
    """Simulates a systemic/fatal error (e.g. GPU OOM, network down)."""


class AnotherError(Exception):
    """A second error type for multi-type tests."""


def _storage(tmp_path: Path, name: str = "test.chkpt") -> PickleSnapshotStorage:
    return PickleSnapshotStorage(str(tmp_path / name))


# ---------------------------------------------------------------------------
# failed_items tracking
# ---------------------------------------------------------------------------


def test_failed_items_empty_when_no_errors(tmp_path: Path):
    """failed_items is empty when processing succeeds for all items."""
    snapper = Snapper(
        range(5),
        lambda x: x * 2,
        snapshot_storage=_storage(tmp_path),
        skip_item_errors=True,
    )
    snapper.start()
    assert snapper.failed_items == []


def test_failed_items_tracks_per_item_exceptions(tmp_path: Path):
    """Items that raise exceptions are recorded in failed_items and skipped."""
    fail_on = {2, 4}

    def process(item: int) -> int:
        if item in fail_on:
            raise ItemError(f"bad item {item}")
        return item * 10

    snapper = Snapper(
        range(6),
        process,
        snapshot_storage=_storage(tmp_path),
        skip_item_errors=True,
    )
    snapper.start()

    # Two items should have failed
    assert len(snapper.failed_items) == 2
    failed_items_values = {fi.item for fi in snapper.failed_items}
    assert failed_items_values == fail_on

    # Each FailedItem carries the right exception
    for fi in snapper.failed_items:
        assert isinstance(fi.exception, ItemError)
        assert str(fi.item) in str(fi.exception)


def test_successful_items_stored_when_some_fail(tmp_path: Path):
    """Items that succeed are still stored even when other items fail."""
    fail_on = {1, 3}

    def process(item: int) -> int:
        if item in fail_on:
            raise ItemError("bad")
        return item * 2

    storage = _storage(tmp_path)
    snapper = Snapper(
        range(5), process, snapshot_storage=storage, skip_item_errors=True
    )
    snapper.start()

    results = snapper.load()
    # Only items 0, 2, 4 succeed → outputs 0, 4, 8
    assert sorted(results) == [0, 4, 8]


def test_failed_items_reset_on_each_start_call(tmp_path: Path):
    """failed_items is reset at the beginning of every start() call."""
    call_count = {"n": 0}

    def process(item: int) -> int:
        call_count["n"] += 1
        if item == 0 and call_count["n"] == 1:
            raise ItemError("first run only")
        return item

    storage = _storage(tmp_path)
    # First run – item 0 fails
    snapper = Snapper([0, 1], process, snapshot_storage=storage, skip_item_errors=True)
    snapper.start()
    assert len(snapper.failed_items) == 1

    # Second run on a fresh Snapper (same storage) – item 0 is retried and succeeds
    storage2 = _storage(tmp_path, "test2.chkpt")
    snapper2 = Snapper(
        [0, 1], process, snapshot_storage=storage2, skip_item_errors=True
    )
    snapper2.start()
    assert snapper2.failed_items == []


# ---------------------------------------------------------------------------
# failed_items provides retry capability
# ---------------------------------------------------------------------------


def test_failed_items_can_be_retried(tmp_path: Path):
    """Items from failed_items can be fed back into a new Snapper for retry."""
    call_counts: dict[int, int] = {}

    def process(item: int) -> int:
        call_counts[item] = call_counts.get(item, 0) + 1
        # Fail on first attempt only
        if call_counts[item] == 1 and item in {1, 3}:
            raise ItemError("transient")
        return item * 100

    storage1 = _storage(tmp_path, "run1.chkpt")
    snapper1 = Snapper(
        range(5), process, snapshot_storage=storage1, skip_item_errors=True
    )
    snapper1.start()

    assert len(snapper1.failed_items) == 2

    # Retry only the failed items with a fresh storage
    retry_items = [fi.item for fi in snapper1.failed_items]
    storage2 = _storage(tmp_path, "run2.chkpt")
    snapper2 = Snapper(
        retry_items, process, snapshot_storage=storage2, skip_item_errors=True
    )
    snapper2.start()

    assert snapper2.failed_items == []
    results = snapper2.load()
    assert sorted(results) == [100, 300]


# ---------------------------------------------------------------------------
# fatal_exceptions
# ---------------------------------------------------------------------------


def test_fatal_exception_halts_processing(tmp_path: Path):
    """A fatal exception type re-raises immediately and stops processing."""
    processed = []

    def process(item: int) -> int:
        processed.append(item)
        if item == 2:
            raise FatalError("system down")
        return item

    snapper = Snapper(
        range(5),
        process,
        snapshot_storage=_storage(tmp_path),
        skip_item_errors=True,
        fatal_exceptions=(FatalError,),
    )

    with pytest.raises(FatalError, match="system down"):
        snapper.start()

    # Items after the fatal one were not processed
    assert 3 not in processed and 4 not in processed


def test_fatal_exception_not_in_failed_items(tmp_path: Path):
    """When a fatal exception halts processing, failed_items stays empty."""

    def process(item: int) -> int:
        if item == 1:
            raise FatalError("fatal")
        return item

    snapper = Snapper(
        range(5),
        process,
        snapshot_storage=_storage(tmp_path),
        skip_item_errors=True,
        fatal_exceptions=(FatalError,),
    )

    with pytest.raises(FatalError):
        snapper.start()

    assert snapper.failed_items == []


def test_non_fatal_exception_not_halted_by_fatal_config(tmp_path: Path):
    """Non-fatal exception types are skipped even when fatal_exceptions is set."""

    def process(item: int) -> int:
        if item == 2:
            raise ItemError("skippable")
        return item * 10

    snapper = Snapper(
        range(4),
        process,
        snapshot_storage=_storage(tmp_path),
        skip_item_errors=True,
        fatal_exceptions=(FatalError,),
    )
    snapper.start()  # should NOT raise

    assert len(snapper.failed_items) == 1
    assert snapper.failed_items[0].item == 2


def test_multiple_fatal_exception_types(tmp_path: Path):
    """Multiple types can be specified in fatal_exceptions."""

    def process(item: int) -> int:
        if item == 1:
            raise AnotherError("also fatal")
        return item

    snapper = Snapper(
        range(4),
        process,
        snapshot_storage=_storage(tmp_path),
        skip_item_errors=True,
        fatal_exceptions=(FatalError, AnotherError),
    )

    with pytest.raises(AnotherError):
        snapper.start()


# ---------------------------------------------------------------------------
# max_consecutive_exceptions
# ---------------------------------------------------------------------------


def test_max_consecutive_exceptions_halts_on_threshold(tmp_path: Path):
    """Processing halts with RuntimeError when consecutive exceptions reach the threshold."""

    def process(item: int) -> int:
        # Always fail → all consecutive
        raise ItemError("always fails")

    snapper = Snapper(
        range(10),
        process,
        snapshot_storage=_storage(tmp_path),
        skip_item_errors=True,
        max_consecutive_exceptions=3,
    )

    with pytest.raises(RuntimeError, match="3 consecutive"):
        snapper.start()


def test_max_consecutive_exceptions_resets_on_success(tmp_path: Path):
    """A successful item resets the consecutive-exception counter."""

    def process(item: int) -> int:
        # Fail on items 0 and 1 (two in a row), succeed on item 2, then fail two more
        if item in {0, 1, 3, 4}:
            raise ItemError("fail")
        return item * 10

    snapper = Snapper(
        range(5),
        process,
        snapshot_storage=_storage(tmp_path),
        skip_item_errors=True,
        max_consecutive_exceptions=3,  # would only trigger at 3 in a row
    )
    # Should complete without raising because 2 consecutive failures < threshold of 3,
    # and the success at item 2 resets the counter.
    snapper.start()
    assert len(snapper.failed_items) == 4


def test_max_consecutive_exceptions_zero_ignored_when_none(tmp_path: Path):
    """Without max_consecutive_exceptions, unlimited consecutive failures are tolerated."""

    def process(item: int) -> int:
        raise ItemError("all fail")

    snapper = Snapper(
        range(5),
        process,
        snapshot_storage=_storage(tmp_path),
        skip_item_errors=True,
    )
    snapper.start()
    assert len(snapper.failed_items) == 5


def test_default_behavior_exceptions_propagate(tmp_path: Path):
    """By default (skip_item_errors=False), exceptions propagate unchanged."""

    def process(item: int) -> int:
        if item == 2:
            raise ItemError("propagates")
        return item

    snapper = Snapper(range(5), process, snapshot_storage=_storage(tmp_path))
    with pytest.raises(ItemError, match="propagates"):
        snapper.start()

    # No items should be tracked in failed_items since skip_item_errors=False
    assert snapper.failed_items == []
