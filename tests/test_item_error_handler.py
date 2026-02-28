"""Unit tests for ItemErrorHandler and FailedItem."""

import pytest

from snapperable import FailedItem, ItemErrorHandler


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class ItemError(Exception):
    """Simulates a per-item data error."""


class FatalError(Exception):
    """Simulates a systemic/fatal error (e.g. GPU OOM, network down)."""


# ---------------------------------------------------------------------------
# FailedItem
# ---------------------------------------------------------------------------


def test_failed_item_stores_item_and_exception():
    exc = ValueError("test error")
    fi: FailedItem[int] = FailedItem(item=42, exception=exc)
    assert fi.item == 42
    assert fi.exception is exc


def test_failed_item_repr():
    exc = ValueError("test error")
    fi: FailedItem[int] = FailedItem(item=42, exception=exc)
    r = repr(fi)
    assert "42" in r
    assert "test error" in r


# ---------------------------------------------------------------------------
# ItemErrorHandler – default behaviour (skip_item_errors=False)
# ---------------------------------------------------------------------------


def test_handler_propagates_by_default():
    """By default (skip_item_errors=False), on_item_error returns False for all exceptions."""
    handler: ItemErrorHandler[int] = ItemErrorHandler()
    exc = ItemError("x")
    assert handler.on_item_error(1, exc) is False
    assert handler.failed_items == []


def test_handler_default_no_failed_items_on_success():
    """on_item_success() is a no-op when skip_item_errors=False."""
    handler: ItemErrorHandler[int] = ItemErrorHandler()
    handler.on_item_success()
    assert handler.failed_items == []


# ---------------------------------------------------------------------------
# ItemErrorHandler – skip_item_errors=True
# ---------------------------------------------------------------------------


def test_handler_skips_when_enabled():
    """With skip_item_errors=True, on_item_error records the failure and returns True."""
    handler: ItemErrorHandler[int] = ItemErrorHandler(skip_item_errors=True)
    exc = ItemError("x")
    assert handler.on_item_error(42, exc) is True
    assert len(handler.failed_items) == 1
    assert handler.failed_items[0].item == 42
    assert handler.failed_items[0].exception is exc


def test_handler_accumulates_multiple_failures():
    """Multiple failed items are all recorded."""
    handler: ItemErrorHandler[int] = ItemErrorHandler(skip_item_errors=True)
    handler.on_item_error(1, ItemError("a"))
    handler.on_item_error(2, ItemError("b"))
    assert len(handler.failed_items) == 2
    assert {fi.item for fi in handler.failed_items} == {1, 2}


# ---------------------------------------------------------------------------
# ItemErrorHandler – fatal_exceptions
# ---------------------------------------------------------------------------


def test_handler_fatal_exception_returns_false():
    """Fatal exception types return False even when skip_item_errors=True."""
    handler: ItemErrorHandler[int] = ItemErrorHandler(
        skip_item_errors=True, fatal_exceptions=(FatalError,)
    )
    assert handler.on_item_error(1, FatalError("boom")) is False
    assert handler.failed_items == []


def test_handler_non_fatal_exception_still_skipped():
    """Non-fatal exception types are still skipped when fatal_exceptions is set."""
    handler: ItemErrorHandler[int] = ItemErrorHandler(
        skip_item_errors=True, fatal_exceptions=(FatalError,)
    )
    assert handler.on_item_error(1, ItemError("skippable")) is True
    assert len(handler.failed_items) == 1


def test_handler_multiple_fatal_exception_types():
    """All types in fatal_exceptions halt processing."""
    class AnotherFatal(Exception):
        pass

    handler: ItemErrorHandler[int] = ItemErrorHandler(
        skip_item_errors=True, fatal_exceptions=(FatalError, AnotherFatal)
    )
    assert handler.on_item_error(1, FatalError("a")) is False
    assert handler.on_item_error(2, AnotherFatal("b")) is False
    assert handler.failed_items == []


# ---------------------------------------------------------------------------
# ItemErrorHandler – max_consecutive_exceptions
# ---------------------------------------------------------------------------


def test_handler_raises_runtime_error_at_consecutive_threshold():
    """RuntimeError is raised when consecutive exceptions reach the threshold."""
    handler: ItemErrorHandler[int] = ItemErrorHandler(
        skip_item_errors=True, max_consecutive_exceptions=2
    )
    handler.on_item_error(0, ItemError("a"))
    with pytest.raises(RuntimeError, match="2 consecutive"):
        handler.on_item_error(1, ItemError("b"))


def test_handler_runtime_error_chains_original_exception():
    """The RuntimeError should chain the original exception as its cause."""
    handler: ItemErrorHandler[int] = ItemErrorHandler(
        skip_item_errors=True, max_consecutive_exceptions=1
    )
    original = ItemError("root cause")
    with pytest.raises(RuntimeError) as exc_info:
        handler.on_item_error(0, original)
    assert exc_info.value.__cause__ is original


def test_handler_on_item_success_resets_consecutive_count():
    """on_item_success() resets the consecutive counter."""
    handler: ItemErrorHandler[int] = ItemErrorHandler(
        skip_item_errors=True, max_consecutive_exceptions=3
    )
    handler.on_item_error(0, ItemError("a"))
    handler.on_item_error(1, ItemError("b"))
    handler.on_item_success()  # counter back to 0
    # Two more errors should not trigger the threshold (would need 3 in a row)
    handler.on_item_error(2, ItemError("c"))
    handler.on_item_error(3, ItemError("d"))
    assert len(handler.failed_items) == 4  # all recorded, none raised


def test_handler_no_consecutive_limit_by_default():
    """Without max_consecutive_exceptions, unlimited consecutive errors are tolerated."""
    handler: ItemErrorHandler[int] = ItemErrorHandler(skip_item_errors=True)
    for i in range(100):
        handler.on_item_error(i, ItemError("always fails"))
    assert len(handler.failed_items) == 100


# ---------------------------------------------------------------------------
# ItemErrorHandler – reset()
# ---------------------------------------------------------------------------


def test_handler_reset_clears_failed_items():
    """reset() clears failed_items."""
    handler: ItemErrorHandler[int] = ItemErrorHandler(skip_item_errors=True)
    handler.on_item_error(1, ItemError("x"))
    handler.on_item_error(2, ItemError("y"))
    handler.reset()
    assert handler.failed_items == []


def test_handler_reset_clears_consecutive_count():
    """reset() restarts the consecutive-exception counter."""
    handler: ItemErrorHandler[int] = ItemErrorHandler(
        skip_item_errors=True, max_consecutive_exceptions=2
    )
    handler.on_item_error(1, ItemError("a"))
    handler.reset()
    # After reset, a single error should not trigger the threshold
    handler.on_item_error(2, ItemError("b"))  # count is 1 → no RuntimeError
    assert len(handler.failed_items) == 1
