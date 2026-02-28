"""Dedicated class for tracking per-item processing failures."""

from typing import Generic, TypeVar

T = TypeVar("T")


class FailedItem(Generic[T]):
    """
    Holds information about an item that failed processing.

    Attributes:
        item: The input item that caused the exception.
        exception: The exception raised when processing the item.
    """

    def __init__(self, item: T, exception: Exception) -> None:
        self.item = item
        self.exception = exception

    def __repr__(self) -> str:
        return f"FailedItem(item={self.item!r}, exception={self.exception!r})"


class ItemErrorHandler(Generic[T]):
    """
    Manages per-item exception tracking and policy enforcement during iterable processing.

    The Snapper main loop notifies this handler after each item succeeds or fails.
    The handler maintains the failure record and decides—based on the configured policy—
    whether a given exception should be skipped or should halt processing.
    """

    def __init__(
        self,
        skip_item_errors: bool = False,
        fatal_exceptions: tuple[type[Exception], ...] = (),
        max_consecutive_exceptions: int | None = None,
    ) -> None:
        """
        Initialize the ItemErrorHandler.

        Args:
            skip_item_errors: When True, non-fatal exceptions are caught, recorded, and
                the item is skipped so that processing continues. When False (the default),
                all exceptions propagate immediately.
            fatal_exceptions: Exception types that should always halt processing immediately,
                even when skip_item_errors=True.
            max_consecutive_exceptions: If not None, halt processing when this many item-level
                exceptions occur consecutively (resets to zero after any successful item).
                Only active when skip_item_errors=True.
        """
        self.skip_item_errors = skip_item_errors
        self.fatal_exceptions = tuple(fatal_exceptions)
        self.max_consecutive_exceptions = max_consecutive_exceptions
        self.failed_items: list[FailedItem[T]] = []
        self._consecutive_count: int = 0

    def reset(self) -> None:
        """Reset all state in preparation for a new processing run."""
        self.failed_items = []
        self._consecutive_count = 0

    def on_item_success(self) -> None:
        """Notify the handler that an item was processed successfully.

        Resets the consecutive-exception counter.
        """
        self._consecutive_count = 0

    def on_item_error(self, item: T, exc: Exception) -> bool:
        """
        Notify the handler that processing an item raised an exception.

        Returns:
            True if the item should be skipped and processing should continue.
            False if the caller should re-raise the original exception unchanged.

        Raises:
            RuntimeError: If the consecutive-exception threshold is reached
                (only when skip_item_errors=True and max_consecutive_exceptions is set).
        """
        # Fatal exceptions always propagate, regardless of skip_item_errors
        if self.fatal_exceptions and isinstance(exc, self.fatal_exceptions):
            return False

        # If item-error skipping is disabled, propagate all exceptions
        if not self.skip_item_errors:
            return False

        self._consecutive_count += 1

        if (
            self.max_consecutive_exceptions is not None
            and self._consecutive_count >= self.max_consecutive_exceptions
        ):
            raise RuntimeError(
                f"Processing halted after {self._consecutive_count} consecutive "
                f"exception(s). Last exception: {exc!r}"
            ) from exc

        # Record the failed item and indicate it should be skipped
        self.failed_items.append(FailedItem(item=item, exception=exc))
        return True
