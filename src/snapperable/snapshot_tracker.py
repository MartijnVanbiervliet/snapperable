"""Tracks which inputs have been processed and determines remaining items."""

from typing import Iterable, Any, TypeVar

from snapperable.storage.snapshot_storage import SnapshotStorage

T = TypeVar("T")


class SnapshotTracker:
    """
    Tracks processed inputs and determines which items remain to be processed.

    This class manages the state of processed items by comparing the current iterable
    against stored inputs.
    """

    def __init__(self, iterable: Iterable[T], snapshot_storage: SnapshotStorage[T]):
        """
        Initialize the SnapshotTracker.

        Args:
            iterable: The iterable containing items to process.
            snapshot_storage: The storage backend for tracking processed items.
        """
        self.iterable = iterable
        self.snapshot_storage = snapshot_storage
        self._processed_inputs_set: set[Any] = set()
        self._initialized = False

    @staticmethod
    def _make_hashable(obj: Any) -> Any:
        """
        Convert an object to a hashable representation.

        Args:
            obj: The object to make hashable.

        Returns:
            A hashable representation of the object.
        """
        if isinstance(obj, (list, tuple)):
            return tuple(SnapshotTracker._make_hashable(item) for item in obj)
        elif isinstance(obj, dict):
            return tuple(
                sorted((k, SnapshotTracker._make_hashable(v)) for k, v in obj.items())
            )
        elif isinstance(obj, set):
            return frozenset(SnapshotTracker._make_hashable(item) for item in obj)
        else:
            return obj

    def _initialize(self) -> None:
        """
        Initialize the tracker by loading stored inputs.
        """
        if self._initialized:
            return

        # Load previously stored inputs
        stored_inputs = self.snapshot_storage.load_inputs()

        # Create a hashable representation of stored inputs
        for inp in stored_inputs:
            try:
                self._processed_inputs_set.add(SnapshotTracker._make_hashable(inp))
            except TypeError:
                # If input is not hashable, we'll process it again
                pass

        self._initialized = True

    def get_remaining(self) -> Iterable[T]:
        """
        Get the remaining items from the iterable that haven't been processed yet.

        Yields:
            Items from the iterable that haven't been processed.
        """
        self._initialize()

        for item in self.iterable:
            # Check if this input was already processed
            try:
                hashable_item = SnapshotTracker._make_hashable(item)
                if hashable_item in self._processed_inputs_set:
                    continue
            except TypeError:
                # If item is not hashable, process it
                pass

            yield item

    def mark_processed(self, item: T) -> None:
        """
        Mark an item as processed.

        Args:
            item: The item that has been processed.
        """
        try:
            hashable_item = SnapshotTracker._make_hashable(item)
            self._processed_inputs_set.add(hashable_item)
        except TypeError:
            # If not hashable, we can't track it
            pass
