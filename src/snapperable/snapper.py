from typing import Iterable, Callable, Any, Optional, TypeVar, Generic
from types import TracebackType
import threading

from snapperable.storage.snapshot_storage import SnapshotStorage
from snapperable.storage.sqlite_storage import SQLiteSnapshotStorage
from snapperable.batch_processor import BatchProcessor

T = TypeVar("T")


class Snapper(Generic[T]):
    """
    Snapper processes an iterable with a user-defined function, saving intermediate snapshots to disk.
    This allows resuming long-running processes without losing progress.
    """

    # Class-level registry to track active storage file paths
    _active_storages: set[str] = set()
    _storage_lock = threading.Lock()

    def __init__(
        self,
        iterable: Iterable[T],
        fn: Callable[[T], Any],
        batch_size: int = 1,
        max_wait_time: float | None = None,
        snapshot_storage: Optional[SnapshotStorage[T]] = None,
        batch_processor: Optional[BatchProcessor] = None,
    ):
        """
        Initialize the Snapper.

        Args:
            iterable: The iterable to process.
            fn: The function to apply to each item in the iterable.
            snapshot_storage: Optional SnapshotStorage instance. Defaults to SQLite storage.
            batch_processor: Optional BatchProcessor instance. If not provided, a default one is created.
            batch_size: The number of items to batch before saving (used if batch_processor is None).
            max_wait_time: The maximum time to wait before saving a batch (used if batch_processor is None).
        
        Raises:
            ValueError: If the provided snapshot_storage is already in use by another Snapper instance.
        """
        self.iterable = iterable
        self.fn = fn

        if snapshot_storage is None:
            snapshot_storage = SQLiteSnapshotStorage()
        
        # Check if this storage file path is already in use
        storage_identifier = snapshot_storage.get_storage_identifier()
        with Snapper._storage_lock:
            if storage_identifier in Snapper._active_storages:
                raise ValueError(
                    "The provided snapshot_storage instance is already in use by another Snapper instance. "
                    "Each Snapper must have its own snapshot_storage instance to avoid race conditions."
                )
            Snapper._active_storages.add(storage_identifier)
        
        self.snapshot_storage = snapshot_storage
        self._storage_identifier = storage_identifier

        if batch_processor is None:
            batch_processor = BatchProcessor(
                storage_backend=self.snapshot_storage,
                batch_size=batch_size,
                max_wait_time=max_wait_time,
            )
        self.batch_processor = batch_processor

    def start(self) -> None:
        """
        Start processing the iterable, saving progress to disk.
        """
        last_index = self.snapshot_storage.load_last_index()

        # Process from last_index + 1
        for idx, item in enumerate(self.iterable):
            if idx <= last_index:
                continue

            result = self.fn(item)
            self.batch_processor.add_item(result)

        # Ensure all remaining items are saved
        self.batch_processor.flush()

    def load(self) -> list[T]:
        """
        Load the processed results from the snapshot storage.
        Returns:
            The list of processed results, or an empty list if no snapshot exists.
        """
        return self.snapshot_storage.load_snapshot()

    def _release_storage(self) -> None:
        """
        Release the storage instance from the active registry.
        This allows the storage to be reused by another Snapper instance.
        """
        with Snapper._storage_lock:
            Snapper._active_storages.discard(self._storage_identifier)

    def __del__(self):
        """
        Cleanup when the Snapper instance is destroyed.
        """
        # Only release if initialization completed successfully
        if hasattr(self, '_storage_identifier'):
            self._release_storage()

    def __enter__(self):
        """
        Context manager entry.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        """
        Context manager exit. Releases the storage.
        """
        self._release_storage()
        return False
