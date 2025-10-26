from typing import Iterable, Callable, Any, Optional, TypeVar, Generic
from snapshot_storage import SnapshotStorage, SqlLiteSnapshotStorage
from batch_processor import BatchProcessor

T = TypeVar("T")


class Snapper(Generic[T]):
    """
    Snapper processes an iterable with a user-defined function, saving intermediate snapshots to disk.
    This allows resuming long-running processes without losing progress.
    """

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
        """
        self.iterable = iterable
        self.fn = fn

        if snapshot_storage is None:
            snapshot_storage = SqlLiteSnapshotStorage()
        self.snapshot_storage = snapshot_storage

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
