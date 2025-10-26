from typing import Iterable, Callable, Any, Optional, TypeVar, Generic
from snapshot_storage import SnapshotStorage, SqlLiteSnapshotStorage


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
        snapshot_storage: Optional[SnapshotStorage[T]] = None,
    ):
        """
        Initialize the Snapper.

        Args:
            iterable: The iterable to process.
            fn: The function to apply to each item in the iterable.
            checkpoint_manager: Optional CheckpointManager instance. If not provided, raises error.
        """
        self.iterable = iterable
        self.fn = fn
        if snapshot_storage is None:
            snapshot_storage = SqlLiteSnapshotStorage()
        self.snapshot_storage = snapshot_storage

    def _save_checkpoint(self, last_index: int, processed: list[T]) -> None:
        """
        Save checkpoint using the checkpoint manager.
        """
        self.snapshot_storage.store_snapshot(last_index, processed=processed)

    def start(self) -> None:
        """
        Start processing the iterable, saving progress to disk.
        """
        last_index = self.snapshot_storage.load_last_index()
        processed: list[T] = []

        # Process from last_index + 1
        for idx, item in enumerate(self.iterable):
            if idx <= last_index:
                continue
            result = self.fn(item)
            processed.append(result)
            # Save checkpoint after each item
            self._save_checkpoint(idx, processed)
            processed = []
            last_index = idx

    def load(self) -> list[T]:
        """
        Load the processed results from the checkpoint manager.
        Returns:
            The list of processed results, or an empty list if no checkpoint exists.
        """
        return self.snapshot_storage.load_snapshot()
