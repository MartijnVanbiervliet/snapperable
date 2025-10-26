from typing import Iterable, Callable, Any, Optional, TypeVar, Generic
from checkpoint_manager import CheckpointManager, SqlLiteCheckpointManager


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
        checkpoint_manager: Optional[CheckpointManager] = None,
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
        if checkpoint_manager is None:
            checkpoint_manager = SqlLiteCheckpointManager()
        self.checkpoint_manager = checkpoint_manager

    def _save_checkpoint(self, last_index: int, processed: list[T]) -> None:
        """
        Save checkpoint using the checkpoint manager.
        """
        self.checkpoint_manager.save_checkpoint(last_index, processed=processed)

    def start(self) -> None:
        """
        Start processing the iterable, saving progress to disk.
        """
        last_index = self.checkpoint_manager.load_last_index()
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
        return self.checkpoint_manager.load_checkpoint()
