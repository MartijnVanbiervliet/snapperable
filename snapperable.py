from typing import Iterable, Callable, Any, Optional
import pickle


class Snapper:
    """
    Snapper processes an iterable with a user-defined function, saving intermediate snapshots to disk.
    This allows resuming long-running processes without losing progress.
    """

    T = Any

    def __init__(
        self,
        iterable: Iterable[T],
        fn: Callable[[Any], Any],
        checkpoint_path: Optional[str] = None,
    ):
        """
        Initialize the Snapper.

        Args:
            iterable: The iterable to process.
            fn: The function to apply to each item in the iterable.
            checkpoint_path: Optional path to save checkpoints. If not provided, a default will be used.
        """
        self.iterable = iterable
        self.fn = fn
        self.checkpoint_path = checkpoint_path or "snapperable.chkpt"

    def _load_checkpoint(self) -> tuple[int, list[Any]]:
        """
        Load checkpoint from file. Returns (last_index, processed).
        """
        try:
            with open(self.checkpoint_path, "rb") as f:
                checkpoint = pickle.load(f)
                last_index = checkpoint.get("last_index", -1)
                processed = checkpoint.get("processed", [])
                return last_index, processed
        except (FileNotFoundError, EOFError, pickle.UnpicklingError):
            return -1, []

    def _save_checkpoint(self, last_index: int, processed: list[Any]) -> None:
        """
        Save checkpoint to file.
        """
        with open(self.checkpoint_path, "wb") as f:
            pickle.dump({"last_index": last_index, "processed": processed}, f)

    def start(self) -> None:
        """
        Start processing the iterable, saving progress to disk.
        """
        last_index, processed = self._load_checkpoint()

        # Process from last_index + 1
        for idx, item in enumerate(self.iterable):
            if idx <= last_index:
                continue
            result = self.fn(item)
            processed.append(result)
            # Save checkpoint after each item
            self._save_checkpoint(idx, processed)
            last_index = idx

    def load(self) -> list[T]:
        """
        Load the processed results from the checkpoint file.
        Returns:
            The list of processed results, or an empty list if no checkpoint exists.
        """
        try:
            with open(self.checkpoint_path, "rb") as f:
                checkpoint = pickle.load(f)
                return checkpoint.get("processed", [])
        except (FileNotFoundError, EOFError, pickle.UnpicklingError):
            return []
