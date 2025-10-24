from typing import Iterable, Callable, Any, Optional


class Snapper:
    """
    Snapper processes an iterable with a user-defined function, saving intermediate snapshots to disk.
    This allows resuming long-running processes without losing progress.
    """

    def __init__(
        self,
        iterable: Iterable,
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

    def start(self) -> None:
        """
        Start processing the iterable, saving progress to disk.
        """
        import pickle

        processed = []
        last_index = -1
        # Try to load checkpoint
        try:
            with open(self.checkpoint_path, "rb") as f:
                checkpoint = pickle.load(f)
                last_index = checkpoint.get("last_index", -1)
                processed = checkpoint.get("processed", [])
        except (FileNotFoundError, EOFError, pickle.UnpicklingError):
            pass

        # Process from last_index + 1
        for idx, item in enumerate(self.iterable):
            if idx <= last_index:
                continue
            result = self.fn(item)
            processed.append(result)
            # Save checkpoint after each item
            with open(self.checkpoint_path, "wb") as f:
                pickle.dump({"last_index": idx, "processed": processed}, f)
            last_index = idx

    # Only one load method needed (remove duplicate)
