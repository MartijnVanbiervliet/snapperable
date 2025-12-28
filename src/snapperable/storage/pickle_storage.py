"""Pickle-based snapshot storage backend."""

import pickle
import os
from typing import TypeVar

from snapperable.storage.snapshot_storage import SnapshotStorage
from snapperable.logger import logger

T = TypeVar("T")


class PickleSnapshotStorage(SnapshotStorage[T]):
    def __init__(self, file_path: str = "snapper_checkpoint.pkl"):
        """
        Initialize the Pickle snapshot storage.

        Args:
            file_path: Path to the pickle file.
        """
        self.file_path = file_path

    def get_storage_identifier(self) -> str:
        """
        Get a unique identifier for this storage backend.
        Returns the absolute path to the pickle file.
        """
        return os.path.abspath(self.file_path)

    def store_snapshot(self, last_index: int, processed: list[T]) -> None:
        """
        Save the last processed index and all processed results to a pickle file.
        This method ensures that existing processed items are loaded and appended before saving.

        Args:
            last_index: The last processed index.
            processed: The list of processed items to save.
        """
        # Load existing processed items
        existing_processed = self.load_snapshot()
        combined_processed = existing_processed + processed

        # Save the combined data
        with open(self.file_path, "wb") as f:
            pickle.dump({"last_index": last_index, "processed": combined_processed}, f)

    def load_snapshot(self) -> list[T]:
        """
        Load all processed results from the pickle file.

        Returns:
            A list of processed items.
        """
        try:
            with open(self.file_path, "rb") as f:
                data = pickle.load(f)
                return data.get("processed", [])
        except (FileNotFoundError, pickle.UnpicklingError, EOFError):
            logger.warning(f"Pickle file '{self.file_path}' is corrupted or missing.")
            return []

    def load_last_index(self) -> int:
        """
        Load the last processed index from the pickle file.

        Returns:
            The last processed index, or -1 if not available.
        """
        try:
            with open(self.file_path, "rb") as f:
                data = pickle.load(f)
                return data.get("last_index", -1)
        except (FileNotFoundError, pickle.UnpicklingError, EOFError):
            logger.warning(f"Pickle file '{self.file_path}' is corrupted or missing.")
            return -1
