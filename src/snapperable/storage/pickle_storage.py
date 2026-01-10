"""Pickle-based snapshot storage backend."""

import pickle
import os
from typing import TypeVar, Any

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
        # Load existing data
        data = self._load_data()
        existing_processed = data.get("processed", [])
        combined_processed = existing_processed + processed

        # Save the combined data
        data["last_index"] = last_index
        data["processed"] = combined_processed
        self._save_data(data)

    def load_snapshot(self) -> list[T]:
        """
        Load all processed results from the pickle file.

        Returns:
            A list of processed items.
        """
        data = self._load_data()
        return data.get("processed", [])

    def load_last_index(self) -> int:
        """
        Load the last processed index from the pickle file.

        Returns:
            The last processed index, or -1 if not available.
        """
        data = self._load_data()
        return data.get("last_index", -1)

    def store_input(self, input_value: Any) -> None:
        """
        Store an input value.
        Args:
            input_value: The input value to store.
        """
        data = self._load_data()
        inputs = data.get("inputs", [])
        inputs.append(input_value)
        data["inputs"] = inputs
        self._save_data(data)

    def load_inputs(self) -> list[Any]:
        """
        Load all stored input values.
        Returns:
            A list of input values.
        """
        data = self._load_data()
        return data.get("inputs", [])

    def load_all_outputs(self) -> list[T]:
        """
        Load all processed outputs from storage, regardless of matching inputs.
        Returns:
            A list of all processed items.
        """
        # This is the same as load_snapshot for Pickle
        return self.load_snapshot()

    def _load_data(self) -> dict:
        """
        Load all data from the pickle file.
        Returns:
            A dictionary containing all stored data.
        """
        try:
            with open(self.file_path, "rb") as f:
                return pickle.load(f)
        except (FileNotFoundError, pickle.UnpicklingError, EOFError):
            logger.warning(f"Pickle file '{self.file_path}' is corrupted or missing.")
            return {}

    def _save_data(self, data: dict) -> None:
        """
        Save all data to the pickle file.
        Args:
            data: A dictionary containing all data to store.
        """
        with open(self.file_path, "wb") as f:
            pickle.dump(data, f)
