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

    def store_snapshot(self, processed: list[T], inputs: list[Any]) -> None:
        """
        Save processed results and corresponding inputs atomically.
        This method ensures that existing processed items and inputs are loaded and appended before saving.

        Args:
            processed: The list of processed items to save.
            inputs: The list of input values corresponding to the processed items.
        """
        # Load existing data
        data = self._load_data()
        existing_processed = data.get("processed", [])
        existing_inputs = data.get("inputs", [])

        # Append new data
        combined_processed = existing_processed + processed
        combined_inputs = existing_inputs + inputs

        # Save the combined data atomically
        data["processed"] = combined_processed
        data["inputs"] = combined_inputs
        self._save_data(data)

    def load_snapshot(self) -> list[T]:
        """
        Load all processed results from the pickle file.

        Returns:
            A list of processed items.
        """
        data = self._load_data()
        return data.get("processed", [])

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
        Save all data to the pickle file atomically.

        Uses a temporary file and atomic rename to ensure data is not corrupted
        if the process crashes during the write operation.

        Args:
            data: A dictionary containing all data to store.
        """
        # Write to a temporary file first
        temp_path = str(self.file_path) + ".tmp"
        with open(temp_path, "wb") as f:
            pickle.dump(data, f)

        # Atomically replace the original file
        # os.replace() is atomic on both Unix and Windows
        os.replace(temp_path, self.file_path)
