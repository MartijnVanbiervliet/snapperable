from pathlib import Path
import sqlite3
import pickle
from typing import TypeVar, Generic
import os
from snapperable.logger import logger

T = TypeVar("T")


class SnapshotStorage(Generic[T]):
    """
    Abstracts checkpoint saving/loading for Snapper.
    This class can be extended to support different backends (file, database, etc).
    """

    def store_snapshot(self, last_index: int, processed: list[T]) -> None:
        """
        Save snapshot to storage.
        Args:
            last_index: The last processed index.
            processed: The list of processed items to save.
        """
        raise NotImplementedError

    def load_snapshot(self) -> list[T]:
        """
        Load snapshot state.
        Returns:
            A list of processed items.
        """
        raise NotImplementedError

    def load_last_index(self) -> int:
        """
        Load only the last processed index, without loading the full processed results.
        Returns:
            The last processed index, or -1 if not available.
        """
        raise NotImplementedError


class SqlLiteSnapshotStorage(SnapshotStorage[T]):
    def __init__(self, db_path: Path | str = "snapper_checkpoint.db"):
        """
        Initialize the SQLite checkpoint manager.

        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = db_path
        self._initialize_database()

    def _initialize_database(self) -> None:
        """Create tables if they do not exist."""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS checkpoints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    last_index INTEGER NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_outputs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    result BLOB NOT NULL
                )
                """
            )
            conn.commit()

    def _reset_database(self):
        """
        Reset the database by reinitializing the schema.
        This is used when the database file is corrupted.
        """
        logger.warning("Database file is corrupted. Resetting the database.")
        self._initialize_database()

    def store_snapshot(self, last_index: int, processed: list[T]) -> None:
        """
        Save the last processed index and append serialized results to the database.

        Args:
            last_index: The last processed index.
            processed: The list of processed items to save.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Update the last index
            cursor.execute("DELETE FROM checkpoints")
            cursor.execute(
                "INSERT INTO checkpoints (last_index) VALUES (?)", (last_index,)
            )

            # Serialize and append processed results
            serialized_data = [(pickle.dumps(item),) for item in processed]
            cursor.executemany(
                "INSERT INTO processed_outputs (result) VALUES (?)",
                serialized_data,
            )
            conn.commit()

    def load_snapshot(self) -> list[T]:
        """
        Load all processed results from the database and deserialize them.

        Returns:
            A list of processed items.
        """
        processed_items = []
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT result FROM processed_outputs")
                rows = cursor.fetchall()
                for row in rows:
                    try:
                        processed_items.append(pickle.loads(row[0]))
                    except (pickle.UnpicklingError, EOFError):
                        logger.warning("Corrupted data encountered and skipped.")
        except sqlite3.DatabaseError:
            self._reset_database()
        return processed_items

    def load_last_index(self) -> int:
        """
        Load the last processed index from the database.

        Returns:
            The last processed index, or -1 if not available.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT last_index FROM checkpoints ORDER BY id DESC LIMIT 1"
                )
                row = cursor.fetchone()
                return row[0] if row else -1
        except sqlite3.DatabaseError:
            self._reset_database()
            return -1


class PickleSnapshotStorage(SnapshotStorage[T]):
    def __init__(self, file_path: str = "snapper_checkpoint.pkl"):
        """
        Initialize the Pickle snapshot storage.

        Args:
            file_path: Path to the pickle file.
        """
        self.file_path = file_path

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
