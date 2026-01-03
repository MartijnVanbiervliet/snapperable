"""SQLite-based snapshot storage backend."""

from pathlib import Path
import sqlite3
import pickle
import os
from typing import TypeVar

from snapperable.storage.snapshot_storage import SnapshotStorage
from snapperable.logger import logger

T = TypeVar("T")


class SQLiteSnapshotStorage(SnapshotStorage[T]):
    def __init__(self, db_path: Path | str = "snapper_checkpoint.db"):
        """
        Initialize the SQLite checkpoint manager.

        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = str(db_path)  # Normalize to string

    def get_storage_identifier(self) -> str:
        """
        Get a unique identifier for this storage backend.
        Returns the absolute path to the database file.
        """
        return os.path.abspath(self.db_path)

    def _initialize_database(self) -> None:
        """Create tables if they do not exist."""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
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
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS inputs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    input_value BLOB NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS function_version (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version TEXT NOT NULL
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
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self._initialize_database()

    def store_snapshot(self, last_index: int, processed: list[T]) -> None:
        """
        Save the last processed index and append serialized results to the database.

        Args:
            last_index: The last processed index.
            processed: The list of processed items to save.
        """
        self._initialize_database()
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
        processed_items: list[T] = []
        try:
            self._initialize_database()
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

    def store_input(self, input_value: any) -> None:
        """
        Store an input value.
        Args:
            input_value: The input value to store.
        """
        self._initialize_database()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            serialized_input = pickle.dumps(input_value)
            cursor.execute(
                "INSERT INTO inputs (input_value) VALUES (?)",
                (serialized_input,)
            )
            conn.commit()

    def load_inputs(self) -> list[any]:
        """
        Load all stored input values.
        Returns:
            A list of input values.
        """
        inputs: list[any] = []
        try:
            self._initialize_database()
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT input_value FROM inputs ORDER BY id")
                rows = cursor.fetchall()
                for row in rows:
                    try:
                        inputs.append(pickle.loads(row[0]))
                    except (pickle.UnpicklingError, EOFError):
                        logger.warning("Corrupted input data encountered and skipped.")
        except sqlite3.DatabaseError:
            self._reset_database()
        return inputs

    def store_function_version(self, fn_version: str) -> None:
        """
        Store the function version (hash).
        Args:
            fn_version: The function version string.
        """
        self._initialize_database()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Clear existing version and store new one
            cursor.execute("DELETE FROM function_version")
            cursor.execute(
                "INSERT INTO function_version (version) VALUES (?)",
                (fn_version,)
            )
            conn.commit()

    def load_function_version(self) -> str | None:
        """
        Load the stored function version.
        Returns:
            The function version string, or None if not available.
        """
        try:
            self._initialize_database()
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT version FROM function_version ORDER BY id DESC LIMIT 1"
                )
                row = cursor.fetchone()
                return row[0] if row else None
        except sqlite3.DatabaseError:
            self._reset_database()
            return None

    def load_all_outputs(self) -> list[T]:
        """
        Load all processed outputs from storage, regardless of matching inputs.
        Returns:
            A list of all processed items.
        """
        # This is the same as load_snapshot for SQLite
        return self.load_snapshot()
