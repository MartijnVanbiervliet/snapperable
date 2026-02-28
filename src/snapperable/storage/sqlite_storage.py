"""SQLite-based snapshot storage backend."""

from pathlib import Path
import sqlite3
import pickle
import json
import os
from typing import TypeVar, Any

from snapperable.storage.snapshot_storage import SnapshotStorage
from snapperable.logger import logger
from snapperable.processing_metrics import ProcessingMetric

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
                CREATE TABLE IF NOT EXISTS processing_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    metric TEXT NOT NULL
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

    def store_snapshot(self, processed: list[T], inputs: list[Any]) -> None:
        """
        Save serialized results and corresponding inputs atomically to the database.

        Args:
            processed: The list of processed items to save.
            inputs: The list of input values corresponding to the processed items.
        """
        self._initialize_database()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Serialize and append processed results
            serialized_outputs = [(pickle.dumps(item),) for item in processed]
            cursor.executemany(
                "INSERT INTO processed_outputs (result) VALUES (?)",
                serialized_outputs,
            )

            # Serialize and append inputs
            serialized_inputs = [(pickle.dumps(item),) for item in inputs]
            cursor.executemany(
                "INSERT INTO inputs (input_value) VALUES (?)",
                serialized_inputs,
            )

            logger.debug("Stored batch of %d items to SQLite database.", len(processed))

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
                logger.debug(
                    "Loaded %d processed items from SQLite database.",
                    len(processed_items),
                )
        except sqlite3.DatabaseError:
            self._reset_database()
        return processed_items

    def load_inputs(self) -> list[Any]:
        """
        Load all stored input values.
        Returns:
            A list of input values.
        """
        inputs: list[Any] = []
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
                logger.debug("Loaded %d input items from SQLite database.", len(inputs))
        except sqlite3.DatabaseError:
            self._reset_database()
        return inputs

    def load_all_outputs(self) -> list[T]:
        """
        Load all processed outputs from storage, regardless of matching inputs.
        Returns:
            A list of all processed items.
        """
        # This is the same as load_snapshot for SQLite
        return self.load_snapshot()

    def store_metrics(self, metrics: list[ProcessingMetric]) -> None:
        """
        Save per-item processing metrics to the database as JSON strings.

        Args:
            metrics: The list of ProcessingMetric instances to save.
        """
        self._initialize_database()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            serialized = [(json.dumps(m.to_dict()),) for m in metrics]
            cursor.executemany(
                "INSERT INTO processing_metrics (metric) VALUES (?)",
                serialized,
            )
            logger.debug("Stored %d metric(s) to SQLite database.", len(metrics))

    def load_metrics(self) -> list[ProcessingMetric]:
        """
        Load all stored per-item processing metrics.
        Returns:
            A list of ProcessingMetric instances.
        """
        result: list[ProcessingMetric] = []
        try:
            self._initialize_database()
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT metric FROM processing_metrics ORDER BY id")
                rows = cursor.fetchall()
                for row in rows:
                    try:
                        result.append(ProcessingMetric.from_dict(json.loads(row[0])))
                    except (json.JSONDecodeError, KeyError, TypeError):
                        logger.warning("Corrupted metric data encountered and skipped.")
                logger.debug("Loaded %d metric(s) from SQLite database.", len(result))
        except sqlite3.DatabaseError:
            self._reset_database()
        return result
