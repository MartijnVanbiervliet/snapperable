from typing import Any, List
import threading

from snapperable.snapshot_storage import SnapshotStorage
from snapperable.logger import logger


class BatchProcessor:
    """
    Handles batching of items and delegates processing to a storage backend.
    """

    def __init__(
        self,
        storage_backend: SnapshotStorage[Any],
        batch_size: int,
        max_wait_time: float | None = None,
    ):
        """
        Initialize the BatchProcessor.

        Args:
            storage_backend: The storage backend to delegate processing to.
            batch_size: The number of items to batch before processing.
            max_wait_time: The maximum time to wait before processing a batch. If None, no time limit is enforced.
        """
        self.storage_backend = storage_backend
        self.batch_size = batch_size
        self.max_wait_time = max_wait_time
        self.current_batch: List[Any] = []
        self.timer = None
        self.lock = threading.Lock()

    def add_item(self, item: Any) -> None:
        """
        Add an item to the current batch. If the batch is full or the maximum wait time is exceeded,
        the batch is flushed.

        Args:
            item: The item to be added to the batch.
        """
        logger.debug("Adding item to batch: %s", item)
        should_flush = False
        with self.lock:
            self.current_batch.append(item)
            logger.debug("Current batch size: %d", len(self.current_batch))
            if self._is_batch_full():
                logger.info("Batch is full. Triggering flush.")
                should_flush = True
            elif self._is_wait_time_exceeded():
                logger.info("Wait time exceeded. Triggering flush.")
                should_flush = True
            elif self.timer is None:
                logger.debug("Starting timer for batch processing.")
                self.start_timer()

        if should_flush:
            self.flush()

    def flush(self) -> None:
        """
        Flush the current batch by storing it using the storage backend. Clears the batch and stops the timer.
        """
        logger.info("Flushing current batch.")
        batch_to_store = None
        with self.lock:
            if self.current_batch:
                batch_to_store = self.current_batch
                self.current_batch = []
                self.stop_timer()
                logger.debug("Batch cleared after flush.")

        if batch_to_store:
            logger.info("Storing batch of size %d.", len(batch_to_store))
            last_index = self.storage_backend.load_last_index() + len(batch_to_store)
            self.storage_backend.store_snapshot(last_index, batch_to_store)
            logger.debug("Batch stored with last index: %d", last_index)

    def _is_wait_time_exceeded(self) -> bool:
        """
        Check if the maximum wait time has been exceeded.

        Returns:
            True if the wait time has been exceeded, False otherwise.
        """
        if self.max_wait_time is None:
            return False
        return self.timer is not None and not self.timer.is_alive()

    def _is_batch_full(self) -> bool:
        """
        Check if the current batch is full.

        Returns:
            True if the batch size has been reached, False otherwise.
        """
        return len(self.current_batch) >= self.batch_size

    def start_timer(self) -> None:
        logger.debug(
            "Starting a new timer with max wait time: %s seconds.", self.max_wait_time
        )
        self.timer = threading.Timer(self.max_wait_time, self.flush)
        self.timer.start()

    def stop_timer(self) -> None:
        logger.debug("Stopping the timer.")
        if self.timer:
            self.timer.cancel()
            self.timer = None
