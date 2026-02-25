from typing import Any, List, Tuple
import time

from snapperable.storage.snapshot_storage import SnapshotStorage
from snapperable.batch_storage_worker import BatchStorageWorker
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
        max_retries: int = 3,
    ):
        """
        Initialize the BatchProcessor.

        Args:
            storage_backend: The storage backend to delegate processing to.
            batch_size: The number of items to batch before processing.
            max_wait_time: The maximum time to wait before processing a batch. If None, no time limit is enforced.
            max_retries: Maximum number of retry attempts for failed storage operations. Default is 3.
        """
        self.storage_backend = storage_backend
        self.batch_size = batch_size
        self.max_wait_time = max_wait_time
        self.current_batch: List[Tuple[Any, Any]] = []  # List of (input, output) tuples
        self.last_flush_time = None

        # Delegate background storage to BatchStorageWorker
        self._storage_worker = BatchStorageWorker(
            storage_backend, max_retries=max_retries
        )

    def add_item(self, item: Any, input_value: Any) -> None:
        """
        Add an item to the current batch. If the batch is full or the maximum wait time is exceeded,
        the batch is flushed.

        Args:
            item: The output item to be added to the batch.
            input_value: The corresponding input value for input-based tracking.
        """
        logger.debug(
            "Adding item to batch: input_value=%s, output_value=%s", input_value, item
        )
        should_flush = False
        self.current_batch.append((input_value, item))
        logger.debug("Current batch size: %d", len(self.current_batch))

        # Initialize last flush time if it's the first item
        if self.last_flush_time is None:
            self._update_last_flush_time()

        if self._is_wait_time_exceeded():
            logger.debug("Wait time exceeded. Triggering flush.")
            should_flush = True

        if self._is_batch_full():
            logger.debug("Batch is full. Triggering flush.")
            should_flush = True

        if should_flush:
            self.flush()

    def flush(self) -> None:
        """
        Flush the current batch by enqueueing it for background saving. Clears the batch.
        """
        logger.debug("Flushing current batch.")
        batch_to_store = None
        if self.current_batch:
            batch_to_store = self.current_batch
            self.current_batch = []
            logger.debug("Batch cleared after flush.")

        if batch_to_store:
            # Separate inputs and outputs
            inputs = [inp for inp, _ in batch_to_store]
            outputs = [out for _, out in batch_to_store]

            # Delegate to storage worker for background saving
            self._storage_worker.enqueue_batch(outputs, inputs)
            self._update_last_flush_time()

    def shutdown(self) -> None:
        """
        Gracefully shutdown the background worker thread.
        Waits for all queued items to be processed before stopping.
        """
        self._storage_worker.shutdown()

    def _is_wait_time_exceeded(self) -> bool:
        """
        Check if the maximum wait time has been exceeded.

        Returns:
            True if the wait time has been exceeded, False otherwise.
        """
        if self.max_wait_time is None:
            return False

        if self.last_flush_time is None:
            return False

        # Check if the last item addition was beyond the max wait time threshold
        current_time = time.time()
        return (current_time - self.last_flush_time) > self.max_wait_time

    def _update_last_flush_time(self) -> None:
        """
        Update the last flush time to the current time.
        """
        self.last_flush_time = time.time()

    def _is_batch_full(self) -> bool:
        """
        Check if the current batch is full.

        Returns:
            True if the batch size has been reached, False otherwise.
        """
        return len(self.current_batch) >= self.batch_size
