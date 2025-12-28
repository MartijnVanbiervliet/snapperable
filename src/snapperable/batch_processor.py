from typing import Any, List
import time
import queue
import threading

from snapperable.storage.snapshot_storage import SnapshotStorage
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
        self.last_flush_time = None
        
        # Thread-safe queue for background saving
        self._save_queue: queue.Queue[tuple[int, List[Any]] | None] = queue.Queue()
        self._worker_thread = threading.Thread(target=self._save_worker, daemon=True)
        self._worker_thread.start()
        self._shutdown = False
        
        # Track the current index (number of items processed so far)
        # Note: This is safe because each BatchProcessor is used by a single Snapper instance,
        # and Snapper prevents multiple instances from sharing the same storage backend
        self._current_index = self.storage_backend.load_last_index()

    def add_item(self, item: Any) -> None:
        """
        Add an item to the current batch. If the batch is full or the maximum wait time is exceeded,
        the batch is flushed.

        Args:
            item: The item to be added to the batch.
        """
        logger.debug("Adding item to batch: %s", item)
        should_flush = False
        self.current_batch.append(item)
        logger.debug("Current batch size: %d", len(self.current_batch))

        # Initialize last flush time if it's the first item
        if self.last_flush_time is None:
            self._update_last_flush_time()

        if self._is_wait_time_exceeded():
            logger.info("Wait time exceeded. Triggering flush.")
            should_flush = True

        if self._is_batch_full():
            logger.info("Batch is full. Triggering flush.")
            should_flush = True

        if should_flush:
            self.flush()

    def flush(self) -> None:
        """
        Flush the current batch by enqueueing it for background saving. Clears the batch.
        """
        logger.info("Flushing current batch.")
        batch_to_store = None
        if self.current_batch:
            batch_to_store = self.current_batch
            self.current_batch = []
            logger.debug("Batch cleared after flush.")

        if batch_to_store:
            logger.info("Enqueueing batch of size %d for background saving.", len(batch_to_store))
            # Calculate the new index based on current index + batch size
            self._current_index += len(batch_to_store)
            # Enqueue the batch for background saving
            self._save_queue.put((self._current_index, batch_to_store))
            self._update_last_flush_time()
            logger.debug("Batch enqueued with last index: %d", self._current_index)

    def _save_worker(self) -> None:
        """
        Background worker thread that processes the save queue.
        Runs continuously until a sentinel value (None) is received.
        """
        while True:
            item = self._save_queue.get()
            if item is None:
                # Sentinel value to stop the worker
                self._save_queue.task_done()
                break
            
            last_index, batch = item
            try:
                logger.info("Background thread storing batch of size %d.", len(batch))
                self.storage_backend.store_snapshot(last_index, batch)
                logger.debug("Background thread stored batch with last index: %d", last_index)
            except Exception as e:
                logger.error("Error storing snapshot in background thread: %s", e)
            finally:
                self._save_queue.task_done()

    def shutdown(self) -> None:
        """
        Gracefully shutdown the background worker thread.
        Waits for all queued items to be processed before stopping.
        """
        if self._shutdown:
            return
        
        logger.info("Shutting down BatchProcessor background worker.")
        # Send sentinel value to stop the worker
        self._save_queue.put(None)
        # Wait for all queued items to be processed
        self._save_queue.join()
        # Wait for worker thread to finish
        self._worker_thread.join()
        self._shutdown = True
        logger.info("BatchProcessor background worker shutdown complete.")


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
