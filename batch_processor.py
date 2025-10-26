from typing import Any, List
import threading

from snapshot_storage import SnapshotStorage


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

    def add_item(self, item: Any) -> None:
        """
        Add an item to the batch. If the batch size is reached, process the batch.

        Args:
            item: The item to add to the batch.
        """
        should_flush = False
        with self.lock:
            self.current_batch.append(item)
            if self._is_batch_full() or self._is_wait_time_exceeded():
                should_flush = True
            elif self.timer is None:
                self.start_timer()

        # Call flush outside the lock to avoid recursive locking
        if should_flush:
            self.flush()

    def flush(self) -> None:
        """
        Process the current batch immediately.
        """
        batch_to_store = None
        with self.lock:
            if self.current_batch:
                batch_to_store = self.current_batch
                self.current_batch = []
                self.stop_timer()

        # Perform storage operation outside the lock
        if batch_to_store:
            last_index = self.storage_backend.load_last_index() + len(batch_to_store)
            self.storage_backend.store_snapshot(last_index, batch_to_store)

    def start_timer(self) -> None:
        """
        Start a timer to process the batch after the maximum wait time.
        """
        self.timer = threading.Timer(self.max_wait_time, self.flush)
        self.timer.start()

    def stop_timer(self) -> None:
        """
        Stop the timer if it is running.
        """
        if self.timer:
            self.timer.cancel()
            self.timer = None
