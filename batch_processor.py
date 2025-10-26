from typing import Any, List, TypeVar
import threading

from snapshot_storage import SnapshotStorage

T = TypeVar("T")


class BatchProcessor:
    """
    Handles batching of items and delegates processing to a storage backend.
    """

    def __init__(
        self, storage_backend: SnapshotStorage[T], batch_size: int, max_wait_time: float
    ):
        """
        Initialize the BatchProcessor.

        Args:
            storage_backend: The storage backend to delegate processing to.
            batch_size: The number of items to batch before processing.
            max_wait_time: The maximum time to wait before processing a batch.
        """
        self.storage_backend = storage_backend
        self.batch_size = batch_size
        self.max_wait_time = max_wait_time
        self.current_batch: List[Any] = []
        self.timer = None
        self.lock = threading.Lock()

    def add_item(self, item: Any) -> None:
        """
        Add an item to the batch. If the batch size is reached, process the batch.

        Args:
            item: The item to add to the batch.
        """
        with self.lock:
            self.current_batch.append(item)
            if len(self.current_batch) >= self.batch_size:
                self.flush()
            elif self.timer is None:
                self.start_timer()

    def flush(self) -> None:
        """
        Process the current batch immediately.
        """
        with self.lock:
            if self.current_batch:
                self.storage_backend.store_snapshot(
                    len(self.current_batch), self.current_batch
                )
                self.current_batch = []
                self.stop_timer()

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
