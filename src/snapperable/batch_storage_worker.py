"""Background worker for asynchronous batch storage."""

from typing import Any, List
import queue
import threading

from snapperable.storage.snapshot_storage import SnapshotStorage
from snapperable.logger import logger


class BatchStorageWorker:
    """
    Manages background thread for asynchronous storage of batches.
    
    This worker runs a dedicated thread that consumes from a queue and
    delegates storage operations to a backend, allowing the main processing
    loop to continue without blocking on I/O.
    """

    def __init__(self, storage_backend: SnapshotStorage[Any]):
        """
        Initialize the BatchStorageWorker.

        Args:
            storage_backend: The storage backend to delegate saving to.
        """
        self.storage_backend = storage_backend
        self._save_queue: queue.Queue[tuple[List[Any], List[Any]] | None] = queue.Queue()
        self._worker_thread = threading.Thread(target=self._save_worker, daemon=True)
        self._shutdown = False
        self._shutdown_lock = threading.Lock()
        self._worker_thread.start()

    def enqueue_batch(self, outputs: List[Any], inputs: List[Any]) -> None:
        """
        Enqueue a batch for background saving.

        Args:
            outputs: The list of processed outputs to save.
            inputs: The list of corresponding inputs.
            
        Raises:
            RuntimeError: If called after shutdown() has been invoked.
        """
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError(
                    "Cannot enqueue batch after BatchStorageWorker has been shut down. "
                    "Items will not be processed."
                )
        
        logger.info("Enqueueing batch of size %d for background saving.", len(outputs))
        self._save_queue.put((outputs, inputs))
        logger.debug("Batch enqueued.")

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
            
            outputs, inputs = item
            try:
                logger.info("Background thread storing batch of size %d.", len(outputs))
                self.storage_backend.store_snapshot(outputs, inputs)
                logger.debug("Background thread stored batch.")
            except Exception as e:
                logger.error("Error storing snapshot in background thread: %s", e)
            finally:
                self._save_queue.task_done()

    def shutdown(self) -> None:
        """
        Gracefully shutdown the background worker thread.
        Waits for all queued items to be processed before stopping.
        
        This method is idempotent and thread-safe - it's safe to call multiple times
        or concurrently from different threads.
        """
        with self._shutdown_lock:
            if self._shutdown:
                return
            self._shutdown = True
        
        logger.info("Shutting down BatchStorageWorker.")
        # Send sentinel value to stop the worker
        self._save_queue.put(None)
        # Wait for all queued items to be processed
        self._save_queue.join()
        # Wait for worker thread to finish
        self._worker_thread.join()
        logger.info("BatchStorageWorker shutdown complete.")
