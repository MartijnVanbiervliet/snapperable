"""Background worker for asynchronous batch storage."""

from typing import Any, List, Optional
import queue
import threading

from snapperable.storage.snapshot_storage import SnapshotStorage
from snapperable.logger import logger
from snapperable.processing_metrics import ProcessingMetric


class BatchStorageWorker:
    """
    Manages background thread for asynchronous storage of batches.

    This worker runs a dedicated thread that consumes from a queue and
    delegates storage operations to a backend, allowing the main processing
    loop to continue without blocking on I/O.
    """

    def __init__(self, storage_backend: SnapshotStorage[Any], max_retries: int = 3):
        """
        Initialize the BatchStorageWorker.

        Args:
            storage_backend: The storage backend to delegate saving to.
            max_retries: Maximum number of retry attempts for failed storage operations.
                        Default is 3. If all retries fail, the exception is raised during shutdown.
        """
        self.storage_backend = storage_backend
        self.max_retries = max_retries
        self._save_queue: queue.Queue[
            tuple[List[Any], List[Any], str, List[ProcessingMetric]] | None
        ] = queue.Queue()
        self._worker_thread = threading.Thread(target=self._save_worker, daemon=True)
        self._shutdown = False
        self._shutdown_lock = threading.Lock()
        self._failed_exception: Optional[Exception] = None
        self._worker_thread.start()

    def enqueue_batch(
        self,
        outputs: List[Any],
        inputs: List[Any],
        batch_id: str = "undefined",
        metrics: List[ProcessingMetric] | None = None,
    ) -> None:
        """
        Enqueue a batch for background saving.

        Args:
            outputs: The list of processed outputs to save.
            inputs: The list of corresponding inputs.
            batch_id: Unique identifier for the batch for tracing in logs.
            metrics: Optional list of ProcessingMetric instances for this batch.

        Raises:
            RuntimeError: If called after shutdown() has been invoked.
        """
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError(
                    "Cannot enqueue batch after BatchStorageWorker has been shut down. "
                    "Items will not be processed."
                )

        logger.debug(
            "Enqueueing batch of size %d for background saving (batch_id=%s).",
            len(outputs),
            batch_id,
        )
        self._save_queue.put((outputs, inputs, batch_id, metrics or []))
        logger.debug("Batch enqueued (batch_id=%s).", batch_id)

    def _save_worker(self) -> None:
        """
        Background worker thread that processes the save queue.
        Runs continuously until a sentinel value (None) is received.

        Retries failed storage operations up to max_retries times. If all retries
        fail, stores the exception to be re-raised during shutdown.
        """
        while True:
            item = self._save_queue.get()
            if item is None:
                # Sentinel value to stop the worker
                self._save_queue.task_done()
                break

            outputs, inputs, batch_id, metrics = item
            last_exception = None

            # Retry loop
            for attempt in range(self.max_retries + 1):
                try:
                    if attempt > 0:
                        logger.warning(
                            "Retrying storage operation (attempt %d/%d) for batch of size %d (batch_id=%s)",
                            attempt,
                            self.max_retries,
                            len(outputs),
                            batch_id,
                        )
                    else:
                        logger.debug(
                            "Background thread storing batch of size %d (batch_id=%s).",
                            len(outputs),
                            batch_id,
                        )

                    self.storage_backend.store_snapshot(outputs, inputs)
                    if metrics:
                        self.storage_backend.store_metrics(metrics)
                    logger.debug(
                        "Background thread stored batch (batch_id=%s).", batch_id
                    )
                    last_exception = None
                    break  # Success - exit retry loop

                except Exception as e:
                    last_exception = e
                    if attempt < self.max_retries:
                        logger.warning(
                            "Storage operation failed (attempt %d/%d) (batch_id=%s): %s",
                            attempt + 1,
                            self.max_retries + 1,
                            batch_id,
                            e,
                        )
                    else:
                        logger.error(
                            "Storage operation failed after %d attempts (batch_id=%s): %s",
                            self.max_retries + 1,
                            batch_id,
                            e,
                        )

            # If all retries failed, store the exception to be raised during shutdown
            if last_exception is not None:
                self._failed_exception = last_exception

            self._save_queue.task_done()

    def shutdown(self) -> None:
        """
        Gracefully shutdown the background worker thread.
        Waits for all queued items to be processed before stopping.

        This method is idempotent and thread-safe - it's safe to call multiple times
        or concurrently from different threads.

        Raises:
            Exception: If any storage operation failed after all retries were exhausted.
        """
        with self._shutdown_lock:
            if self._shutdown:
                return
            self._shutdown = True

        logger.debug("Shutting down BatchStorageWorker.")
        # Send sentinel value to stop the worker
        self._save_queue.put(None)
        # Wait for all queued items to be processed
        self._save_queue.join()
        # Wait for worker thread to finish
        self._worker_thread.join()
        logger.info("BatchStorageWorker shutdown complete.")

        # If there was a failure, raise the exception now
        if self._failed_exception is not None:
            raise self._failed_exception
