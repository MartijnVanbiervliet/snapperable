from typing import Iterable, Callable, Any, Optional, TypeVar, Generic
from types import TracebackType
import threading

from snapperable.storage.snapshot_storage import SnapshotStorage
from snapperable.storage.sqlite_storage import SQLiteSnapshotStorage
from snapperable.batch_processor import BatchProcessor
from snapperable.snapshot_tracker import SnapshotTracker

T = TypeVar("T")


class Snapper(Generic[T]):
    """
    Snapper processes an iterable with a user-defined function, saving intermediate snapshots to disk.
    This allows resuming long-running processes without losing progress.
    """

    # Class-level registry to track active storage file paths
    _active_storages: set[str] = set()
    _storage_lock = threading.Lock()

    def __init__(
        self,
        iterable: Iterable[T],
        fn: Callable[[T], Any],
        batch_size: int = 1,
        max_wait_time: float | None = None,
        max_retries: int = 3,
        snapshot_storage: Optional[SnapshotStorage[T]] = None,
        batch_processor: Optional[BatchProcessor] = None,
    ):
        """
        Initialize the Snapper.

        Args:
            iterable: The iterable to process.
            fn: The function to apply to each item in the iterable.
            snapshot_storage: Optional SnapshotStorage instance. Defaults to SQLite storage.
            batch_processor: Optional BatchProcessor instance. If not provided, a default one is created.
            batch_size: The number of items to batch before saving (used if batch_processor is None).
            max_wait_time: The maximum time to wait before saving a batch (used if batch_processor is None).
            max_retries: Maximum number of retry attempts for failed storage operations (used if batch_processor is None). Default is 3.
        
        Raises:
            ValueError: If the provided snapshot_storage is already in use by another Snapper instance.
        """
        self.iterable = iterable
        self.fn = fn

        if snapshot_storage is None:
            snapshot_storage = SQLiteSnapshotStorage()
        
        # Check if this storage file path is already in use
        storage_identifier = snapshot_storage.get_storage_identifier()
        with Snapper._storage_lock:
            if storage_identifier in Snapper._active_storages:
                raise ValueError(
                    "The provided snapshot_storage instance is already in use by another Snapper instance. "
                    "Each Snapper must have its own snapshot_storage instance to avoid race conditions."
                )
            Snapper._active_storages.add(storage_identifier)
        
        self.snapshot_storage = snapshot_storage
        self._storage_identifier = storage_identifier

        if batch_processor is None:
            batch_processor = BatchProcessor(
                storage_backend=self.snapshot_storage,
                batch_size=batch_size,
                max_wait_time=max_wait_time,
                max_retries=max_retries,
            )
        self.batch_processor = batch_processor
        
        # Cache for materialized inputs (used to optimize load() after start())
        self._cached_inputs: list[T] | None = None

    def start(self) -> None:
        """
        Start processing the iterable, saving progress to disk.
        Uses input-based tracking to handle dynamic iterables robustly.
        
        Note: This method caches the materialized iterable to optimize subsequent load() calls.
        """
        try:
            # Materialize and cache the iterable for efficient load() calls
            # This is done once during start() to avoid repeated materialization in load()
            # NOTE: This materializes the entire iterable into memory, which could be problematic
            # for very large or infinite iterables. This defeats lazy evaluation and memory efficiency.
            # See GitHub issue for potential future improvement to make this configurable.
            materialized_inputs = list(self.iterable)
            self._cached_inputs = materialized_inputs
            
            # Create snapshot tracker to manage processed inputs
            snapshot_tracker = SnapshotTracker(
                iterable=materialized_inputs,
                snapshot_storage=self.snapshot_storage
            )
            
            # Process remaining items
            for item in snapshot_tracker.get_remaining():
                # Process the item
                result = self.fn(item)
                
                # Add to batch processor with input value
                # The batch processor will store both input and output atomically
                self.batch_processor.add_item(result, input_value=item)
                
                # Mark as processed
                snapshot_tracker.mark_processed(item)

            # Ensure all remaining items are saved
            self.batch_processor.flush()
        finally:
            # Wait for background thread to finish saving, even if there's an exception
            self.batch_processor.shutdown()

    def load(self) -> list[T]:
        """
        Load the processed results from the snapshot storage.
        Returns outputs that match the current input sequence.
        
        Performance notes:
        - If load() is called after start(), it uses cached inputs (fast, no materialization).
        - If load() is called after program restart or interruption, the iterable must be 
          materialized to compare with stored inputs (slower, unavoidable).
        - For large iterables where input matching is not needed, consider using load_all().
        
        Returns:
            The list of processed results, or an empty list if no snapshot exists.
        """
        stored_inputs = self.snapshot_storage.load_inputs()
        
        # If no stored inputs, return all outputs (backward compatibility)
        if not stored_inputs:
            return self.snapshot_storage.load_snapshot()
        
        # Use cached inputs if available (after start()), otherwise materialize
        if self._cached_inputs is not None:
            current_inputs = self._cached_inputs
        else:
            # Materialize the iterable to compare with stored inputs
            # This is necessary after program restart or interruption to determine 
            # which outputs match the current input sequence
            current_inputs = list(self.iterable)
        
        # If inputs match, return the outputs
        if self._inputs_match(current_inputs, stored_inputs):
            return self.snapshot_storage.load_snapshot()
        
        # Otherwise, return outputs for matching inputs only
        return self._get_matching_outputs(current_inputs, stored_inputs)
    
    def load_all(self) -> list[T]:
        """
        Load all processed outputs from the snapshot storage,
        regardless of whether they match the current input sequence.
        
        Returns:
            The list of all processed results, or an empty list if no snapshot exists.
        """
        return self.snapshot_storage.load_all_outputs()
    
    def _inputs_match(self, current_inputs: list[Any], stored_inputs: list[Any]) -> bool:
        """
        Check if current inputs match stored inputs.
        
        Args:
            current_inputs: Current input values.
            stored_inputs: Stored input values.
            
        Returns:
            True if inputs match, False otherwise.
        """
        if len(current_inputs) != len(stored_inputs):
            return False
        
        for current, stored in zip(current_inputs, stored_inputs):
            if current != stored:
                return False
        
        return True
    
    def _get_matching_outputs(self, current_inputs: list[Any], stored_inputs: list[Any]) -> list[T]:
        """
        Get outputs that match the current inputs.
        
        Args:
            current_inputs: Current input values.
            stored_inputs: Stored input values.
            
        Returns:
            List of outputs for matching inputs.
        """
        all_outputs = self.snapshot_storage.load_all_outputs()
        
        # Create a mapping from stored inputs to outputs
        # NOTE: If there are duplicate inputs in storage (which shouldn't happen
        # with correct implementation, but could with external storage manipulation),
        # only the last output for each duplicate input will be kept.
        input_to_output = {}
        seen_inputs = set()
        for inp, out in zip(stored_inputs, all_outputs):
            try:
                hashable_inp = SnapshotTracker._make_hashable(inp)
                if hashable_inp in seen_inputs:
                    import warnings
                    warnings.warn(
                        f"Duplicate input detected in storage: {inp}. "
                        "Only the last output will be used.",
                        UserWarning,
                        stacklevel=2
                    )
                seen_inputs.add(hashable_inp)
                input_to_output[hashable_inp] = out
            except TypeError:
                # If not hashable, skip
                pass
        
        # Get outputs for current inputs
        matching_outputs = []
        for inp in current_inputs:
            try:
                hashable_inp = SnapshotTracker._make_hashable(inp)
                if hashable_inp in input_to_output:
                    matching_outputs.append(input_to_output[hashable_inp])
            except TypeError:
                # If not hashable, skip
                pass
        
        return matching_outputs

    def _release_storage(self) -> None:
        """
        Release the storage instance from the active registry.
        This allows the storage to be reused by another Snapper instance.
        """
        with Snapper._storage_lock:
            Snapper._active_storages.discard(self._storage_identifier)

    def __del__(self):
        """
        Cleanup when the Snapper instance is destroyed.
        """
        # Only release if initialization completed successfully
        if hasattr(self, '_storage_identifier'):
            self._release_storage()

    def __enter__(self):
        """
        Context manager entry.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        """
        Context manager exit. Shuts down the batch processor and releases the storage.
        """
        # shutdown() is idempotent, so it's safe to call even if already called in start()
        self.batch_processor.shutdown()
        self._release_storage()
        return False
