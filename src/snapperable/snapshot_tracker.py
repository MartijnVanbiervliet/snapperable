"""Tracks which inputs have been processed and determines remaining items."""

from typing import Iterable, Any, Callable, TypeVar

from snapperable.storage.snapshot_storage import SnapshotStorage
from snapperable.function_hasher import FunctionHasher

T = TypeVar("T")


class SnapshotTracker:
    """
    Tracks processed inputs and determines which items remain to be processed.
    
    This class manages the state of processed items by comparing the current iterable
    against stored inputs, and handles function version changes.
    """
    
    def __init__(
        self,
        iterable: Iterable[T],
        fn: Callable[[T], Any],
        snapshot_storage: SnapshotStorage[T]
    ):
        """
        Initialize the SnapshotTracker.
        
        Args:
            iterable: The iterable containing items to process.
            fn: The processing function.
            snapshot_storage: The storage backend for tracking processed items.
        """
        self.iterable = iterable
        self.fn = fn
        self.snapshot_storage = snapshot_storage
        self._processed_inputs_set: set[Any] = set()
        self._initialized = False
    
    def _make_hashable(self, obj: Any) -> Any:
        """
        Convert an object to a hashable representation.
        
        Args:
            obj: The object to make hashable.
            
        Returns:
            A hashable representation of the object.
        """
        if isinstance(obj, (list, tuple)):
            return tuple(self._make_hashable(item) for item in obj)
        elif isinstance(obj, dict):
            return tuple(sorted((k, self._make_hashable(v)) for k, v in obj.items()))
        elif isinstance(obj, set):
            return frozenset(self._make_hashable(item) for item in obj)
        else:
            return obj
    
    def _initialize(self) -> None:
        """
        Initialize the tracker by loading stored inputs and checking function version.
        """
        if self._initialized:
            return
        
        # Compute and check function version
        current_fn_version = FunctionHasher.compute_hash(self.fn)
        stored_fn_version = self.snapshot_storage.load_function_version()
        
        # Load previously stored inputs
        stored_inputs = self.snapshot_storage.load_inputs()
        
        # If function version changed and we have stored data, clear stored inputs
        # (we'll reprocess items with the new function)
        if stored_fn_version is not None and stored_fn_version != current_fn_version:
            # Function changed - we should reprocess with new function
            self._processed_inputs_set.clear()
        else:
            # Create a hashable representation of stored inputs
            for inp in stored_inputs:
                try:
                    self._processed_inputs_set.add(self._make_hashable(inp))
                except TypeError:
                    # If input is not hashable, we'll process it again
                    pass
        
        # Store the current function version
        self.snapshot_storage.store_function_version(current_fn_version)
        
        self._initialized = True
    
    def get_remaining(self) -> Iterable[T]:
        """
        Get the remaining items from the iterable that haven't been processed yet.
        
        Yields:
            Items from the iterable that haven't been processed.
        """
        self._initialize()
        
        for item in self.iterable:
            # Check if this input was already processed
            try:
                hashable_item = self._make_hashable(item)
                if hashable_item in self._processed_inputs_set:
                    continue
            except TypeError:
                # If item is not hashable, process it
                pass
            
            yield item
    
    def mark_processed(self, item: T) -> None:
        """
        Mark an item as processed.
        
        Args:
            item: The item that has been processed.
        """
        try:
            hashable_item = self._make_hashable(item)
            self._processed_inputs_set.add(hashable_item)
        except TypeError:
            # If not hashable, we can't track it
            pass
