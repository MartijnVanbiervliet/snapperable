"""Unit tests for SnapshotTracker class."""

from unittest.mock import MagicMock

from snapperable.snapshot_tracker import SnapshotTracker


class TestSnapshotTracker:
    """Test suite for SnapshotTracker class."""

    def test_initialization_empty_storage(self):
        """Test tracker initialization with no previously stored inputs."""
        mock_storage = MagicMock()
        mock_storage.load_inputs.return_value = []
        
        iterable = [1, 2, 3]
        tracker = SnapshotTracker(iterable, mock_storage)
        
        # Get remaining items should return all items
        remaining = list(tracker.get_remaining())
        assert remaining == [1, 2, 3]
        mock_storage.load_inputs.assert_called_once()

    def test_initialization_with_stored_inputs(self):
        """Test tracker initialization with previously stored inputs."""
        mock_storage = MagicMock()
        mock_storage.load_inputs.return_value = [1, 2]
        
        iterable = [1, 2, 3, 4]
        tracker = SnapshotTracker(iterable, mock_storage)
        
        # Get remaining items should skip stored inputs
        remaining = list(tracker.get_remaining())
        assert remaining == [3, 4]

    def test_mark_processed(self):
        """Test marking items as processed."""
        mock_storage = MagicMock()
        mock_storage.load_inputs.return_value = []
        
        iterable = [1, 2, 3]
        tracker = SnapshotTracker(iterable, mock_storage)
        
        # Mark item 1 as processed
        tracker.mark_processed(1)
        
        # Get remaining should exclude item 1
        remaining = list(tracker.get_remaining())
        assert remaining == [2, 3]

    def test_mark_processed_during_iteration(self):
        """Test marking items as processed during iteration."""
        mock_storage = MagicMock()
        mock_storage.load_inputs.return_value = []
        
        iterable = [1, 2, 3, 4, 5]
        tracker = SnapshotTracker(iterable, mock_storage)
        
        # Simulate processing items one by one
        processed = []
        for item in tracker.get_remaining():
            processed.append(item)
            tracker.mark_processed(item)
            if len(processed) >= 3:
                break
        
        # Configure storage to return processed items, then create new tracker to verify persistence
        mock_storage.load_inputs.return_value = [1, 2, 3]
        tracker2 = SnapshotTracker([1, 2, 3, 4, 5], mock_storage)
        
        remaining = list(tracker2.get_remaining())
        assert remaining == [4, 5]

    def test_make_hashable_list(self):
        """Test _make_hashable with list inputs."""
        mock_storage = MagicMock()
        mock_storage.load_inputs.return_value = [[1, 2], [3, 4]]
        
        iterable = [[1, 2], [3, 4], [5, 6]]
        tracker = SnapshotTracker(iterable, mock_storage)
        
        remaining = list(tracker.get_remaining())
        assert remaining == [[5, 6]]

    def test_make_hashable_dict(self):
        """Test _make_hashable with dict inputs."""
        mock_storage = MagicMock()
        mock_storage.load_inputs.return_value = [{"a": 1, "b": 2}]
        
        iterable = [{"a": 1, "b": 2}, {"c": 3, "d": 4}]
        tracker = SnapshotTracker(iterable, mock_storage)
        
        remaining = list(tracker.get_remaining())
        assert len(remaining) == 1
        assert remaining[0] == {"c": 3, "d": 4}

    def test_make_hashable_set(self):
        """Test _make_hashable with set inputs."""
        mock_storage = MagicMock()
        mock_storage.load_inputs.return_value = [{1, 2, 3}]
        
        iterable = [{1, 2, 3}, {4, 5, 6}]
        tracker = SnapshotTracker(iterable, mock_storage)
        
        remaining = list(tracker.get_remaining())
        assert len(remaining) == 1
        assert remaining[0] == {4, 5, 6}

    def test_make_hashable_nested_structures(self):
        """Test _make_hashable with nested data structures."""
        mock_storage = MagicMock()
        mock_storage.load_inputs.return_value = [{"a": [1, 2], "b": {3, 4}}]
        
        iterable = [{"a": [1, 2], "b": {3, 4}}, {"a": [5, 6], "b": {7, 8}}]
        tracker = SnapshotTracker(iterable, mock_storage)
        
        remaining = list(tracker.get_remaining())
        assert len(remaining) == 1
        assert remaining[0] == {"a": [5, 6], "b": {7, 8}}

    def test_unhashable_objects_are_processed_again(self):
        """Test that unhashable objects are always processed."""
        # Create a custom unhashable class
        class UnhashableClass:
            def __init__(self, value):
                self.value = value
            
            def __eq__(self, other):
                return isinstance(other, UnhashableClass) and self.value == other.value
            
            # Mark class as unhashable in the standard Python way
            __hash__ = None
        
        mock_storage = MagicMock()
        obj1 = UnhashableClass(1)
        mock_storage.load_inputs.return_value = [obj1]
        
        obj2 = UnhashableClass(1)
        obj3 = UnhashableClass(2)
        iterable = [obj2, obj3]
        tracker = SnapshotTracker(iterable, mock_storage)
        
        # Unhashable objects should always be yielded
        remaining = list(tracker.get_remaining())
        assert len(remaining) == 2

    def test_initialization_is_lazy(self):
        """Test that initialization only happens once."""
        mock_storage = MagicMock()
        mock_storage.load_inputs.return_value = [1, 2]
        
        iterable = [1, 2, 3]
        tracker = SnapshotTracker(iterable, mock_storage)
        
        # load_inputs should not be called until get_remaining is called
        mock_storage.load_inputs.assert_not_called()
        
        # First call to get_remaining triggers initialization
        list(tracker.get_remaining())
        assert mock_storage.load_inputs.call_count == 1
        
        # Second call should not trigger another load
        list(tracker.get_remaining())
        assert mock_storage.load_inputs.call_count == 1

    def test_duplicates_in_iterable(self):
        """Test handling of duplicate items in the iterable."""
        mock_storage = MagicMock()
        mock_storage.load_inputs.return_value = [1]
        
        iterable = [1, 2, 1, 3, 1]
        tracker = SnapshotTracker(iterable, mock_storage)
        
        # First 1 is stored, so should be skipped
        # Other occurrences of 1 should also be skipped
        remaining = list(tracker.get_remaining())
        assert remaining == [2, 3]

    def test_empty_iterable(self):
        """Test with an empty iterable."""
        mock_storage = MagicMock()
        mock_storage.load_inputs.return_value = []
        
        iterable = []
        tracker = SnapshotTracker(iterable, mock_storage)
        
        remaining = list(tracker.get_remaining())
        assert remaining == []

    def test_all_items_already_processed(self):
        """Test when all items in iterable are already processed."""
        mock_storage = MagicMock()
        mock_storage.load_inputs.return_value = [1, 2, 3, 4, 5]
        
        iterable = [1, 2, 3]
        tracker = SnapshotTracker(iterable, mock_storage)
        
        remaining = list(tracker.get_remaining())
        assert remaining == []

    def test_mark_processed_with_unhashable_object(self):
        """Test that marking unhashable objects doesn't crash."""
        class UnhashableClass:
            # Mark class as unhashable in the standard Python way
            __hash__ = None
        
        mock_storage = MagicMock()
        mock_storage.load_inputs.return_value = []
        
        iterable = [1, 2]
        tracker = SnapshotTracker(iterable, mock_storage)
        
        obj = UnhashableClass()
        # Should not raise an error
        tracker.mark_processed(obj)

    def test_tuple_vs_list_equivalence(self):
        """Test that tuples and lists with same content are treated as equivalent."""
        mock_storage = MagicMock()
        mock_storage.load_inputs.return_value = [[1, 2, 3]]
        
        # Iterable has tuple with same content
        iterable = [(1, 2, 3), [4, 5, 6]]
        tracker = SnapshotTracker(iterable, mock_storage)
        
        # The tuple (1, 2, 3) should match the stored list [1, 2, 3]
        remaining = list(tracker.get_remaining())
        assert len(remaining) == 1
        assert remaining[0] == [4, 5, 6]

    def test_generator_iterable(self):
        """Test that tracker works with generator iterables."""
        mock_storage = MagicMock()
        mock_storage.load_inputs.return_value = [1, 2]
        
        def gen():
            yield 1
            yield 2
            yield 3
            yield 4
        
        tracker = SnapshotTracker(gen(), mock_storage)
        
        remaining = list(tracker.get_remaining())
        assert remaining == [3, 4]
