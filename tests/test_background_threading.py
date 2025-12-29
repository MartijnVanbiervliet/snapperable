import pytest
import time
import threading
from unittest.mock import MagicMock, patch
from snapperable.batch_processor import BatchProcessor
from snapperable.snapper import Snapper
from snapperable.storage.pickle_storage import PickleSnapshotStorage
from pathlib import Path


def test_processing_continues_during_slow_save():
    """
    Test that the main processing loop continues even when saves are slow.
    This demonstrates that I/O is non-blocking.
    """
    # Create a mock storage that simulates slow saves
    mock_storage = MagicMock()
    save_count = [0]
    
    def slow_store_snapshot(last_index, processed):
        save_count[0] += 1
        time.sleep(0.5)  # Simulate slow I/O
    
    mock_storage.store_snapshot.side_effect = slow_store_snapshot
    mock_storage.load_last_index.return_value = -1
    
    processor = BatchProcessor(storage_backend=mock_storage, batch_size=2)
    
    start_time = time.time()
    
    # Add 6 items, which will create 3 batches
    for i in range(6):
        processor.add_item(f"item{i}")
    
    # Time after adding all items (before shutdown)
    time_after_adding = time.time()
    
    processor.shutdown()
    end_time = time.time()
    
    # The time to add all items should be much less than 3 * 0.5 seconds
    # because the saves happen in the background
    time_to_add = time_after_adding - start_time
    assert time_to_add < 0.5, f"Processing should not block on I/O, took {time_to_add}s"
    
    # However, shutdown should wait for all saves to complete
    total_time = end_time - start_time
    # With 3 batches and 0.5s per save, total should be at least 1.5s
    # (though they may overlap slightly depending on timing)
    assert total_time >= 1.0, f"Shutdown should wait for all saves, took {total_time}s"
    
    # Verify all batches were saved
    assert save_count[0] == 3


def test_graceful_shutdown_ensures_all_items_saved():
    """
    Test that shutdown() waits for all queued items to be saved.
    """
    mock_storage = MagicMock()
    mock_storage.load_last_index.return_value = -1
    save_count = []
    
    def track_saves(last_index, processed):
        save_count.append(len(processed))
        time.sleep(0.1)  # Small delay to ensure items are queued
    
    mock_storage.store_snapshot.side_effect = track_saves
    
    processor = BatchProcessor(storage_backend=mock_storage, batch_size=2)
    
    # Add items rapidly
    for i in range(10):
        processor.add_item(f"item{i}")
    
    # Shutdown and verify all items were saved
    processor.shutdown()
    
    assert mock_storage.store_snapshot.call_count == 5  # 10 items / 2 batch_size
    assert sum(save_count) == 10  # All 10 items should be saved


def test_multiple_batches_saved_in_correct_order():
    """
    Test that multiple batches are saved in the correct order with correct indices.
    """
    mock_storage = MagicMock()
    mock_storage.load_last_index.return_value = -1
    saved_data = []
    
    def capture_saves(last_index, processed):
        saved_data.append((last_index, processed.copy()))
    
    mock_storage.store_snapshot = capture_saves
    
    processor = BatchProcessor(storage_backend=mock_storage, batch_size=2)
    
    # Add 6 items
    for i in range(6):
        processor.add_item(i)
    
    processor.shutdown()
    
    # Verify saves happened in order with correct indices
    assert len(saved_data) == 3
    assert saved_data[0] == (1, [0, 1])  # First batch: indices 0-1
    assert saved_data[1] == (3, [2, 3])  # Second batch: indices 2-3
    assert saved_data[2] == (5, [4, 5])  # Third batch: indices 4-5


def test_exception_in_save_worker_does_not_crash():
    """
    Test that exceptions in the background worker are caught and logged.
    """
    mock_storage = MagicMock()
    mock_storage.load_last_index.return_value = -1
    
    call_count = [0]
    
    def failing_store(last_index, processed):
        call_count[0] += 1
        if call_count[0] == 1:
            raise Exception("Simulated storage error")
    
    mock_storage.store_snapshot = failing_store
    
    processor = BatchProcessor(storage_backend=mock_storage, batch_size=1)
    
    # Add items
    processor.add_item("item1")
    processor.add_item("item2")
    
    # Should not crash, should complete gracefully
    processor.shutdown()
    
    # Verify that both saves were attempted
    assert call_count[0] == 2


def test_snapper_with_slow_storage_backend(tmp_path: Path):
    """
    Integration test: Verify that Snapper works correctly with slow storage backend.
    """
    snapshot_storage_path = tmp_path / "test.pkl"
    
    # Create a custom storage that adds delays
    class SlowPickleStorage(PickleSnapshotStorage):
        def store_snapshot(self, last_index, processed):
            time.sleep(0.2)  # Simulate slow I/O
            super().store_snapshot(last_index, processed)
    
    storage = SlowPickleStorage(str(snapshot_storage_path))
    
    def process_item(item):
        return item * 2
    
    data = list(range(10))
    
    start_time = time.time()
    
    with Snapper(data, process_item, snapshot_storage=storage, batch_size=2) as snapper:
        snapper.start()
    
    end_time = time.time()
    
    # Verify results are correct
    result = storage.load_snapshot()
    assert result == [i * 2 for i in data]
    
    # With background threading, processing should be faster than
    # if we waited for each save sequentially
    # (5 batches * 0.2s = 1.0s minimum for sequential)
    total_time = end_time - start_time
    # The actual time should be dominated by shutdown waiting, not processing
    assert total_time >= 0.8, "Should still take time to save all batches"


def test_worker_thread_is_daemon():
    """
    Test that the worker thread is a daemon thread so it doesn't prevent program exit.
    """
    mock_storage = MagicMock()
    mock_storage.load_last_index.return_value = -1
    
    processor = BatchProcessor(storage_backend=mock_storage, batch_size=10)
    
    # Verify the worker thread is a daemon
    assert processor._storage_worker._worker_thread.daemon is True
    
    processor.shutdown()


def test_shutdown_idempotent():
    """
    Test that calling shutdown multiple times is safe and idempotent.
    """
    mock_storage = MagicMock()
    mock_storage.load_last_index.return_value = -1
    
    processor = BatchProcessor(storage_backend=mock_storage, batch_size=1)
    processor.add_item("item1")
    
    # Call shutdown multiple times
    processor.shutdown()
    processor.shutdown()
    processor.shutdown()
    
    # Should not raise any errors
    assert mock_storage.store_snapshot.call_count == 1
