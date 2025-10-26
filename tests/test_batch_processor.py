import pytest
from unittest.mock import MagicMock
from snapperable.batch_processor import BatchProcessor
import time


@pytest.fixture
def mock_snapshot_storage():
    return MagicMock()


@pytest.fixture
def batch_processor(mock_snapshot_storage):
    return BatchProcessor(storage_backend=mock_snapshot_storage, batch_size=3, max_wait_time=None)


def test_add_item(batch_processor):
    """
    Test that items can be added to the batch and the batch size increases accordingly.
    """
    batch_processor.add_item("item1")
    batch_processor.add_item("item2")
    assert len(batch_processor.current_batch) == 2


def test_batch_threshold(batch_processor, mock_snapshot_storage):
    """
    Test that the batch is flushed and processed when the batch size threshold is reached.
    """
    batch_processor.add_item("item1")
    batch_processor.add_item("item2")
    batch_processor.add_item("item3")

    # Ensure the batch was processed
    mock_snapshot_storage.store_snapshot.assert_called_once()
    assert len(batch_processor.current_batch) == 0


def test_process_batch(batch_processor, mock_snapshot_storage):
    """
    Test the flush method to ensure it processes and clears the current batch.
    """
    batch_processor.add_item("item1")
    batch_processor.add_item("item2")
    batch_processor.flush()

    # Ensure the batch was processed
    mock_snapshot_storage.store_snapshot.assert_called_once()
    assert len(batch_processor.current_batch) == 0


def test_different_batch_sizes(mock_snapshot_storage):
    """
    Test the BatchProcessor with different batch sizes to ensure proper functionality.
    """
    processor_small = BatchProcessor(storage_backend=mock_snapshot_storage, batch_size=2)
    processor_large = BatchProcessor(storage_backend=mock_snapshot_storage, batch_size=5)

    # Small batch size
    processor_small.add_item("item1")
    processor_small.add_item("item2")
    mock_snapshot_storage.store_snapshot.assert_called_once()

    # Large batch size
    processor_large.add_item("item1")
    processor_large.add_item("item2")
    processor_large.add_item("item3")
    processor_large.add_item("item4")
    processor_large.add_item("item5")
    assert mock_snapshot_storage.store_snapshot.call_count == 2


def test_wait_time_functionality(mock_snapshot_storage):
    """
    Test that the batch is flushed and processed when the maximum wait time is exceeded.
    """
    processor = BatchProcessor(storage_backend=mock_snapshot_storage, batch_size=10, max_wait_time=1)

    processor.add_item("item1")
    time.sleep(1.5)  # Wait for the batch to be processed due to timeout

    mock_snapshot_storage.store_snapshot.assert_called_once()
    assert len(processor.current_batch) == 0
