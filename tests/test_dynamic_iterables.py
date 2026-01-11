"""Tests for dynamic iterable support and input-based tracking."""

import os
from pathlib import Path

from snapperable.snapper import Snapper
from snapperable.storage.pickle_storage import PickleSnapshotStorage
from snapperable.storage.sqlite_storage import SQLiteSnapshotStorage


def test_snapper_handles_growing_iterable_pickle(tmp_path: Path):
    """
    Test that Snapper can handle a growing iterable.
    When items are added to the iterable between runs, it should process only new items.
    """
    snapshot_storage_path = os.path.join(tmp_path, "test_growing.pkl")
    
    def process_item(item: int) -> int:
        return item * 2
    
    # First run: process first 5 items
    data_v1 = list(range(5))
    storage1 = PickleSnapshotStorage[int](snapshot_storage_path)
    with Snapper(data_v1, process_item, snapshot_storage=storage1) as snapper:
        snapper.start()
        result = snapper.load()
    
    assert len(result) == 5
    assert result == [0, 2, 4, 6, 8]
    
    # Second run: add more items to the iterable
    data_v2 = list(range(10))  # Now has 10 items
    storage2 = PickleSnapshotStorage[int](snapshot_storage_path)
    with Snapper(data_v2, process_item, snapshot_storage=storage2) as snapper:
        snapper.start()
        result = snapper.load()
    
    # Should have all 10 items processed
    assert len(result) == 10
    assert result == [i * 2 for i in range(10)]


def test_snapper_handles_growing_iterable_sqlite(tmp_path: Path):
    """
    Test that Snapper can handle a growing iterable with SQLite storage.
    """
    snapshot_storage_path = tmp_path / "test_growing.db"
    
    def process_item(item: int) -> int:
        return item * 2
    
    # First run: process first 5 items
    data_v1 = list(range(5))
    storage1 = SQLiteSnapshotStorage[int](snapshot_storage_path)
    with Snapper(data_v1, process_item, snapshot_storage=storage1) as snapper:
        snapper.start()
        result = snapper.load()
    
    assert len(result) == 5
    assert result == [0, 2, 4, 6, 8]
    
    # Second run: add more items to the iterable
    data_v2 = list(range(10))  # Now has 10 items
    storage2 = SQLiteSnapshotStorage[int](snapshot_storage_path)
    with Snapper(data_v2, process_item, snapshot_storage=storage2) as snapper:
        snapper.start()
        result = snapper.load()
    
    # Should have all 10 items processed
    assert len(result) == 10
    assert result == [i * 2 for i in range(10)]


def test_snapper_handles_reordered_iterable(tmp_path: Path):
    """
    Test that Snapper handles reordered iterables correctly.
    When items are reordered, the output should match the new order.
    """
    snapshot_storage_path = os.path.join(tmp_path, "test_reordered.pkl")
    
    def process_item(item: int) -> int:
        return item * 2
    
    # First run: process items in order
    data_v1 = [1, 2, 3, 4, 5]
    storage1 = PickleSnapshotStorage[int](snapshot_storage_path)
    with Snapper(data_v1, process_item, snapshot_storage=storage1) as snapper:
        snapper.start()
        result = snapper.load()
    
    assert result == [2, 4, 6, 8, 10]
    
    # Second run: reorder items
    data_v2 = [5, 4, 3, 2, 1]  # Reversed order
    storage2 = PickleSnapshotStorage[int](snapshot_storage_path)
    with Snapper(data_v2, process_item, snapshot_storage=storage2) as snapper:
        # Don't need to reprocess, just load
        result = snapper.load()
    
    # Should return outputs in the new order (matching the current inputs)
    assert result == [10, 8, 6, 4, 2]


def test_snapper_load_all_returns_all_outputs(tmp_path: Path):
    """
    Test that load_all() returns all outputs regardless of current inputs.
    """
    snapshot_storage_path = os.path.join(tmp_path, "test_load_all.pkl")
    
    def process_item(item: int) -> int:
        return item * 2
    
    # First run: process items
    data_v1 = [1, 2, 3, 4, 5]
    storage1 = PickleSnapshotStorage[int](snapshot_storage_path)
    with Snapper(data_v1, process_item, snapshot_storage=storage1) as snapper:
        snapper.start()
    
    # Second run: different iterable
    data_v2 = [10, 20, 30]
    storage2 = PickleSnapshotStorage[int](snapshot_storage_path)
    with Snapper(data_v2, process_item, snapshot_storage=storage2) as snapper:
        all_outputs = snapper.load_all()
        matching_outputs = snapper.load()
    
    # load_all() should return all 5 original outputs
    assert len(all_outputs) == 5
    assert all_outputs == [2, 4, 6, 8, 10]
    
    # load() should return empty because inputs don't match
    assert len(matching_outputs) == 0


def test_snapper_handles_duplicates_in_iterable(tmp_path: Path):
    """
    Test that Snapper correctly handles duplicate values in the iterable.
    """
    snapshot_storage_path = os.path.join(tmp_path, "test_duplicates.pkl")
    
    process_count = {"count": 0}
    
    def process_item(item: int) -> int:
        process_count["count"] += 1
        return item * 2
    
    # First run with duplicates
    data = [1, 2, 2, 3, 3, 3]
    storage = PickleSnapshotStorage[int](snapshot_storage_path)
    with Snapper(data, process_item, snapshot_storage=storage) as snapper:
        snapper.start()
        result = snapper.load()
    
    # Should process unique values only once
    # So count should be 3 (for 1, 2, 3)
    assert process_count["count"] == 3
    # Result should still have all outputs matching the input order
    assert result == [2, 4, 4, 6, 6, 6]


def test_snapper_handles_shrinking_iterable(tmp_path: Path):
    """
    Test that Snapper handles a shrinking iterable (fewer items in second run).
    """
    snapshot_storage_path = os.path.join(tmp_path, "test_shrinking.pkl")
    
    def process_item(item: int) -> int:
        return item * 2
    
    # First run: process 10 items
    data_v1 = list(range(10))
    storage1 = PickleSnapshotStorage[int](snapshot_storage_path)
    with Snapper(data_v1, process_item, snapshot_storage=storage1) as snapper:
        snapper.start()
        result = snapper.load()
    
    assert len(result) == 10
    
    # Second run: only 5 items
    data_v2 = list(range(5))
    storage2 = PickleSnapshotStorage[int](snapshot_storage_path)
    with Snapper(data_v2, process_item, snapshot_storage=storage2) as snapper:
        result = snapper.load()
        all_result = snapper.load_all()
    
    # load() should return only matching outputs (5 items)
    assert len(result) == 5
    assert result == [i * 2 for i in range(5)]
    
    # load_all() should return all stored outputs (10 items)
    assert len(all_result) == 10
    assert all_result == [i * 2 for i in range(10)]


def test_snapper_with_complex_inputs(tmp_path: Path):
    """
    Test that Snapper handles complex inputs like tuples and dicts.
    """
    snapshot_storage_path = os.path.join(tmp_path, "test_complex.pkl")
    
    def process_item(item: tuple) -> str:
        name, age = item
        return f"{name}:{age}"
    
    # First run
    data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
    storage = PickleSnapshotStorage[str](snapshot_storage_path)
    with Snapper(data, process_item, snapshot_storage=storage) as snapper:
        snapper.start()
        result = snapper.load()
    
    assert result == ["Alice:30", "Bob:25", "Charlie:35"]
    
    # Second run: add new items
    data_v2 = [("Alice", 30), ("Bob", 25), ("Charlie", 35), ("Dave", 40)]
    storage2 = PickleSnapshotStorage[str](snapshot_storage_path)
    with Snapper(data_v2, process_item, snapshot_storage=storage2) as snapper:
        snapper.start()
        result = snapper.load()
    
    # Should process only the new item
    assert len(result) == 4
    assert result == ["Alice:30", "Bob:25", "Charlie:35", "Dave:40"]


def test_snapper_preserves_order_of_outputs(tmp_path: Path):
    """
    Test that output order matches input order even after resume.
    """
    snapshot_storage_path = os.path.join(tmp_path, "test_order.pkl")
    
    def process_item(item: int) -> int:
        return item * 2
    
    # First run: process some items
    data_v1 = [5, 3, 8, 1, 9]
    storage1 = PickleSnapshotStorage[int](snapshot_storage_path)
    with Snapper(data_v1, process_item, snapshot_storage=storage1) as snapper:
        snapper.start()
        result = snapper.load()
    
    assert result == [10, 6, 16, 2, 18]
    
    # Second run: same items in different order plus new ones
    data_v2 = [1, 3, 5, 8, 9, 2, 4]
    storage2 = PickleSnapshotStorage[int](snapshot_storage_path)
    with Snapper(data_v2, process_item, snapshot_storage=storage2) as snapper:
        snapper.start()
        result = snapper.load()
    
    # Should return outputs in the new input order, with new items processed
    assert result == [2, 6, 10, 16, 18, 4, 8]
