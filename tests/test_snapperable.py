import os
import pytest
from pathlib import Path

from snapperable.snapper import Snapper
from snapperable.storage.pickle_storage import PickleSnapshotStorage
from snapperable.storage.sqlite_storage import SqlLiteSnapshotStorage


class SimulatedInterrupt(Exception):
    pass


def test_snapper_resume_after_interrupt(tmp_path: Path):
    data = list(range(10))
    processed: list[int] = []
    interrupt_at = 5
    first_run = {"done": False}

    def process(item: int) -> int:
        processed.append(item)
        # Only raise on the first run
        if item == interrupt_at and not first_run["done"]:
            first_run["done"] = True
            raise SimulatedInterrupt()
        return item * 2

    snapshot_storage_path = os.path.join(tmp_path, "test_snapperable.chkpt")
    snapshot_storage = PickleSnapshotStorage[int](snapshot_storage_path)

    # First run - will be interrupted
    with Snapper(data, process, snapshot_storage=snapshot_storage) as snapper:
        # Simulate interruption
        with pytest.raises(SimulatedInterrupt):
            snapper.start()

    # Now resume with a new Snapper instance but same storage file path
    processed.clear()
    snapshot_storage2 = PickleSnapshotStorage[int](snapshot_storage_path)
    with Snapper(data, process, snapshot_storage=snapshot_storage2) as snapper2:
        snapper2.start()
        result = snapper2.load()

        assert set(result) == set([i * 2 for i in data])
        assert result == [process(it) for it in data]
        assert all(i in processed or i == interrupt_at for i in data)


def test_snapper_prevents_shared_storage(tmp_path: Path):
    """
    Test that Snapper prevents multiple instances from sharing the same storage.
    This protects against race conditions in multithreaded scenarios.
    """
    # Define a shared storage backend
    snapshot_storage_path = tmp_path / "test_shared_storage.db"
    shared_storage = SqlLiteSnapshotStorage[int](snapshot_storage_path)

    # Define a simple processing function
    def process_item(item: int) -> int:
        return item * 2

    # Define the iterable
    iterable = range(10)

    # Create first Snapper instance with the storage
    _snapper1 = Snapper(iterable, process_item, snapshot_storage=shared_storage)

    # Attempting to create a second Snapper with the same storage should raise ValueError
    with pytest.raises(ValueError, match="already in use by another Snapper instance"):
        _snapper2 = Snapper(iterable, process_item, snapshot_storage=shared_storage)


@pytest.mark.xfail(
    reason="True multithreading support with shared storage not implemented yet"
)
def test_snapper_multithreading_with_shared_storage(tmp_path: Path):
    """
    Test Snapper's behavior when executed with multiple threads on the same storage.
    Future implementation should allow multiple threads to coordinate work using shared storage
    with proper locking and work distribution.
    """
    import threading

    # Define a shared storage backend
    snapshot_storage_path = tmp_path / "test_multithreading.db"
    shared_storage = SqlLiteSnapshotStorage[int](snapshot_storage_path)

    # Define a simple processing function
    def process_item(item: int) -> int:
        return item * 2

    # Define the iterable
    iterable = range(10)

    # Create two Snapper instances sharing the same storage
    # This should work in the future with proper thread-safe coordination
    snapper1 = Snapper(iterable, process_item, snapshot_storage=shared_storage)
    snapper2 = Snapper(iterable, process_item, snapshot_storage=shared_storage)

    # Define threads
    thread1 = threading.Thread(target=snapper1.start)
    thread2 = threading.Thread(target=snapper2.start)

    # Start threads
    thread1.start()
    thread2.start()

    # Wait for threads to complete
    thread1.join()
    thread2.join()

    # Load results from the shared storage
    results = shared_storage.load_snapshot()

    # Verify the results - each item should be processed exactly once
    assert len(results) == len(iterable)
    assert all(result == item * 2 for item, result in zip(iterable, results))


def test_snapper_prevents_different_storage_instances_same_file_sqlite(tmp_path: Path):
    """
    Test that Snapper prevents two different SqlLiteSnapshotStorage instances
    pointing to the same file from being used simultaneously.
    """
    snapshot_storage_path = tmp_path / "same_file.db"
    
    # Create two different storage instances pointing to the same file
    storage1 = SqlLiteSnapshotStorage[int](snapshot_storage_path)
    storage2 = SqlLiteSnapshotStorage[int](snapshot_storage_path)
    
    def process_item(item: int) -> int:
        return item * 2
    
    iterable = range(10)
    
    # Create first Snapper with storage1
    _snapper1 = Snapper(iterable, process_item, snapshot_storage=storage1)
    
    # Attempting to create a second Snapper with storage2 (different instance, same file)
    # should raise ValueError
    with pytest.raises(ValueError, match="already in use by another Snapper instance"):
        _snapper2 = Snapper(iterable, process_item, snapshot_storage=storage2)


def test_snapper_prevents_different_storage_instances_same_file_pickle(tmp_path: Path):
    """
    Test that Snapper prevents two different PickleSnapshotStorage instances
    pointing to the same file from being used simultaneously.
    """
    snapshot_storage_path = str(tmp_path / "same_file.pkl")
    
    # Create two different storage instances pointing to the same file
    storage1 = PickleSnapshotStorage[int](snapshot_storage_path)
    storage2 = PickleSnapshotStorage[int](snapshot_storage_path)
    
    def process_item(item: int) -> int:
        return item * 2
    
    iterable = range(10)
    
    # Create first Snapper with storage1
    _snapper1 = Snapper(iterable, process_item, snapshot_storage=storage1)
    
    # Attempting to create a second Snapper with storage2 (different instance, same file)
    # should raise ValueError
    with pytest.raises(ValueError, match="already in use by another Snapper instance"):
        _snapper2 = Snapper(iterable, process_item, snapshot_storage=storage2)


def test_snapper_prevents_mixed_storage_types_same_file(tmp_path: Path):
    """
    Test that Snapper prevents different storage types (Pickle and SQLite)
    from being used if they somehow point to the same file path.
    This is an edge case but good to protect against.
    """
    snapshot_storage_path = str(tmp_path / "same_file.data")
    
    # Create different storage types pointing to the same file path
    storage1 = SqlLiteSnapshotStorage[int](snapshot_storage_path)
    storage2 = PickleSnapshotStorage[int](snapshot_storage_path)
    
    def process_item(item: int) -> int:
        return item * 2
    
    iterable = range(10)
    
    # Create first Snapper with storage1
    _snapper1 = Snapper(iterable, process_item, snapshot_storage=storage1)
    
    # Attempting to create a second Snapper with storage2 (different type, same file)
    # should raise ValueError
    with pytest.raises(ValueError, match="already in use by another Snapper instance"):
        _snapper2 = Snapper(iterable, process_item, snapshot_storage=storage2)
