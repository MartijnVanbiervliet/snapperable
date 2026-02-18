import os
import pytest
from pathlib import Path

from snapperable.snapper import Snapper
from snapperable.storage.pickle_storage import PickleSnapshotStorage
from snapperable.storage.sqlite_storage import SQLiteSnapshotStorage


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
    shared_storage = SQLiteSnapshotStorage[int](snapshot_storage_path)

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
    shared_storage = SQLiteSnapshotStorage[int](snapshot_storage_path)

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
    Test that Snapper prevents two different SQLiteSnapshotStorage instances
    pointing to the same file from being used simultaneously.
    """
    snapshot_storage_path = tmp_path / "same_file.db"
    
    # Create two different storage instances pointing to the same file
    storage1 = SQLiteSnapshotStorage[int](snapshot_storage_path)
    storage2 = SQLiteSnapshotStorage[int](snapshot_storage_path)
    
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
    storage1 = SQLiteSnapshotStorage[int](snapshot_storage_path)
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


def test_snapper_with_cache_disabled(tmp_path: Path):
    """
    Test that Snapper works correctly with iterable caching disabled (default behavior).
    This is the default mode and should support lazy evaluation.
    """
    snapshot_storage_path = tmp_path / "test_no_cache.db"
    storage = SQLiteSnapshotStorage[int](snapshot_storage_path)
    
    # Use a generator to verify lazy evaluation
    def number_generator():
        for i in range(10):
            yield i
    
    def process_item(item: int) -> int:
        return item * 2
    
    # Create Snapper with cache_iterable=False (default)
    with Snapper(number_generator(), process_item, snapshot_storage=storage, cache_iterable=False) as snapper:
        snapper.start()
        results = snapper.load()
        
        assert len(results) == 10
        assert results == [i * 2 for i in range(10)]


def test_snapper_with_cache_enabled(tmp_path: Path):
    """
    Test that Snapper correctly caches the iterable when cache_iterable=True.
    """
    snapshot_storage_path = tmp_path / "test_with_cache.db"
    storage = SQLiteSnapshotStorage[int](snapshot_storage_path)
    
    # Track if the iterator was consumed
    consumed_count = {"count": 0}
    
    def counting_generator():
        for i in range(10):
            consumed_count["count"] += 1
            yield i
    
    def process_item(item: int) -> int:
        return item * 2
    
    # Create Snapper with cache_iterable=True
    with Snapper(
        counting_generator(), 
        process_item, 
        snapshot_storage=storage, 
        cache_iterable=True
    ) as snapper:
        snapper.start()
        
        # Verify the generator was consumed once during caching
        assert consumed_count["count"] == 10
        
        # Verify the cache was created
        assert snapper._cached_iterable is not None
        assert len(snapper._cached_iterable) == 10
        
        results = snapper.load()
        assert len(results) == 10
        assert results == [i * 2 for i in range(10)]


def test_snapper_clear_cache(tmp_path: Path):
    """
    Test that clear_cache() method successfully clears the cached iterable.
    """
    snapshot_storage_path = tmp_path / "test_clear_cache.db"
    storage = SQLiteSnapshotStorage[int](snapshot_storage_path)
    
    data = list(range(10))
    
    def process_item(item: int) -> int:
        return item * 2
    
    # Create Snapper with cache_iterable=True
    snapper = Snapper(
        data, 
        process_item, 
        snapshot_storage=storage, 
        cache_iterable=True
    )
    
    # Process the iterable
    snapper.start()
    
    # Verify cache was created
    assert snapper._cached_iterable is not None
    
    # Clear the cache
    snapper.clear_cache()
    
    # Verify cache was cleared
    assert snapper._cached_iterable is None
    
    # Verify we can still load results
    results = snapper.load()
    assert len(results) == 10
    assert results == [i * 2 for i in range(10)]
    
    # Clean up
    snapper._release_storage()


def test_snapper_cache_reused_on_multiple_starts(tmp_path: Path):
    """
    Test that cached iterable is reused when start() is called multiple times.
    """
    snapshot_storage_path = tmp_path / "test_cache_reuse.db"
    
    consumed_count = {"count": 0}
    
    def counting_generator():
        for i in range(5):
            consumed_count["count"] += 1
            yield i
    
    def process_item(item: int) -> int:
        return item * 2
    
    # First run - process all items
    storage1 = SQLiteSnapshotStorage[int](snapshot_storage_path)
    with Snapper(
        counting_generator(), 
        process_item, 
        snapshot_storage=storage1, 
        cache_iterable=True
    ) as snapper:
        snapper.start()
        assert consumed_count["count"] == 5
        results = snapper.load()
        assert len(results) == 5
    
    # Second run with a new storage pointing to the same file
    # This should resume from where it left off (all items already processed)
    storage2 = SQLiteSnapshotStorage[int](snapshot_storage_path)
    with Snapper(
        counting_generator(), 
        process_item, 
        snapshot_storage=storage2, 
        cache_iterable=True
    ) as snapper2:
        # Since all items are already processed, start should not process anything new
        # But it should still cache the generator
        initial_count = consumed_count["count"]
        snapper2.start()
        # The generator will be consumed for caching
        assert consumed_count["count"] == initial_count + 5
        
        # Verify the cache was created
        assert snapper2._cached_iterable is not None
        
        results = snapper2.load()
        assert len(results) == 5
        assert results == [i * 2 for i in range(5)]


def test_snapper_without_cache_supports_generators(tmp_path: Path):
    """
    Test that without caching, Snapper can process generators (lazy evaluation).
    Note: Generators can only be consumed once, so this tests the default behavior.
    """
    snapshot_storage_path = tmp_path / "test_generator.db"
    storage = SQLiteSnapshotStorage[int](snapshot_storage_path)
    
    def process_item(item: int) -> int:
        return item ** 2
    
    # Use a generator (cannot be consumed multiple times)
    generator = (i for i in range(100))
    
    # Process with cache_iterable=False (default) - should work
    with Snapper(generator, process_item, snapshot_storage=storage) as snapper:
        snapper.start()
        results = snapper.load()
        
        assert len(results) == 100
        assert results[0] == 0
        assert results[99] == 99 ** 2
