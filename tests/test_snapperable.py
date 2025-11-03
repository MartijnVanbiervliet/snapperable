import os
import pytest
from snapperable import Snapper
from snapperable.snapshot_storage import PickleSnapshotStorage, SqlLiteSnapshotStorage

from pathlib import Path
import threading


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
    snapper = Snapper(data, process, snapshot_storage=snapshot_storage)
    # Simulate interruption
    with pytest.raises(SimulatedInterrupt):
        snapper.start()

    # Now resume
    processed.clear()
    snapper = Snapper(data, process, snapshot_storage=snapshot_storage)
    snapper.start()
    result = snapper.load()

    assert set(result) == set([i * 2 for i in data])
    assert result == [process(it) for it in data]
    assert all(i in processed or i == interrupt_at for i in data)


@pytest.mark.xfail(reason="Multithreading support not implemented yet")
def test_snapper_multithreading():
    """
    Test Snapper's behavior when executed with multiple threads on the same storage.
    """
    # Define a shared storage backend
    shared_storage = SqlLiteSnapshotStorage()

    # Define a simple processing function
    def process_item(item):
        return item * 2

    # Define the iterable
    iterable = range(10)

    # Create two Snapper instances sharing the same storage
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

    # Verify the results
    assert len(results) == len(iterable)
    assert all(result == item * 2 for item, result in zip(iterable, results))
