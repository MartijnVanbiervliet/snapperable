import os
import pytest
import tempfile
from snapperable import Snapper
from snapperable.snapshot_storage import PickleSnapshotStorage

from pathlib import Path


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
