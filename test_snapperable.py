import os
import pytest
import tempfile
from snapperable import Snapper

from checkpoint_manager import CheckpointManager, PickleCheckpointManager

from pathlib import Path

from typing import TypeVar


@pytest.fixture
def checkpoint_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


class SimulatedInterrupt(Exception):
    pass


def test_snapper_resume_after_interrupt(checkpoint_dir: Path):
    data = list(range(10))
    processed = []
    interrupt_at = 5
    first_run = {"done": False}

    T = TypeVar("T")

    def process(item: T) -> T:
        processed.append(item)
        # Only raise on the first run
        if item == interrupt_at and not first_run["done"]:
            first_run["done"] = True
            raise SimulatedInterrupt()
        return item * 2

    checkpoint_path = os.path.join(checkpoint_dir, "test_snapperable.chkpt")
    checkpoint_manager = PickleCheckpointManager(checkpoint_path)
    snapper = Snapper(data, process, checkpoint_manager=checkpoint_manager)
    # Simulate interruption
    with pytest.raises(SimulatedInterrupt):
        snapper.start()

    # Now resume
    processed.clear()
    snapper = Snapper(data, process, checkpoint_manager=checkpoint_manager)
    snapper.start()
    result = snapper.load()

    assert set(result) == set([i * 2 for i in data])
    assert result == [process(it) for it in data]
    assert all(i in processed or i == interrupt_at for i in data)
