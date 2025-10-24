import os
import pytest
from snapperable import Snapper

CHECKPOINT_PATH = "test_snapperable.chkpt"


def cleanup_checkpoint():
    if os.path.exists(CHECKPOINT_PATH):
        os.remove(CHECKPOINT_PATH)


@pytest.fixture(autouse=True)
def run_around_tests():
    cleanup_checkpoint()
    yield
    cleanup_checkpoint()


class SimulatedInterrupt(Exception):
    pass


def test_snapper_resume_after_interrupt():
    data = list(range(10))
    processed = []
    interrupt_at = 5
    first_run = {'done': False}

    def process(item):
        processed.append(item)
        # Only raise on the first run
        if item == interrupt_at and not first_run['done']:
            first_run['done'] = True
            raise SimulatedInterrupt()
        return item * 2

    snapper = Snapper(data, process, checkpoint_path=CHECKPOINT_PATH)
    # Simulate interruption
    with pytest.raises(SimulatedInterrupt):
        snapper.start()

    # Now resume
    processed.clear()
    snapper = Snapper(data, process, checkpoint_path=CHECKPOINT_PATH)
    snapper.start()
    result = snapper.load()

    assert set(result) == set([i * 2 for i in data])
    assert all(i in processed or i == interrupt_at for i in data)
