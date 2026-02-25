#!/usr/bin/env python3
"""
Demonstration of the new dynamic iterable support in Snapperable.

This script shows how Snapperable now handles:
1. Growing iterables (adding new items)
2. Reordered iterables
3. Different output retrieval methods (load vs load_all)
"""

import os
import tempfile
from snapperable import Snapper
from snapperable.storage.sqlite_storage import SQLiteSnapshotStorage
from snapperable.logger import logger
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(filename)s - %(levelname)s - %(message)s",
)

# logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


def demo_growing_iterable():
    """Demonstrate handling of growing iterables."""
    print("\n" + "=" * 60)
    print("Demo 1: Growing Iterable")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        storage_path = os.path.join(tmpdir, "demo1.pkl")

        def process(x):
            print(f"Processing {x}...")
            return x * 2

        # First run: process 5 items
        print("\nFirst run: Processing [0, 1, 2, 3, 4]")
        data_v1 = list(range(5))
        storage1 = SQLiteSnapshotStorage(storage_path)
        with Snapper(data_v1, process, snapshot_storage=storage1) as snapper:
            snapper.start()
            result = snapper.load()
        print(f"Results: {result}")

        # Second run: add more items
        print("\nSecond run: Processing [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]")
        data_v2 = list(range(10))
        storage2 = SQLiteSnapshotStorage(storage_path)
        with Snapper(data_v2, process, snapshot_storage=storage2) as snapper:
            snapper.start()
            result = snapper.load()
        print(f"Results: {result}")
        print("✓ Only new items (5-9) were processed!")


def demo_load_vs_load_all():
    """Demonstrate difference between load() and load_all()."""
    print("\n" + "=" * 60)
    print("Demo 2: load() vs load_all()")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        storage_path = os.path.join(tmpdir, "demo3.pkl")

        def process(x):
            print(f"Processing {x}...")
            return x * 2

        # First run: process items
        print("\nFirst run: Processing [1, 2, 3, 4, 5]")
        data_v1 = [1, 2, 3, 4, 5]
        storage1 = SQLiteSnapshotStorage(storage_path)
        with Snapper(data_v1, process, snapshot_storage=storage1) as snapper:
            snapper.start()

        # Second run: different iterable
        print("\nSecond run: Changed iterable to [10, 20, 30]")
        data_v2 = [10, 20, 30]
        storage2 = SQLiteSnapshotStorage(storage_path)
        with Snapper(data_v2, process, snapshot_storage=storage2) as snapper:
            snapper.start()
            result_load = snapper.load()
            result_load_all = snapper.load_all()

        print(f"\nload() returns outputs matching current inputs: {result_load}")
        print(f"load_all() returns all stored outputs: {result_load_all}")
        print("✓ load() filters by current inputs, load_all() returns everything!")


def demo_reordered_iterable():
    """Demonstrate handling of reordered iterables."""
    print("\n" + "=" * 60)
    print("Demo 3: Reordered Iterable")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        storage_path = os.path.join(tmpdir, "demo4.pkl")

        def process(x):
            print(f"Processing {x}...")
            return x * 2

        # First run: process items in order
        print("\nFirst run: Processing [1, 2, 3, 4, 5]")
        data_v1 = [1, 2, 3, 4, 5]
        storage1 = SQLiteSnapshotStorage(storage_path)
        with Snapper(data_v1, process, snapshot_storage=storage1) as snapper:
            snapper.start()
            result = snapper.load()
        print(f"Results: {result}")

        # Second run: reorder items
        print("\nSecond run: Processing [5, 4, 3, 2, 1] (reversed)")
        data_v2 = [5, 4, 3, 2, 1]
        storage2 = SQLiteSnapshotStorage(storage_path)
        with Snapper(data_v2, process, snapshot_storage=storage2) as snapper:
            # No need to process, just load
            result = snapper.load()
        print(f"Results: {result}")
        print("✓ Outputs match the new order without reprocessing!")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Snapperable Dynamic Iterable Support Demo")
    print("=" * 60)

    demo_growing_iterable()
    demo_load_vs_load_all()
    demo_reordered_iterable()

    print("\n" + "=" * 60)
    print("All demos completed successfully!")
    print("=" * 60 + "\n")
