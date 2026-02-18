# snapperable

A Python library for robust processing of long-running iterables with automatic checkpointing.

## Overview

`snapperable` helps you process any iterable that may take a long time to complete by saving intermediate snapshots (checkpoints) to disk. This ensures that progress is never lost, and you can resume processing from the last checkpoint in case of interruption.

## Features
- Process any iterable with a user-defined function
- Automatic periodic checkpointing to disk
- Resume processing from the last saved state
- Simple API: `Snapper(iterable, fn).start()` and `.load()`
- Multiple storage backends: SQLite and Pickle
- Batch processing with configurable thresholds

## Example Usage
```python

from snapperable import Snapper

def process_item(item):
    # Your processing logic here
    return item * 2

snapper = Snapper(range(1000), process_item)
snapper.start()

# To resume processing later
snapper.start()

# To load results after processing
results = snapper.load()
```

## Storage Classes

`snapperable` provides two built-in storage backends for checkpointing:

### SQLiteSnapshotStorage (Default)

The default storage backend uses SQLite to persist checkpoints. This is the recommended option for most use cases as it's robust and handles concurrent writes safely.

```python
from snapperable import Snapper
from snapperable.storage.sqlite_storage import SQLiteSnapshotStorage

# Create a custom SQLite storage with a specific database path
storage = SQLiteSnapshotStorage(db_path="my_checkpoint.db")

snapper = Snapper(
    range(1000),
    process_item,
    snapshot_storage=storage
)
snapper.start()
```

**Features:**
- Stores checkpoints in a SQLite database
- Handles corrupted data gracefully
- Efficient for large datasets
- Default path: `snapper_checkpoint.db`

### PickleSnapshotStorage

An alternative storage backend that uses Python's pickle module to serialize checkpoints to a file.

```python
from snapperable import Snapper
from snapperable.storage.pickle_storage import PickleSnapshotStorage

# Create a pickle-based storage
storage = PickleSnapshotStorage(file_path="my_checkpoint.pkl")

snapper = Snapper(
    range(1000),
    process_item,
    snapshot_storage=storage
)
snapper.start()
```

**Features:**
- Stores checkpoints in a pickle file
- Simple file-based storage
- Good for smaller datasets
- Default path: `snapper_checkpoint.pkl`

### Batch Processing Configuration

Control when snapshots are saved using batch size and time thresholds:

```python
from snapperable import Snapper

snapper = Snapper(
    range(10000),
    process_item,
    batch_size=100,          # Save every 100 items
    max_wait_time=30.0       # Or save every 30 seconds
)
snapper.start()
```

**Parameters:**
- `batch_size`: Number of processed items to accumulate before saving a snapshot (default: 1)
- `max_wait_time`: Maximum time in seconds to wait before saving, regardless of batch size (default: None)

### Iterable Caching Configuration

By default, `snapperable` processes iterables lazily without materializing them into memory. This allows processing of very large or infinite iterables with minimal memory footprint. However, for certain use cases, you may want to enable caching:

```python
from snapperable import Snapper

# Enable iterable caching for optimization (uses more memory)
snapper = Snapper(
    range(10000),
    process_item,
    cache_iterable=True      # Cache the iterable in memory
)
snapper.start()

# Manually clear the cache to free memory when done
snapper.clear_cache()
```

**When to use `cache_iterable=True`:**
- When you need to call `start()` multiple times on the same Snapper instance
- When the iterable is already in memory (e.g., a list) and you want to avoid re-iterating
- When you have sufficient memory and want to optimize for speed

**When to use `cache_iterable=False` (default):**
- Processing very large iterables that don't fit in memory
- Working with infinite iterables or generators
- When memory efficiency is more important than speed
- When lazy evaluation is important for your use case

**Important Notes:**
- `cache_iterable=False` is the default to support lazy evaluation and memory efficiency
- Caching materializes the entire iterable into memory, which can be problematic for very large datasets
- You can manually clear the cache using `snapper.clear_cache()` to free memory

## Development

### Editable installation

```bash
uv venv
uv pip install -e .
```

### Running Tests

To run the tests, use:

```bash
uv run pytest -v
```

Note: The test simulates an interruption using KeyboardInterrupt. If you see a KeyboardInterrupt in the output, this is expected behavior for the test. The test will catch and handle it internally.

## License
MIT
