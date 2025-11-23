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

### SqlLiteSnapshotStorage (Default)

The default storage backend uses SQLite to persist checkpoints. This is the recommended option for most use cases as it's robust and handles concurrent writes safely.

```python
from snapperable import Snapper
from snapperable.storage.sqlite_storage import SqlLiteSnapshotStorage

# Create a custom SQLite storage with a specific database path
storage = SqlLiteSnapshotStorage(db_path="my_checkpoint.db")

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
