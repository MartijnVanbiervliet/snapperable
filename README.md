# snapperable

A Python library for robust processing of long-running iterables with automatic checkpointing.

## Overview

`snapperable` helps you process any iterable that may take a long time to complete by saving intermediate snapshots (checkpoints) to disk. This ensures that progress is never lost, and you can resume processing from the last checkpoint in case of interruption.

## Features
- Process any iterable with a user-defined function
- Automatic periodic checkpointing to disk
- Resume processing from the last saved state
- **Input-based tracking** for dynamic iterables that can grow or change
- **Function versioning** to detect and handle changes in processing logic
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

## Dynamic Iterable Support

Snapperable now supports dynamic iterables that can grow, shrink, or change between runs:

```python
from snapperable import Snapper
from snapperable.storage.pickle_storage import PickleSnapshotStorage

def process_item(item):
    return item * 2

storage = PickleSnapshotStorage("checkpoint.pkl")

# First run: process initial items
data_v1 = [1, 2, 3, 4, 5]
with Snapper(data_v1, process_item, snapshot_storage=storage) as snapper:
    snapper.start()

# Second run: add more items to the iterable
data_v2 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]  # New items added
with Snapper(data_v2, process_item, snapshot_storage=storage) as snapper:
    snapper.start()  # Only processes items 6-10
    results = snapper.load()  # Returns all 10 results
```

### Load Methods

Snapperable provides two methods for retrieving results:

- **`load()`**: Returns outputs that match the current input sequence
- **`load_all()`**: Returns all stored outputs, regardless of current inputs

```python
# After processing [1, 2, 3, 4, 5]
with Snapper([1, 2, 3], process_item, snapshot_storage=storage) as snapper:
    matching_results = snapper.load()      # [2, 4, 6] - matches current inputs
    all_results = snapper.load_all()       # [2, 4, 6, 8, 10] - all stored outputs
```

### Function Version Detection

Snapperable automatically detects when your processing function changes and reprocesses items accordingly:

```python
# First run with one function
def process_v1(x):
    return x * 2

snapper = Snapper([1, 2, 3], process_v1, snapshot_storage=storage)
snapper.start()  # Results: [2, 4, 6]

# Second run with a different function
def process_v2(x):
    return x * 3

snapper = Snapper([1, 2, 3], process_v2, snapshot_storage=storage)
snapper.start()  # Reprocesses all items, results: [3, 6, 9]
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
