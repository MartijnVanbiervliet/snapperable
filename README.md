# snapperable

A Python library for robust processing of long-running iterables with automatic checkpointing.

## Overview

`snapperable` helps you process any iterable that may take a long time to complete by saving intermediate snapshots (checkpoints) to disk. This ensures that progress is never lost, and you can resume processing from the last checkpoint in case of interruption.

## Features
- Process any iterable with a user-defined function
- Automatic periodic checkpointing to disk
- Resume processing from the last saved state
- Simple API: `Snapper(iterable, fn).start()` and `.load()`

## Example Usage
```python

from snapperable import Snapper

def process_item(item):
    # Your processing logic here
    return item * 2

snapper = Snapper(range(1000), process_item)
snapper.start()

# To resume later:
results = snapper.load()
```

## Development

### Running Tests

To run the tests, use:

```bash
uv run pytest -v
```

Note: The test simulates an interruption using KeyboardInterrupt. If you see a KeyboardInterrupt in the output, this is expected behavior for the test. The test will catch and handle it internally.

## License
MIT
