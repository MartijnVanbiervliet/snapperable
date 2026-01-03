# Implementation Summary: Dynamic Iterables Support

## Overview
This implementation adds robust support for dynamic iterables to Snapperable, allowing the library to handle iterables that can grow, shrink, or change between processing runs.

## Key Features Implemented

### 1. Input-Based Tracking
- **Previous behavior**: Tracked progress using only the index of the iterable
- **New behavior**: Stores actual input values for each processed item
- **Benefit**: Can detect which items have been processed regardless of iterable changes

### 2. Function Versioning
- Uses SHA-256 hashing of function source code or bytecode
- Automatically detects when the processing function changes
- Reprocesses items when function version changes
- Ensures outputs are always produced with the latest function version

### 3. Enhanced Output Retrieval
- **`load()`**: Returns outputs matching the current input sequence
- **`load_all()`**: Returns all stored outputs regardless of current inputs
- Useful for scenarios where the iterable changes between runs

### 4. Atomic Input/Output Storage
- Modified `BatchProcessor` to store inputs and outputs together
- Ensures consistency even if processing is interrupted
- Inputs are only stored after successful output generation

## Implementation Details

### Modified Files

1. **`src/snapperable/storage/snapshot_storage.py`**
   - Extended abstract base class with new methods:
     - `store_input()` / `load_inputs()`
     - `store_function_version()` / `load_function_version()`
     - `load_all_outputs()`

2. **`src/snapperable/storage/sqlite_storage.py`**
   - Added new database tables for inputs and function versions
   - Implemented new storage methods for SQLite backend

3. **`src/snapperable/storage/pickle_storage.py`**
   - Extended pickle format to store inputs and function versions
   - Refactored to use helper methods `_load_data()` and `_save_data()`

4. **`src/snapperable/snapper.py`**
   - Added `compute_function_version()` utility function
   - Modified `start()` to use input-based tracking
   - Added `load_all()` method for retrieving all outputs
   - Enhanced `load()` to match outputs with current inputs
   - Added helper methods for input comparison and hashable conversion

5. **`src/snapperable/batch_processor.py`**
   - Modified to accept and store input values alongside outputs
   - Updated to store inputs and outputs atomically
   - Maintains backward compatibility with None inputs

### New Test Files

1. **`tests/test_dynamic_iterables.py`**
   - 9 comprehensive test cases covering:
     - Growing iterables
     - Shrinking iterables
     - Reordered iterables
     - Function version changes
     - Duplicate handling
     - Complex input types
     - Output retrieval methods

### Documentation

1. **`README.md`**
   - Added section on Dynamic Iterable Support
   - Examples of growing iterables
   - Documentation of `load()` vs `load_all()`
   - Function version detection examples

2. **`demo_dynamic_iterables.py`**
   - Interactive demonstration script
   - Shows all major features in action
   - Includes 4 different demo scenarios

## Test Results

- **All 29 existing tests pass**: No breaking changes
- **9 new tests pass**: Comprehensive coverage of new features
- **Code quality**: All review feedback addressed
- **Security**: No vulnerabilities found by CodeQL

## Backward Compatibility

All changes are backward compatible:
- Existing code continues to work without modification
- Old checkpoints can be loaded (handled gracefully)
- Optional input tracking (backward compatible with None)

## Performance Considerations

- Input hashing is efficient for most data types
- Function version computation is done once per run
- `load()` materializes the iterable for comparison (documented)
- `load_all()` provides fast retrieval without input matching

## Usage Examples

### Growing Iterable
```python
# First run: process 5 items
data_v1 = list(range(5))
snapper = Snapper(data_v1, process_fn, snapshot_storage=storage)
snapper.start()

# Second run: process 10 items (only new ones processed)
data_v2 = list(range(10))
snapper = Snapper(data_v2, process_fn, snapshot_storage=storage)
snapper.start()  # Only processes items 5-9
```

### Function Version Change
```python
# First run with one function
def process_v1(x): return x * 2
snapper = Snapper(data, process_v1, snapshot_storage=storage)
snapper.start()

# Second run with different function (reprocesses all)
def process_v2(x): return x * 3
snapper = Snapper(data, process_v2, snapshot_storage=storage)
snapper.start()  # Reprocesses everything with new function
```

### Output Retrieval
```python
snapper = Snapper(current_data, process_fn, snapshot_storage=storage)
matching = snapper.load()      # Outputs matching current inputs
all_outputs = snapper.load_all()  # All stored outputs
```

## Future Enhancements

Possible future improvements:
1. Incremental input comparison (avoid materializing entire iterable)
2. Support for custom hash functions for input comparison
3. Input deduplication strategies for better storage efficiency
4. Metadata tracking (timestamps, processing duration, etc.)
