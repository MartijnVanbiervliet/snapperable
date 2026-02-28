"""Tests for per-item processing metrics collection and report generation."""

import json
import time
from pathlib import Path

import pytest

from snapperable import Snapper, ProcessingMetric, generate_metrics_report, generate_json_report, generate_markdown_report
from snapperable.storage.pickle_storage import PickleSnapshotStorage
from snapperable.storage.sqlite_storage import SQLiteSnapshotStorage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class ItemError(Exception):
    """Simulates a per-item data error."""


def _pickle_storage(tmp_path: Path, name: str = "test.pkl") -> PickleSnapshotStorage:
    return PickleSnapshotStorage(str(tmp_path / name))


def _sqlite_storage(tmp_path: Path, name: str = "test.db") -> SQLiteSnapshotStorage:
    return SQLiteSnapshotStorage(str(tmp_path / name))


# ---------------------------------------------------------------------------
# ProcessingMetric dataclass
# ---------------------------------------------------------------------------


def test_processing_metric_duration():
    metric = ProcessingMetric(
        input_item=1, start_time=1000.0, end_time=1001.5, success=True
    )
    assert metric.duration == pytest.approx(1.5)


def test_processing_metric_success_fields():
    metric = ProcessingMetric(
        input_item="x", start_time=0.0, end_time=0.1, success=True
    )
    assert metric.success is True
    assert metric.error_message is None


def test_processing_metric_failure_fields():
    metric = ProcessingMetric(
        input_item=42,
        start_time=0.0,
        end_time=0.2,
        success=False,
        error_message="something went wrong",
    )
    assert metric.success is False
    assert metric.error_message == "something went wrong"


# ---------------------------------------------------------------------------
# Metrics stored for successful items – SQLite
# ---------------------------------------------------------------------------


def test_metrics_stored_for_successful_items_sqlite(tmp_path):
    storage = _sqlite_storage(tmp_path)
    snapper = Snapper(range(3), lambda x: x * 2, snapshot_storage=storage)
    snapper.start()

    metrics = storage.load_metrics()
    assert len(metrics) == 3
    assert all(isinstance(m, ProcessingMetric) for m in metrics)
    assert all(m.success for m in metrics)
    assert {m.input_item for m in metrics} == {0, 1, 2}


def test_metrics_stored_for_successful_items_pickle(tmp_path):
    storage = _pickle_storage(tmp_path)
    snapper = Snapper(range(3), lambda x: x * 2, snapshot_storage=storage)
    snapper.start()

    metrics = storage.load_metrics()
    assert len(metrics) == 3
    assert all(m.success for m in metrics)
    assert {m.input_item for m in metrics} == {0, 1, 2}


# ---------------------------------------------------------------------------
# Metrics stored for failed items (skip_item_errors=True)
# ---------------------------------------------------------------------------


def test_metrics_stored_for_failed_items(tmp_path):
    fail_on = {1, 3}

    def process(x):
        if x in fail_on:
            raise ItemError(f"bad item {x}")
        return x * 2

    storage = _sqlite_storage(tmp_path)
    snapper = Snapper(
        range(5), process, snapshot_storage=storage, skip_item_errors=True
    )
    snapper.start()

    metrics = storage.load_metrics()
    assert len(metrics) == 5

    failed = [m for m in metrics if not m.success]
    succeeded = [m for m in metrics if m.success]

    assert len(failed) == 2
    assert len(succeeded) == 3
    assert {m.input_item for m in failed} == fail_on
    assert all(m.error_message for m in failed)


def test_failed_metric_error_message_contains_exception_text(tmp_path):
    def process(x):
        raise ItemError(f"error for item {x}")

    storage = _sqlite_storage(tmp_path)
    snapper = Snapper([7], process, snapshot_storage=storage, skip_item_errors=True)
    snapper.start()

    metrics = storage.load_metrics()
    assert len(metrics) == 1
    assert not metrics[0].success
    assert "error for item 7" in metrics[0].error_message


# ---------------------------------------------------------------------------
# Timing fields
# ---------------------------------------------------------------------------


def test_metrics_timing_fields(tmp_path):
    def slow_fn(x):
        time.sleep(0.01)
        return x

    storage = _sqlite_storage(tmp_path)
    before = time.time()
    snapper = Snapper([1], slow_fn, snapshot_storage=storage)
    snapper.start()
    after = time.time()

    metrics = storage.load_metrics()
    assert len(metrics) == 1
    m = metrics[0]
    assert m.start_time >= before
    assert m.end_time <= after
    assert m.duration >= 0.01


# ---------------------------------------------------------------------------
# Snapper.load_metrics()
# ---------------------------------------------------------------------------


def test_snapper_load_metrics(tmp_path):
    storage = _sqlite_storage(tmp_path)
    snapper = Snapper(range(4), lambda x: x, snapshot_storage=storage)
    snapper.start()

    metrics = snapper.load_metrics()
    assert len(metrics) == 4
    assert all(isinstance(m, ProcessingMetric) for m in metrics)


def test_load_metrics_empty_when_nothing_processed(tmp_path):
    storage = _sqlite_storage(tmp_path)
    snapper = Snapper([], lambda x: x, snapshot_storage=storage)
    snapper.start()

    metrics = snapper.load_metrics()
    assert metrics == []


# ---------------------------------------------------------------------------
# Snapper.generate_report()
# ---------------------------------------------------------------------------


def test_generate_report_markdown(tmp_path):
    storage = _sqlite_storage(tmp_path)
    snapper = Snapper(range(3), lambda x: x, snapshot_storage=storage)
    snapper.start()

    report = snapper.generate_report(format="markdown")
    assert "# Processing Metrics Report" in report
    assert "3" in report  # total items


def test_generate_report_json(tmp_path):
    storage = _sqlite_storage(tmp_path)
    snapper = Snapper(range(3), lambda x: x, snapshot_storage=storage)
    snapper.start()

    report = snapper.generate_report(format="json")
    data = json.loads(report)
    assert data["total_items"] == 3
    assert data["failed_count"] == 0


def test_generate_report_unsupported_format_raises(tmp_path):
    storage = _sqlite_storage(tmp_path)
    snapper = Snapper([1], lambda x: x, snapshot_storage=storage)
    snapper.start()

    with pytest.raises(ValueError, match="Unsupported report format"):
        snapper.generate_report(format="csv")


# ---------------------------------------------------------------------------
# generate_metrics_report() standalone function
# ---------------------------------------------------------------------------


def test_generate_metrics_report_empty():
    report = generate_metrics_report([])
    assert report["total_items"] == 0
    assert report["failed_count"] == 0
    assert report["avg_duration"] is None


def test_generate_metrics_report_all_success():
    metrics = [
        ProcessingMetric(input_item=i, start_time=float(i), end_time=float(i) + 0.1, success=True)
        for i in range(5)
    ]
    report = generate_metrics_report(metrics)
    assert report["total_items"] == 5
    assert report["successful_items"] == 5
    assert report["failed_count"] == 0
    assert report["avg_duration"] == pytest.approx(0.1)


def test_generate_metrics_report_with_failures():
    metrics = [
        ProcessingMetric(input_item=0, start_time=0.0, end_time=0.1, success=True),
        ProcessingMetric(input_item=1, start_time=0.1, end_time=0.2, success=False, error_message="oops"),
    ]
    report = generate_metrics_report(metrics)
    assert report["total_items"] == 2
    assert report["failed_count"] == 1
    assert len(report["failed_items"]) == 1
    assert report["failed_items"][0]["error_message"] == "oops"


def test_generate_metrics_report_outliers():
    # 10 fast items + 1 very slow outlier – enough data for 2σ detection to trigger
    metrics = [
        ProcessingMetric(input_item=i, start_time=0.0, end_time=0.01, success=True)
        for i in range(10)
    ]
    metrics.append(
        ProcessingMetric(input_item=10, start_time=0.0, end_time=1000.0, success=True)
    )
    report = generate_metrics_report(metrics)
    assert len(report["slow_outliers"]) >= 1
    assert report["slow_outliers"][0]["input_item"] == repr(10)


def test_generate_metrics_report_time_range():
    metrics = [
        ProcessingMetric(input_item=0, start_time=1000.0, end_time=1001.0, success=True),
        ProcessingMetric(input_item=1, start_time=1002.0, end_time=1005.0, success=True),
    ]
    report = generate_metrics_report(metrics)
    assert report["total_elapsed"] == pytest.approx(5.0)  # 1005.0 - 1000.0


# ---------------------------------------------------------------------------
# generate_json_report() standalone function
# ---------------------------------------------------------------------------


def test_generate_json_report_returns_valid_json():
    metrics = [
        ProcessingMetric(input_item=1, start_time=0.0, end_time=0.5, success=True)
    ]
    result = generate_json_report(metrics)
    data = json.loads(result)
    assert data["total_items"] == 1


# ---------------------------------------------------------------------------
# generate_markdown_report() standalone function
# ---------------------------------------------------------------------------


def test_generate_markdown_report_contains_summary():
    metrics = [
        ProcessingMetric(input_item=1, start_time=0.0, end_time=0.5, success=True),
        ProcessingMetric(input_item=2, start_time=0.5, end_time=1.0, success=False, error_message="err"),
    ]
    result = generate_markdown_report(metrics)
    assert "# Processing Metrics Report" in result
    assert "## Summary" in result
    assert "## Failed Items" in result
    assert "err" in result


def test_generate_markdown_report_empty():
    result = generate_markdown_report([])
    assert "No items were processed" in result


# ---------------------------------------------------------------------------
# Metrics persist across runs (resume scenario)
# ---------------------------------------------------------------------------


def test_metrics_accumulate_across_runs(tmp_path):
    storage = _sqlite_storage(tmp_path)

    # First run: process items 0-2
    with Snapper([0, 1, 2], lambda x: x, snapshot_storage=storage) as snapper1:
        snapper1.start()

    # Second run: process items 3-4 (new items added to iterable)
    with Snapper([0, 1, 2, 3, 4], lambda x: x, snapshot_storage=storage) as snapper2:
        snapper2.start()
        metrics = snapper2.load_metrics()

    # All 5 items should have metrics
    assert len(metrics) == 5
