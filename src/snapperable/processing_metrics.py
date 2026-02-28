"""Per-item processing metrics collection and report generation."""

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import statistics
from typing import Any


@dataclass
class ProcessingMetric:
    """
    Holds timing and result information for a single processed item.

    Attributes:
        input_item: The input value that was processed.
        start_time: Unix timestamp when processing started.
        end_time: Unix timestamp when processing finished.
        success: True if the item was processed successfully, False if it failed.
        error_message: The error message if processing failed, None otherwise.
    """

    input_item: Any
    start_time: float
    end_time: float
    success: bool
    error_message: str | None = None

    @property
    def duration(self) -> float:
        """Processing duration in seconds."""
        return self.end_time - self.start_time

    def to_dict(self) -> dict:
        """
        Serialise the metric to a JSON-compatible dictionary.

        ``input_item`` is converted with ``repr()`` when it is not natively
        JSON-serialisable so that the serialised format never depends on pickle.
        """
        try:
            serialised_input = json.loads(json.dumps(self.input_item))
        except (TypeError, ValueError):
            serialised_input = repr(self.input_item)
        return {
            "input_item": serialised_input,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "success": self.success,
            "error_message": self.error_message,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ProcessingMetric":
        """
        Reconstruct a ProcessingMetric from a dictionary produced by :meth:`to_dict`.

        Unknown keys are ignored so that future additions to the format do not
        break older readers.
        """
        return cls(
            input_item=data.get("input_item"),
            start_time=data["start_time"],
            end_time=data["end_time"],
            success=data["success"],
            error_message=data.get("error_message"),
        )


def generate_metrics_report(metrics: list[ProcessingMetric]) -> dict:
    """
    Generate a summary report from a list of ProcessingMetric instances.

    Args:
        metrics: List of ProcessingMetric instances collected during processing.

    Returns:
        A dictionary containing:
            - total_items: Total number of items processed.
            - successful_items: Number of items processed successfully.
            - failed_count: Number of items that failed.
            - avg_duration: Average processing duration in seconds.
            - min_duration: Minimum processing duration in seconds.
            - max_duration: Maximum processing duration in seconds.
            - processing_start: ISO 8601 timestamp of the earliest item start time.
            - processing_end: ISO 8601 timestamp of the latest item end time.
            - total_elapsed: Total elapsed wall-clock time in seconds.
            - slow_outliers: List of items with unusually long processing times.
            - fast_outliers: List of items with unusually short processing times.
            - failed_items: List of dicts with input_item and error_message for each failure.
    """
    if not metrics:
        return {
            "total_items": 0,
            "successful_items": 0,
            "failed_count": 0,
            "avg_duration": None,
            "min_duration": None,
            "max_duration": None,
            "processing_start": None,
            "processing_end": None,
            "total_elapsed": None,
            "slow_outliers": [],
            "fast_outliers": [],
            "failed_items": [],
        }

    durations = [m.duration for m in metrics]
    avg_duration = sum(durations) / len(durations)
    min_duration = min(durations)
    max_duration = max(durations)
    processing_start = min(m.start_time for m in metrics)
    processing_end = max(m.end_time for m in metrics)
    total_elapsed = processing_end - processing_start

    failed = [m for m in metrics if not m.success]

    # Identify outliers using mean ± 2 standard deviations (requires ≥ 2 items)
    slow_outliers: list[dict] = []
    fast_outliers: list[dict] = []
    if len(durations) >= 2:
        mean = statistics.mean(durations)
        stdev = statistics.stdev(durations)
        for m in metrics:
            if m.duration > mean + 2 * stdev:
                slow_outliers.append(
                    {"input_item": repr(m.input_item), "duration": m.duration}
                )
            elif m.duration < mean - 2 * stdev:
                fast_outliers.append(
                    {"input_item": repr(m.input_item), "duration": m.duration}
                )

    return {
        "total_items": len(metrics),
        "successful_items": len(metrics) - len(failed),
        "failed_count": len(failed),
        "avg_duration": avg_duration,
        "min_duration": min_duration,
        "max_duration": max_duration,
        "processing_start": datetime.fromtimestamp(
            processing_start, tz=timezone.utc
        ).isoformat(),
        "processing_end": datetime.fromtimestamp(
            processing_end, tz=timezone.utc
        ).isoformat(),
        "total_elapsed": total_elapsed,
        "slow_outliers": slow_outliers,
        "fast_outliers": fast_outliers,
        "failed_items": [
            {"input_item": repr(m.input_item), "error_message": m.error_message}
            for m in failed
        ],
    }


def generate_json_report(metrics: list[ProcessingMetric]) -> str:
    """
    Generate a JSON string report from a list of ProcessingMetric instances.

    Args:
        metrics: List of ProcessingMetric instances collected during processing.

    Returns:
        A JSON string containing the metrics summary report.
    """
    return json.dumps(generate_metrics_report(metrics), indent=2)


def generate_markdown_report(metrics: list[ProcessingMetric]) -> str:
    """
    Generate a Markdown string report from a list of ProcessingMetric instances.

    Args:
        metrics: List of ProcessingMetric instances collected during processing.

    Returns:
        A Markdown string containing the metrics summary report.
    """
    report = generate_metrics_report(metrics)

    lines = ["# Processing Metrics Report", ""]

    if report["total_items"] == 0:
        lines.append("No items were processed.")
        return "\n".join(lines)

    lines += [
        "## Summary",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total items processed | {report['total_items']} |",
        f"| Successful | {report['successful_items']} |",
        f"| Failed | {report['failed_count']} |",
        f"| Average duration | {report['avg_duration']:.4f}s |",
        f"| Min duration | {report['min_duration']:.4f}s |",
        f"| Max duration | {report['max_duration']:.4f}s |",
        f"| Processing start | {report['processing_start']} |",
        f"| Processing end | {report['processing_end']} |",
        f"| Total elapsed | {report['total_elapsed']:.4f}s |",
        "",
    ]

    if report["slow_outliers"]:
        lines += ["## Slow Outliers (> mean + 2σ)", ""]
        lines += [
            f"| Input Item | Duration |",
            f"|------------|----------|",
        ]
        for item in report["slow_outliers"]:
            lines.append(f"| {item['input_item']} | {item['duration']:.4f}s |")
        lines.append("")

    if report["fast_outliers"]:
        lines += ["## Fast Outliers (< mean − 2σ)", ""]
        lines += [
            f"| Input Item | Duration |",
            f"|------------|----------|",
        ]
        for item in report["fast_outliers"]:
            lines.append(f"| {item['input_item']} | {item['duration']:.4f}s |")
        lines.append("")

    if report["failed_items"]:
        lines += ["## Failed Items", ""]
        lines += [
            f"| Input Item | Error |",
            f"|------------|-------|",
        ]
        for item in report["failed_items"]:
            lines.append(f"| {item['input_item']} | {item['error_message']} |")
        lines.append("")

    return "\n".join(lines)
