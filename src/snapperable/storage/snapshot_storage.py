"""Abstract base class for snapshot storage backends."""

from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Any

from snapperable.processing_metrics import ProcessingMetric

T = TypeVar("T")


class SnapshotStorage(ABC, Generic[T]):
    """
    Abstract base class for checkpoint saving/loading in Snapper.
    This class must be extended to support different backends (file, database, etc).
    """

    @abstractmethod
    def store_snapshot(self, processed: list[T], inputs: list[Any]) -> None:
        """
        Save snapshot to storage atomically.
        Args:
            processed: The list of processed items to save.
            inputs: The list of input values corresponding to the processed items.
        """
        pass

    @abstractmethod
    def load_snapshot(self) -> list[T]:
        """
        Load snapshot state.
        Returns:
            A list of processed items.
        """
        pass

    @abstractmethod
    def get_storage_identifier(self) -> str:
        """
        Get a unique identifier for this storage backend.
        This is used to prevent multiple Snapper instances from using storage
        that points to the same underlying file/database.

        Returns:
            A unique string identifier for this storage (typically the absolute file path).
        """
        pass

    @abstractmethod
    def load_inputs(self) -> list[Any]:
        """
        Load all stored input values.
        Returns:
            A list of input values.
        """
        pass

    @abstractmethod
    def load_all_outputs(self) -> list[T]:
        """
        Load all processed outputs from storage, regardless of matching inputs.
        Returns:
            A list of all processed items.
        """
        pass

    @abstractmethod
    def store_metrics(self, metrics: list[ProcessingMetric]) -> None:
        """
        Save per-item processing metrics to storage.
        Args:
            metrics: The list of ProcessingMetric instances to save.
        """
        pass

    @abstractmethod
    def load_metrics(self) -> list[ProcessingMetric]:
        """
        Load all stored per-item processing metrics.
        Returns:
            A list of ProcessingMetric instances.
        """
        pass
