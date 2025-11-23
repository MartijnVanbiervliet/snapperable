"""Base class for snapshot storage backends."""

from typing import TypeVar, Generic

T = TypeVar("T")


class SnapshotStorage(Generic[T]):
    """
    Abstracts checkpoint saving/loading for Snapper.
    This class can be extended to support different backends (file, database, etc).
    """

    def store_snapshot(self, last_index: int, processed: list[T]) -> None:
        """
        Save snapshot to storage.
        Args:
            last_index: The last processed index.
            processed: The list of processed items to save.
        """
        raise NotImplementedError

    def load_snapshot(self) -> list[T]:
        """
        Load snapshot state.
        Returns:
            A list of processed items.
        """
        raise NotImplementedError

    def load_last_index(self) -> int:
        """
        Load only the last processed index, without loading the full processed results.
        Returns:
            The last processed index, or -1 if not available.
        """
        raise NotImplementedError

    def get_storage_identifier(self) -> str:
        """
        Get a unique identifier for this storage backend.
        This is used to prevent multiple Snapper instances from using storage
        that points to the same underlying file/database.
        
        Returns:
            A unique string identifier for this storage (typically the absolute file path).
        """
        raise NotImplementedError
