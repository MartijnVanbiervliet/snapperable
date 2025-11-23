import os
import pytest
import pytest
from snapperable.storage.pickle_storage import PickleSnapshotStorage
from snapperable.storage.sqlite_storage import SqlLiteSnapshotStorage


class TestPickleSnapshotStorage:
    @pytest.fixture
    def storage(self, tmp_path):
        file_path = tmp_path / "test_snapshot.pkl"
        return PickleSnapshotStorage(file_path)

    def test_save_and_load_snapshot(self, storage: PickleSnapshotStorage):
        # Arrange
        data = {"key": "value"}
        last_index = 1

        # Act
        storage.store_snapshot(last_index, [data])
        loaded_data = storage.load_snapshot()
        loaded_last_index = storage.load_last_index()

        # Assert
        assert loaded_data == [data]
        assert loaded_last_index == last_index

    def test_load_snapshot_when_file_does_not_exist(
        self, storage: PickleSnapshotStorage
    ):
        # Act
        loaded_data = storage.load_snapshot()
        loaded_last_index = storage.load_last_index()

        # Assert
        assert loaded_data == []
        assert loaded_last_index == -1

    def test_save_snapshot_overwrites_existing_file(
        self, storage: PickleSnapshotStorage
    ):
        # Arrange
        data1 = {"key1": "value1"}
        data2 = {"key2": "value2"}
        last_index1 = 1
        last_index2 = 2

        # Act
        storage.store_snapshot(last_index1, [data1])
        storage.store_snapshot(last_index2, [data2])
        loaded_data = storage.load_snapshot()
        loaded_last_index = storage.load_last_index()

        # Assert
        assert loaded_data == [data1, data2]
        assert loaded_last_index == last_index2

    def test_save_snapshot_creates_file(self, storage: PickleSnapshotStorage):
        # Act
        storage.store_snapshot(0, [{"key": "value"}])

        # Assert
        assert os.path.exists(storage.file_path)

    def test_load_snapshot_with_corrupted_file(self, storage: PickleSnapshotStorage):
        # Arrange
        with open(storage.file_path, "wb") as f:
            f.write(b"corrupted data")

        # Act
        loaded_data = storage.load_snapshot()
        loaded_last_index = storage.load_last_index()

        # Assert
        assert loaded_data == []
        assert loaded_last_index == -1


class TestSqlLiteSnapshotStorage:
    @pytest.fixture
    def storage(self, tmp_path):
        db_path = tmp_path / "test_snapshot.db"
        return SqlLiteSnapshotStorage(db_path)

    def test_save_and_load_snapshot(self, storage: SqlLiteSnapshotStorage):
        # Arrange
        data = {"key": "value"}
        last_index = 1

        # Act
        storage.store_snapshot(last_index, [data])
        loaded_data = storage.load_snapshot()
        loaded_last_index = storage.load_last_index()

        # Assert
        assert loaded_data == [data]
        assert loaded_last_index == last_index

    def test_load_snapshot_when_db_is_empty(self, storage: SqlLiteSnapshotStorage):
        # Act
        loaded_data = storage.load_snapshot()
        loaded_last_index = storage.load_last_index()

        # Assert
        assert loaded_data == []
        assert loaded_last_index == -1

    def test_save_snapshot_overwrites_existing_data(
        self, storage: SqlLiteSnapshotStorage
    ):
        # Arrange
        data1 = {"key1": "value1"}
        data2 = {"key2": "value2"}
        last_index1 = 1
        last_index2 = 2

        # Act
        storage.store_snapshot(last_index1, [data1])
        storage.store_snapshot(last_index2, [data2])
        loaded_data = storage.load_snapshot()
        loaded_last_index = storage.load_last_index()

        # Assert
        assert loaded_data == [data1, data2]
        assert loaded_last_index == last_index2

    def test_save_snapshot_creates_db_file(self, storage: SqlLiteSnapshotStorage):
        # Act
        storage.store_snapshot(0, [{"key": "value"}])

        # Assert
        assert os.path.exists(storage.db_path)

    def test_load_snapshot_with_corrupted_db(self, storage: SqlLiteSnapshotStorage):
        # Arrange
        with open(storage.db_path, "wb") as f:
            f.write(b"corrupted data")

        # Act
        loaded_data = storage.load_snapshot()
        loaded_last_index = storage.load_last_index()

        # Assert
        assert loaded_data == []
        assert loaded_last_index == -1
