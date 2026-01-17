import os
import pytest
from snapperable.storage.pickle_storage import PickleSnapshotStorage
from snapperable.storage.sqlite_storage import SQLiteSnapshotStorage


class TestPickleSnapshotStorage:
    @pytest.fixture
    def storage(self, tmp_path):
        file_path = tmp_path / "test_snapshot.pkl"
        return PickleSnapshotStorage(file_path)

    def test_save_and_load_snapshot(self, storage: PickleSnapshotStorage):
        # Arrange
        data = {"key": "value"}
        input_data = "input1"

        # Act
        storage.store_snapshot([data], [input_data])
        loaded_data = storage.load_snapshot()
        loaded_inputs = storage.load_inputs()

        # Assert
        assert loaded_data == [data]
        assert loaded_inputs == [input_data]

    def test_load_snapshot_when_file_does_not_exist(
        self, storage: PickleSnapshotStorage
    ):
        # Act
        loaded_data = storage.load_snapshot()

        # Assert
        assert loaded_data == []

    def test_save_snapshot_overwrites_existing_file(
        self, storage: PickleSnapshotStorage
    ):
        # Arrange
        data1 = {"key1": "value1"}
        data2 = {"key2": "value2"}
        input1 = "input1"
        input2 = "input2"

        # Act
        storage.store_snapshot([data1], [input1])
        storage.store_snapshot([data2], [input2])
        loaded_data = storage.load_snapshot()
        loaded_inputs = storage.load_inputs()

        # Assert
        assert loaded_data == [data1, data2]
        assert loaded_inputs == [input1, input2]

    def test_save_snapshot_creates_file(self, storage: PickleSnapshotStorage):
        # Act
        storage.store_snapshot([{"key": "value"}], ["input1"])

        # Assert
        assert os.path.exists(storage.file_path)

    def test_load_snapshot_with_corrupted_file(self, storage: PickleSnapshotStorage):
        # Arrange
        with open(storage.file_path, "wb") as f:
            f.write(b"corrupted data")

        # Act
        loaded_data = storage.load_snapshot()

        # Assert
        assert loaded_data == []


class TestSQLiteSnapshotStorage:
    @pytest.fixture
    def storage(self, tmp_path):
        db_path = tmp_path / "test_snapshot.db"
        return SQLiteSnapshotStorage(db_path)

    def test_save_and_load_snapshot(self, storage: SQLiteSnapshotStorage):
        # Arrange
        data = {"key": "value"}
        input_data = "input1"

        # Act
        storage.store_snapshot([data], [input_data])
        loaded_data = storage.load_snapshot()
        loaded_inputs = storage.load_inputs()

        # Assert
        assert loaded_data == [data]
        assert loaded_inputs == [input_data]

    def test_load_snapshot_when_db_is_empty(self, storage: SQLiteSnapshotStorage):
        # Act
        loaded_data = storage.load_snapshot()

        # Assert
        assert loaded_data == []

    def test_save_snapshot_overwrites_existing_data(
        self, storage: SQLiteSnapshotStorage
    ):
        # Arrange
        data1 = {"key1": "value1"}
        data2 = {"key2": "value2"}
        input1 = "input1"
        input2 = "input2"

        # Act
        storage.store_snapshot([data1], [input1])
        storage.store_snapshot([data2], [input2])
        loaded_data = storage.load_snapshot()
        loaded_inputs = storage.load_inputs()

        # Assert
        assert loaded_data == [data1, data2]
        assert loaded_inputs == [input1, input2]

    def test_save_snapshot_creates_db_file(self, storage: SQLiteSnapshotStorage):
        # Act
        storage.store_snapshot([{"key": "value"}], ["input1"])

        # Assert
        assert os.path.exists(storage.db_path)

    def test_load_snapshot_with_corrupted_db(self, storage: SQLiteSnapshotStorage):
        # Arrange
        with open(storage.db_path, "wb") as f:
            f.write(b"corrupted data")

        # Act
        loaded_data = storage.load_snapshot()

        # Assert
        assert loaded_data == []
