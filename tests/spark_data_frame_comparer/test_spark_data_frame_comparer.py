from pathlib import Path
from typing import Union

import pytest
from pyspark.sql.types import Row
from pyspark.sql import SparkSession, DataFrame

from spark_data_frame_comparer.spark_data_frame_comparer import (
    assert_compare_data_frames,
)
from spark_data_frame_comparer.spark_data_frame_comparer_exception import (
    SparkDataFrameComparerException,
)
from spark_data_frame_comparer.spark_data_frame_exception_type import ExceptionType


@pytest.fixture
def sample_data(spark_session: SparkSession) -> DataFrame:
    """Creates a sample DataFrame for testing."""
    data = [
        Row(id=1, name="Alice", age=30),
        Row(id=2, name="Bob", age=25),
        Row(id=3, name="Charlie", age=35),
    ]
    return spark_session.createDataFrame(data)


@pytest.fixture
def modified_data(spark_session: SparkSession) -> DataFrame:
    """Creates a slightly different DataFrame for testing."""
    data = [
        Row(id=1, name="Alice", age=30),
        Row(id=2, name="Bob", age=27),  # different age
        Row(id=3, name="Charlie", age=35),
    ]
    return spark_session.createDataFrame(data)


@pytest.fixture
def reordered_data(spark_session: SparkSession) -> DataFrame:
    """Creates a DataFrame with columns in a different order."""
    data = [
        Row(name="Alice", age=30, id=1),
        Row(name="Bob", age=25, id=2),
        Row(name="Charlie", age=35, id=3),
    ]
    return spark_session.createDataFrame(data)


def test_compare_equal_dataframes(sample_data: DataFrame) -> None:
    """Test if two identical DataFrames pass without error."""
    try:
        assert_compare_data_frames(sample_data, sample_data)
    except SparkDataFrameComparerException:
        pytest.fail("DataFrames should be equal but exception was raised.")


def test_compare_unequal_dataframes(
    sample_data: DataFrame, modified_data: DataFrame
) -> None:
    """Test if a SparkDataFrameComparerException is raised for unequal DataFrames."""
    with pytest.raises(SparkDataFrameComparerException) as excinfo:
        assert_compare_data_frames(sample_data, modified_data)

    assert excinfo.value.exception_type == ExceptionType.DataMismatch


def test_compare_different_column_order(
    sample_data: DataFrame, reordered_data: DataFrame
) -> None:
    """Test if the DataFrames with columns in different order pass."""
    try:
        assert_compare_data_frames(sample_data, reordered_data, order_by=["id"])
    except SparkDataFrameComparerException:
        pytest.fail(
            "DataFrames should be equal but exception was raised due to column order."
        )


def test_column_mismatch(sample_data: DataFrame, spark_session: SparkSession) -> None:
    """Test if the exception is raised when columns mismatch."""
    additional_column_df = spark_session.createDataFrame(
        [Row(id=1, name="Alice", age=30, gender="F")]
    )

    with pytest.raises(SparkDataFrameComparerException) as excinfo:
        assert_compare_data_frames(sample_data, additional_column_df)

    assert excinfo.value.exception_type == ExceptionType.SchemaMismatch
    assert "Columns not matched" in str(excinfo.value)


def test_row_count_mismatch(
    sample_data: DataFrame, spark_session: SparkSession
) -> None:
    """Test if the exception is raised when row count is different."""
    fewer_rows_df = spark_session.createDataFrame([Row(id=1, name="Alice", age=30)])

    with pytest.raises(SparkDataFrameComparerException) as excinfo:
        assert_compare_data_frames(sample_data, fewer_rows_df)

    assert excinfo.value.exception_type == ExceptionType.RowMismatch
    assert "Expected 3 rows, actual 1 rows" in str(excinfo.value)


def test_exclude_columns(sample_data: DataFrame) -> None:
    """Test exclude_columns parameter to ensure columns are excluded from comparison."""
    try:
        assert_compare_data_frames(sample_data, sample_data, exclude_columns=["age"])
    except SparkDataFrameComparerException:
        pytest.fail("DataFrames should be equal after excluding 'age' column.")


def test_include_columns(sample_data: DataFrame) -> None:
    """Test include_columns parameter to ensure only specific columns are included in comparison."""
    try:
        assert_compare_data_frames(
            sample_data, sample_data, include_columns=["id", "name"]
        )
    except SparkDataFrameComparerException:
        pytest.fail(
            "DataFrames should be equal when only 'id' and 'name' are compared."
        )


def test_auto_sort(sample_data: DataFrame, reordered_data: DataFrame) -> None:
    """Test if auto_sort correctly sorts the DataFrame."""
    try:
        assert_compare_data_frames(sample_data, reordered_data, auto_sort=True)
    except SparkDataFrameComparerException:
        pytest.fail("DataFrames should match after auto sorting.")


def test_temp_no_file_creation_on_pass(sample_data: DataFrame, tmpdir: Path) -> None:
    """Test if the temp file is created correctly for comparison."""
    temp_folder: Union[Path, str] = tmpdir
    try:
        assert_compare_data_frames(
            sample_data,
            sample_data,
            expected_path="/expected",
            result_path="/result",
            temp_folder=temp_folder,
        )
    except SparkDataFrameComparerException:
        pytest.fail("Comparison should pass without error.")
    # Check if the temp file was created
    temp_file_path = Path(temp_folder) / "compare_expected.command"
    assert not temp_file_path.exists(), "Temporary compare file was not created."


def test_temp_file_creation_on_fail(
    sample_data: DataFrame, modified_data: DataFrame, tmpdir: Path
) -> None:
    """Test if the temp file is created correctly for comparison."""
    temp_folder: Union[Path, str] = tmpdir

    with pytest.raises(SparkDataFrameComparerException):
        assert_compare_data_frames(
            sample_data,
            modified_data,
            expected_path="/expected",
            result_path="/result",
            temp_folder=temp_folder,
        )

    # Check if the temp file was created
    temp_file_path = Path(temp_folder) / "compare_expected.command"
    assert temp_file_path.exists(), "Temporary compare file was not created."
