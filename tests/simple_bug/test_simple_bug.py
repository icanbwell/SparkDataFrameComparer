from os import path, mkdir
from pathlib import Path
from shutil import rmtree

from pyspark.sql import SparkSession, DataFrame

from spark_data_frame_comparer.spark_data_frame_comparer import (
    assert_compare_data_frames,
)
from tests.conftest import clean_spark_session


def test_simple_bug(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")
    actual_file_path: str = f"{data_dir.joinpath('input/input_simple.json')}"
    expected_file_path: str = f"{data_dir.joinpath('output/output_simple.json')}"

    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    mkdir(temp_folder)

    actual: DataFrame = spark_session.read.json(actual_file_path)
    expected: DataFrame = spark_session.read.json(expected_file_path)

    # Act
    result_path = temp_folder.joinpath("result.json")
    expected.write.json(str(result_path))

    # Assert
    assert_compare_data_frames(
        expected_df=actual,
        result_df=expected,
        expected_path=actual_file_path,
        result_path=result_path,
        func_path_modifier=lambda x: x,
    )
