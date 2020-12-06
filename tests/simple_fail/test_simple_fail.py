from os import path, mkdir
from pathlib import Path
from shutil import rmtree

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

from spark_data_frame_comparer.spark_data_frame_comparer import assert_compare_data_frames
from spark_data_frame_comparer.spark_data_frame_comparer_exception import SparkDataFrameComparerException
from tests.conftest import clean_spark_session


def test_can_compare_simple_fail(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath('./')
    test_file_path: str = f"{data_dir.joinpath('input/complex.json')}"

    temp_folder = data_dir.joinpath('./temp')
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    mkdir(temp_folder)

    df1: DataFrame = spark_session.read.json(test_file_path)
    df2: DataFrame = spark_session.read.json(test_file_path)
    df2 = df2.withColumn("simple_field", lit("1"))

    # Act
    result_path = temp_folder.joinpath("result.json")
    df2.write.json(str(result_path))

    # Assert
    with pytest.raises(SparkDataFrameComparerException):
        assert_compare_data_frames(
            expected_df=df1,
            result_df=df2,
            expected_path=test_file_path,
            result_path=result_path,
            temp_folder=temp_folder,
            func_path_modifier=lambda x: x
        )

    assert path.exists(data_dir.joinpath("temp/compare_complex.json.command"))
    with open(
        data_dir.joinpath("temp/compare_complex.json.command"), "r"
    ) as file:
        line: str = file.read()
        assert line == "/usr/local/bin/charm diff " + \
                       str(data_dir.joinpath("temp/result.json")) + " " + \
                       str(data_dir.joinpath("input/complex.json"))
