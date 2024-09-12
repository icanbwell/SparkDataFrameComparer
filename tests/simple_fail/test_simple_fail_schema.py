from os import path, mkdir
from pathlib import Path
from shutil import rmtree

import pytest

# noinspection PyProtectedMember
from _pytest._code import ExceptionInfo
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

from spark_data_frame_comparer.spark_data_frame_comparer import (
    assert_compare_data_frames,
)
from spark_data_frame_comparer.spark_data_frame_comparer_exception import (
    SparkDataFrameComparerException,
)
from spark_data_frame_comparer.spark_data_frame_exception_type import ExceptionType
from tests.conftest import clean_spark_session


def test_simple_fail_schema(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('input/complex.json')}"

    temp_folder = data_dir.joinpath("./temp")
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
    exc_info: ExceptionInfo[SparkDataFrameComparerException]
    with pytest.raises(SparkDataFrameComparerException) as exc_info:
        assert_compare_data_frames(
            expected_df=df1,
            result_df=df2,
            expected_path=test_file_path,
            result_path=result_path,
            temp_folder=temp_folder,
            func_path_modifier=lambda x: x,
        )

    spark_data_frame_exception: SparkDataFrameComparerException = exc_info.value
    print(spark_data_frame_exception)
    assert exc_info.value.exception_type == ExceptionType.SchemaMismatch
    assert not path.exists(data_dir.joinpath("temp/compare_complex.json.command"))
