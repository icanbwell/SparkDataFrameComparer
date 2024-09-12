from os import path, mkdir
from pathlib import Path
from shutil import rmtree

import pytest

# noinspection PyProtectedMember
from _pytest._code import ExceptionInfo
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    ArrayType,
    StringType,
)

from spark_data_frame_comparer.spark_data_frame_comparer import (
    assert_compare_data_frames,
)
from spark_data_frame_comparer.spark_data_frame_comparer_exception import (
    SparkDataFrameComparerException,
)
from spark_data_frame_comparer.spark_data_frame_exception_type import ExceptionType
from tests.conftest import clean_spark_session


def test_simple_fail(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('input/complex.json')}"

    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    mkdir(temp_folder)

    # Define the schema
    schema = StructType(
        [
            StructField("simple_field", IntegerType(), True),
            StructField(
                "complex_field",
                ArrayType(
                    StructType(
                        [
                            StructField("inner_key_str", StringType(), True),
                            StructField("inner_key_int", IntegerType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )
    # Define the data
    # Create the DataFrame
    df1 = spark_session.createDataFrame(
        [
            {
                "simple_field": 1,
                "complex_field": [{"inner_key_str": "test", "inner_key_int": 23}],
            }
        ],
        schema,
    )

    df2 = spark_session.createDataFrame(
        [
            {
                "simple_field": 2,
                "complex_field": [{"inner_key_str": "test", "inner_key_int": 23}],
            }
        ],
        schema,
    )

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
    assert exc_info.value.exception_type == ExceptionType.DataMismatch
    assert path.exists(data_dir.joinpath("temp/compare_complex.json.command"))
