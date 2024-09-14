from os import path, mkdir
from pathlib import Path
from shutil import rmtree

from pyspark.sql import SparkSession, DataFrame

from spark_data_frame_comparer.spark_data_frame_sorter import SparkDataFrameSorter
from tests.conftest import clean_spark_session


def test_data_frame_sorter(spark_session: SparkSession) -> None:
    # Arrange
    clean_spark_session(spark_session)
    data_dir: Path = Path(__file__).parent.joinpath("./")
    test_file_path: str = f"{data_dir.joinpath('input/complex.json')}"

    temp_folder = data_dir.joinpath("./temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)
    mkdir(temp_folder)

    df1: DataFrame = spark_session.read.json(test_file_path)
    result_df: DataFrame = SparkDataFrameSorter.deterministic_sort(df=df1)

    print("------- result ----")
    result_df.show()

    results = [row.asDict(recursive=True) for row in result_df.collect()]

    assert len(results) == 2
    assert results[0]["simple_field"] == 1
    assert results[1]["simple_field"] == 2
