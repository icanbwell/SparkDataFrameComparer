import os
from pathlib import Path
from typing import List, Tuple, Optional, Union, Callable, Dict

from pyspark.sql import DataFrame
from pyspark.sql.types import StructField, Row

from spark_data_frame_comparer.list_utils import diff_lists
from spark_data_frame_comparer.spark_data_frame_comparer_exception import (
    SparkDataFrameComparerException,
)
from spark_data_frame_comparer.spark_data_frame_comparer_helper import (
    SparkDataFrameComparerHelper,
)
from spark_data_frame_comparer.spark_data_frame_error import (
    SparkDataFrameError,
)
from spark_data_frame_comparer.spark_data_frame_exception_type import ExceptionType
from spark_data_frame_comparer.spark_data_frame_sorter import SparkDataFrameSorter


def assert_compare_data_frames(
    expected_df: DataFrame,
    result_df: DataFrame,
    exclude_columns: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    order_by: Optional[List[str]] = None,
    expected_path: Optional[Union[Path, str]] = None,
    result_path: Optional[Union[Path, str]] = None,
    temp_folder: Optional[Union[Path, str]] = None,
    func_path_modifier: Optional[Callable[[Union[Path, str]], Union[Path, str]]] = None,
    auto_sort: Optional[bool] = None,
) -> None:
    """
    Compare two data frames and throws an exception if there is any difference

    :param func_path_modifier:
    :param expected_df:
    :param result_df:
    :param exclude_columns:
    :param include_columns:
    :param order_by:
    :param expected_path:
    :param result_path:
    :param temp_folder:
    :param auto_sort: whether to automatically sort each data frame deterministically
    :return:
    """
    if exclude_columns:
        result_df = result_df.drop(*exclude_columns)
        expected_df = expected_df.drop(*exclude_columns)
    if order_by:
        expected_df = expected_df.orderBy(*order_by)
        result_df = result_df.orderBy(*order_by)
    if include_columns:
        result_df = result_df.select(*include_columns)
        expected_df = expected_df.select(*include_columns)

    result_df = result_df.select(sorted(result_df.columns))
    expected_df = expected_df.select(sorted(expected_df.columns))

    if auto_sort:
        result_df = SparkDataFrameSorter.deterministic_sort(df=result_df)
        expected_df = SparkDataFrameSorter.deterministic_sort(df=expected_df)

    compare_sh_path: Optional[Path] = None
    if expected_path and result_path and temp_folder:
        expected_file_name: str = os.path.basename(expected_path)
        # create a temp file to launch the diff tool
        # use .command: https://stackoverflow.com/questions/5125907/how-to-run-a-shell-script-in-os-x-by-double-clicking
        compare_sh_path = Path(temp_folder).joinpath(
            f"compare_{expected_file_name}.command"
        )
        with open(compare_sh_path, "w") as compare_sh:
            compare_sh.write(
                f"/usr/local/bin/charm diff "
                f"{func_path_modifier(result_path) if func_path_modifier else result_path} "
                f"{func_path_modifier(expected_path) if func_path_modifier else expected_path}"
            )
            os.fchmod(compare_sh.fileno(), 0o7777)

    if sorted(result_df.columns) != sorted(expected_df.columns):
        print_data_frame_info(expected_df=expected_df, result_df=result_df)
        additional_info = f"Columns not matched:[{diff_lists(expected_df.columns, result_df.columns)}]"
        # remove the compare file since the issue is schema
        if os.path.exists(str(compare_sh_path)):
            os.remove(str(compare_sh_path))
        raise SparkDataFrameComparerException(
            exception_type=ExceptionType.SchemaMismatch,
            result_df=result_df,
            expected_df=expected_df,
            expected_path=expected_path,
            result_path=result_path,
            compare_path=compare_sh_path,
            message="Columns not matched",
            additional_info=additional_info,
            func_path_modifier=func_path_modifier,
        )

    # compare the types
    result_columns: List[Tuple[str, str]] = list(
        map(lambda t: (t.name, t.dataType.typeName()), result_df.schema)
    )
    expected_columns: List[Tuple[str, str]] = list(
        map(lambda t: (t.name, t.dataType.typeName()), expected_df.schema)
    )
    number_of_mismatched_columns: int = 0
    mismatched_column_errors: List[str] = []
    for i in range(0, len(result_columns)):
        result_column = result_columns[i]
        expected_column = expected_columns[i]
        if result_column != expected_column:
            mismatched_column_errors.append(
                f"column type for {result_column[0]} did not match in {result_path}: "
                f"expected: {expected_column[1]}, actual: {result_column[1]}"
            )

            number_of_mismatched_columns += 1

    if number_of_mismatched_columns > 0:
        print_data_frame_info(expected_df=expected_df, result_df=result_df)
        message = f"{number_of_mismatched_columns} column types did not match"
        # remove the compare file since the issue is schema
        if os.path.exists(str(compare_sh_path)):
            os.remove(str(compare_sh_path))
        raise SparkDataFrameComparerException(
            exception_type=ExceptionType.SchemaMismatch,
            result_df=result_df,
            expected_df=expected_df,
            expected_path=expected_path,
            result_path=result_path,
            compare_path=compare_sh_path,
            message=message,
            additional_info=",".join(mismatched_column_errors),
            func_path_modifier=func_path_modifier,
        )

    if expected_df.count() != result_df.count():
        print_data_frame_info(expected_df, result_df)
        message = (
            f"Expected {expected_df.count()} rows, actual {result_df.count()} rows"
        )
        raise SparkDataFrameComparerException(
            exception_type=ExceptionType.RowMismatch,
            result_df=result_df,
            expected_df=expected_df,
            expected_path=expected_path,
            result_path=result_path,
            compare_path=compare_sh_path,
            message=message,
            additional_info="",
            func_path_modifier=func_path_modifier,
        )
    error_count: int = 0
    result_rows: List[Row] = result_df.collect()
    expected_rows: List[Row] = expected_df.collect()
    if len(result_rows) != len(expected_rows):
        print_data_frame_info(expected_df, result_df)
        message = f"Expected {len(expected_rows)} rows, actual {len(result_rows)} rows in collect()"
        raise SparkDataFrameComparerException(
            exception_type=ExceptionType.RowMismatch,
            result_df=result_df,
            expected_df=expected_df,
            expected_path=expected_path,
            result_path=result_path,
            compare_path=compare_sh_path,
            message=message,
            additional_info="",
            func_path_modifier=func_path_modifier,
        )
    # expected_column_schemas: Dict[str, StructField] = {
    #     t.name: t for t in expected_df.schema
    # }
    result_column_schemas: Dict[str, StructField] = {
        t.name: t for t in result_df.schema
    }
    my_errors: List[SparkDataFrameError] = []
    for row_num in range(0, len(result_rows)):
        if len(expected_rows[row_num]) != len(result_rows[row_num]):
            print_data_frame_info(expected_df, result_df)
            message = (
                f"Expected {len(expected_rows[row_num])} columns, actual {len(result_rows[row_num])} "
                f"columns for row_num: {row_num}"
            )
            raise SparkDataFrameComparerException(
                exception_type=ExceptionType.RowMismatch,
                result_df=result_df,
                expected_df=expected_df,
                expected_path=expected_path,
                result_path=result_path,
                compare_path=compare_sh_path,
                message=message,
                additional_info="",
                func_path_modifier=func_path_modifier,
            )

        error_count, my_errors = SparkDataFrameComparerHelper.check_data_frame(
            error_count=error_count,
            expected_rows=expected_rows,
            my_errors=my_errors,
            result_column_schemas=result_column_schemas,
            result_columns=result_columns,
            result_rows=result_rows,
            row_num=row_num,
        )

    if error_count != 0:
        print_data_frame_info(expected_df=expected_df, result_df=result_df)
        raise SparkDataFrameComparerException(
            exception_type=ExceptionType.DataMismatch,
            result_df=result_df,
            expected_df=expected_df,
            errors=my_errors,
            expected_path=expected_path,
            result_path=result_path,
            compare_path=compare_sh_path,
            message=f"{result_path} did not match expected {expected_path}.",
            func_path_modifier=func_path_modifier,
        )

    # if everything worked then remove the compare file
    if compare_sh_path and os.path.exists(str(compare_sh_path)):
        os.remove(str(compare_sh_path))


def print_data_frame_info(expected_df: DataFrame, result_df: DataFrame) -> None:
    print("comparing result to expected")
    print("schema for result")
    result_df.printSchema()
    print("schema for expected")
    expected_df.printSchema()
    print("--------- result ---------")
    result_df.show(truncate=False, n=100)
    print("--------- expected ----------")
    expected_df.show(truncate=False, n=100)
    # print("------- difference (result - expected) -------")
    # result_df.subtract(expected_df).show(truncate=False, n=100)
    # print("------- difference (expected - result) -------")
    # expected_df.subtract(result_df).show(truncate=False, n=100)
