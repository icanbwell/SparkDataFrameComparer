import os
from datetime import datetime
from math import isnan
from pathlib import Path
from typing import List, Any, Tuple, Optional, Union, Callable, Dict

from pyspark.sql import DataFrame
from pyspark.sql.types import StructField, ArrayType, StructType, DataType, Row

from spark_data_frame_comparer.list_utils import diff_lists
from spark_data_frame_comparer.spark_data_frame_comparer_exception import (
    SparkDataFrameComparerException,
    ExceptionType,
)
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
            result=",".join(result_df.columns),
            expected=",".join(expected_df.columns),
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
            result=",".join(result_df.columns),
            expected=",".join(expected_df.columns),
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
            result=str(result_df.count()),
            expected=str(expected_df.count()),
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
            result=str(len(result_rows)),
            expected=str(len(expected_rows)),
            expected_path=expected_path,
            result_path=result_path,
            compare_path=compare_sh_path,
            message=message,
            additional_info="",
            func_path_modifier=func_path_modifier,
        )
    column_schemas: List[StructField] = [t for t in result_df.schema]
    my_errors: List[str] = []
    for row_num in range(0, len(result_rows)):
        if len(expected_rows[row_num]) != len(result_rows[row_num]):
            print_data_frame_info(expected_df, result_df)
            message = (
                f"Expected {len(expected_rows[row_num])} columns, actual {len(result_rows[row_num])} "
                f"columns for row_num: {row_num}"
            )
            raise SparkDataFrameComparerException(
                exception_type=ExceptionType.RowMismatch,
                result=str(len(result_rows[row_num])),
                expected=str(len(expected_rows[row_num])),
                expected_path=expected_path,
                result_path=result_path,
                compare_path=compare_sh_path,
                message=message,
                additional_info="",
                func_path_modifier=func_path_modifier,
            )
        for column_num in range(0, len(result_columns)):
            schema_for_column: StructField = column_schemas[column_num]
            column_name: str = schema_for_column.name
            result_value = result_rows[row_num][column_num]
            expected_value = expected_rows[row_num][column_num]
            if (expected_value is None or expected_value == "") and (
                result_value is None or result_value == ""
            ):
                pass
            elif result_value is None or expected_value is None:
                error_count += 1
                my_errors.append(
                    f"row {row_num}: column {column_num} "
                    f"expected: [{expected_value}] actual: [{result_value}]"
                )
            else:
                column_error_count: int
                column_errors: List[str]
                column_error_count, column_errors = check_column_value(
                    column_name=column_name,
                    error_count=error_count,
                    expected_value=expected_value,
                    result_columns=result_columns,
                    result_value=result_value,
                    row_num=row_num,
                    data_type_for_column=schema_for_column.dataType,
                )
                error_count += column_error_count
                my_errors = my_errors + column_errors

    if error_count != 0:
        print_data_frame_info(expected_df=expected_df, result_df=result_df)
        raise SparkDataFrameComparerException(
            exception_type=ExceptionType.DataMismatch,
            result=str(result_df.count()),
            expected=str(expected_df.count()),
            expected_path=expected_path,
            result_path=result_path,
            compare_path=compare_sh_path,
            message=f"{result_path} did not match expected {expected_path}.",
            additional_info=",".join(my_errors),
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


def check_column_value(
    column_name: str,
    error_count: int,
    expected_value: Any,
    result_columns: List[Tuple[str, str]],
    result_value: Any,
    row_num: int,
    data_type_for_column: DataType,
) -> Tuple[int, List[str]]:
    assert isinstance(column_name, str)
    my_errors: List[str] = []
    if isinstance(data_type_for_column, ArrayType):
        if result_value is None and expected_value is None:
            return error_count, []
        if result_value is None or expected_value is None:
            return error_count + 1, [
                f"Expected array in row:{row_num}, col:{column_name} to be {expected_value} "
                f"but actual is {result_value}"
            ]

        for array_item_index in range(0, len(result_value)):
            element_type: DataType = data_type_for_column.elementType
            result_array_item = result_value[array_item_index]
            if len(expected_value) < array_item_index + 1:
                return error_count + 1, [
                    f"Expected row:{row_num}, col:{column_name} has only {len(expected_value)} "
                    f"items but Actual has > {array_item_index + 1}"
                ]
            expected_array_item = expected_value[array_item_index]
            if isinstance(result_array_item, Row):
                column_error_count: int
                column_errors: List[str]
                column_error_count, column_errors = check_column_value(
                    column_name=column_name,
                    error_count=error_count,
                    expected_value=expected_array_item,
                    result_value=result_array_item,
                    result_columns=result_columns,
                    row_num=row_num,
                    data_type_for_column=element_type,
                )
                error_count += column_error_count
                my_errors = my_errors + column_errors

    elif isinstance(data_type_for_column, StructType):
        column_error_count, column_errors = check_struct(
            column_name=column_name,
            error_count=error_count,
            expected_value=expected_value,
            result_value=result_value,
            row_num=row_num,
        )
        error_count += column_error_count
        my_errors = my_errors + column_errors
    else:
        column_error_count, column_errors = check_column_simple_value(
            error_count=error_count,
            expected_value=expected_value,
            result_value=result_value,
            row_num=row_num,
            column_name=column_name,
        )
        error_count += column_error_count
        my_errors = my_errors + column_errors
    return error_count, my_errors


def check_struct(
    column_name: str,
    error_count: int,
    expected_value: Row,
    result_value: Row,
    row_num: int,
) -> Tuple[int, List[str]]:
    assert isinstance(column_name, str)
    if expected_value is None and result_value is None:
        return error_count, []
    if result_value is None or expected_value is None:
        error_count += 1
        return error_count, [
            f"Expected struct in row:{row_num}, col:{column_name} to be {expected_value} "
            f"but actual is {result_value}"
        ]
    result_dict: Dict[str, Any] = result_value.asDict(recursive=True)
    expected_dict: Dict[str, Any] = expected_value.asDict(recursive=True)
    normalize_dictionaries(d1=result_dict, d2=expected_dict)
    if result_dict != expected_dict:
        return error_count + 1, [
            f"Expected struct in row:{row_num}, col:{column_name} to be {expected_dict} "
            f"but actual is {result_dict}"
        ]
    return error_count, []


def check_column_simple_value(
    column_name: str,
    error_count: int,
    expected_value: Any,
    result_value: Any,
    row_num: int,
) -> Tuple[int, List[str]]:
    assert isinstance(column_name, str)
    result_isnan = (
        isinstance(result_value, float)
        and isinstance(expected_value, float)
        and (isnan(result_value) == isnan(expected_value))
    )
    # if result does not match
    #   and result is not NaN
    #   and both result and expected are not false
    if (
        not compare_scalar(expected_value, result_value)
        and not result_isnan
        and not (not result_value and not expected_value)
    ):
        error_count += 1
        return error_count + 1, [
            f"row {row_num}: column {column_name} "
            + f"expected: [{expected_value}] actual: [{result_value}]"
        ]
    return error_count, []


def compare_scalar(
    expected_value: Union[int, float, bool, str],
    result_value: Union[int, float, bool, str],
) -> bool:
    # for datetime lose the microseconds when comparing
    if isinstance(expected_value, datetime) and isinstance(result_value, datetime):
        return expected_value.replace(microsecond=0) == result_value.replace(
            microsecond=0
        )
    return result_value == expected_value


def normalize_dictionaries(d1: Dict[str, Any], d2: Dict[str, Any]) -> None:
    """
    Recursively adds missing keys with value None in both dictionaries.
    If a key is present in one dictionary with None value and missing in the other, they are treated as equal.
    """
    all_keys = set(d1.keys()).union(
        set(d2.keys())
    )  # Get all keys from both dictionaries

    for key in all_keys:
        # If the key is missing in d1 but present in d2, add it to d1 with None
        if key not in d1:
            d1[key] = None
        # If the key is missing in d2 but present in d1, add it to d2 with None
        if key not in d2:
            d2[key] = None

        # Recursively normalize nested dictionaries
        if isinstance(d1[key], dict) and isinstance(d2[key], dict):
            normalize_dictionaries(d1[key], d2[key])
