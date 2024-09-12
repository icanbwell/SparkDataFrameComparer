from datetime import datetime
from math import isnan
from typing import List, Any, Tuple, Union, Dict

from pyspark.sql.types import ArrayType, StructType, DataType, Row

from spark_data_frame_comparer.spark_data_frame_error import (
    SparkDataFrameError,
)
from spark_data_frame_comparer.spark_data_frame_exception_type import ExceptionType


class SparkDataFrameComparerHelper:
    @staticmethod
    def check_column_value(
        column_name: str,
        error_count: int,
        expected_value: Any,
        result_columns: List[Tuple[str, str]],
        result_value: Any,
        row_num: int,
        data_type_for_column: DataType,
    ) -> Tuple[int, List[SparkDataFrameError]]:
        assert isinstance(column_name, str)
        my_errors: List[SparkDataFrameError] = []
        if isinstance(data_type_for_column, ArrayType):
            if result_value is None and expected_value is None:
                return error_count, []
            if result_value is None or expected_value is None:
                return error_count + 1, [
                    SparkDataFrameError(
                        exception_type=ExceptionType.DataMismatch,
                        result=str(result_value),
                        expected=str(expected_value),
                        message=f"Expected array in row:{row_num}, col:{column_name} to be {expected_value} "
                        f"but actual is {result_value}",
                    )
                ]

            for array_item_index in range(0, len(result_value)):
                element_type: DataType = data_type_for_column.elementType
                result_array_item = result_value[array_item_index]
                if len(expected_value) < array_item_index + 1:
                    return error_count + 1, [
                        SparkDataFrameError(
                            exception_type=ExceptionType.DataMismatch,
                            result=str(result_value),
                            expected=str(expected_value),
                            message=f"Expected row:{row_num}, col:{column_name} has only {len(expected_value)} "
                            f"items but Actual has > {array_item_index + 1}",
                        )
                    ]
                expected_array_item = expected_value[array_item_index]
                if isinstance(result_array_item, Row):
                    column_error_count: int
                    column_errors: List[SparkDataFrameError]
                    column_error_count, column_errors = (
                        SparkDataFrameComparerHelper.check_column_value(
                            column_name=column_name,
                            error_count=error_count,
                            expected_value=expected_array_item,
                            result_value=result_array_item,
                            result_columns=result_columns,
                            row_num=row_num,
                            data_type_for_column=element_type,
                        )
                    )
                    error_count += column_error_count
                    my_errors = my_errors + column_errors

        elif isinstance(data_type_for_column, StructType):
            column_error_count, column_errors = (
                SparkDataFrameComparerHelper.check_struct(
                    column_name=column_name,
                    error_count=error_count,
                    expected_value=expected_value,
                    result_value=result_value,
                    row_num=row_num,
                )
            )
            error_count += column_error_count
            my_errors = my_errors + column_errors
        else:
            column_error_count, column_errors = (
                SparkDataFrameComparerHelper.check_column_simple_value(
                    error_count=error_count,
                    expected_value=expected_value,
                    result_value=result_value,
                    row_num=row_num,
                    column_name=column_name,
                )
            )
            error_count += column_error_count
            my_errors = my_errors + column_errors
        return error_count, my_errors

    @staticmethod
    def check_struct(
        column_name: str,
        error_count: int,
        expected_value: Row,
        result_value: Row,
        row_num: int,
    ) -> Tuple[int, List[SparkDataFrameError]]:
        assert isinstance(column_name, str)
        if expected_value is None and result_value is None:
            return error_count, []
        if result_value is None or expected_value is None:
            error_count += 1
            return error_count, [
                SparkDataFrameError(
                    exception_type=ExceptionType.DataMismatch,
                    result=str(result_value),
                    expected=str(expected_value),
                    message=f"Expected struct in row:{row_num}, col:{column_name} to be {expected_value} "
                    f"but actual is {result_value}",
                )
            ]
        result_dict: Dict[str, Any] = result_value.asDict(recursive=True)
        expected_dict: Dict[str, Any] = expected_value.asDict(recursive=True)
        SparkDataFrameComparerHelper.normalize_dictionaries(
            d1=result_dict, d2=expected_dict
        )
        if result_dict != expected_dict:
            return error_count + 1, [
                SparkDataFrameError(
                    exception_type=ExceptionType.DataMismatch,
                    result=str(result_dict),
                    expected=str(expected_dict),
                    message=f"Expected struct in row:{row_num}, col:{column_name} to be expected vs actual:"
                    f"\n{expected_dict}"
                    f"\n{result_dict}\n",
                )
            ]
        return error_count, []

    @staticmethod
    def check_column_simple_value(
        column_name: str,
        error_count: int,
        expected_value: Any,
        result_value: Any,
        row_num: int,
    ) -> Tuple[int, List[SparkDataFrameError]]:
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
            not SparkDataFrameComparerHelper.compare_scalar(
                expected_value, result_value
            )
            and not result_isnan
            and not (not result_value and not expected_value)
        ):
            error_count += 1
            return error_count + 1, [
                SparkDataFrameError(
                    exception_type=ExceptionType.DataMismatch,
                    result=str(result_value),
                    expected=str(expected_value),
                    message=f"Expected row:{row_num}, col:{column_name} to be {expected_value} "
                    f"but actual is {result_value}",
                )
            ]
        return error_count, []

    @staticmethod
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

    @staticmethod
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
                SparkDataFrameComparerHelper.normalize_dictionaries(d1[key], d2[key])
