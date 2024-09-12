from datetime import datetime, time
from math import isnan
from typing import List, Any, Tuple, Union, Dict

from pyspark.sql.types import ArrayType, StructType, DataType, Row, StructField

# Custom error classes to handle dataframe comparison errors
from spark_data_frame_comparer.spark_data_frame_error import SparkDataFrameError
from spark_data_frame_comparer.spark_data_frame_exception_type import ExceptionType


class SparkDataFrameComparerHelper:
    @staticmethod
    def check_data_frame(
        *,
        error_count: int,
        expected_rows: List[Row],
        my_errors: List[SparkDataFrameError],
        result_column_schemas: Dict[str, StructField],
        result_columns: List[Tuple[str, str]],
        result_rows: List[Row],
        row_num: int,
    ) -> Tuple[int, List[SparkDataFrameError]]:
        """
        Compares rows of the result DataFrame with expected rows and identifies mismatches.
        Returns the updated error count and a list of identified errors.


        :param error_count: The current error count.
        :param expected_rows: The list of expected rows.
        :param my_errors: The list of errors identified so far.
        :param result_column_schemas: The schema of the result DataFrame.
        :param result_columns: The list of column names and types.
        :param result_rows: The list of rows from the result DataFrame.
        :param row_num: The current row number being compared.
        :return: The updated error count and a list of identified errors.
        """

        # Iterate over each column in the result dataframe schema
        for result_column_num, (
            result_column_name,
            result_schema_for_column,
        ) in enumerate(result_column_schemas.items()):
            column_name: str = result_schema_for_column.name
            result_value = result_rows[row_num][result_column_num]
            expected_value = expected_rows[row_num][result_column_num]

            # Check if both expected and result values are None or empty, and skip if true
            if (expected_value is None or expected_value == "") and (
                result_value is None or result_value == ""
            ):
                pass
            # Handle case where one value is None but not the other
            elif result_value is None or expected_value is None:
                error_count += 1
                # Append error for mismatch
                my_errors.append(
                    SparkDataFrameError(
                        exception_type=ExceptionType.DataMismatch,
                        result=str(result_value),
                        expected=str(expected_value),
                        message=f"row {row_num}: column {result_column_name} "
                        f"expected: [{expected_value}] actual: [{result_value}]",
                    )
                )
            else:
                # Check the individual column value and accumulate any column-specific errors
                column_error_count, column_errors = (
                    SparkDataFrameComparerHelper.check_column_value(
                        column_name=column_name,
                        error_count=error_count,
                        expected_value=expected_value,
                        result_columns=result_columns,
                        result_value=result_value,
                        row_num=row_num,
                        data_type_for_column=result_schema_for_column.dataType,
                    )
                )
                error_count += column_error_count
                my_errors.extend(column_errors)

        return error_count, my_errors

    @staticmethod
    def check_column_value(
        *,
        column_name: str,
        error_count: int,
        expected_value: Any,
        result_columns: List[Tuple[str, str]],
        result_value: Any,
        row_num: int,
        data_type_for_column: DataType,
    ) -> Tuple[int, List[SparkDataFrameError]]:
        """
        Compares the values in a column and returns any mismatch errors.
        Handles complex data types like arrays and structs.


        :param column_name: The name of the column being compared.
        :param error_count: The current error count.
        :param expected_value: The expected value in the column.
        :param result_columns: The list of column names and types.
        :param result_value: The actual value in the column.
        :param row_num: The current row number being compared.
        :param data_type_for_column: The data type of the column.
        :return: The updated error count and a list of identified errors.
        """
        my_errors: List[SparkDataFrameError] = []

        # If the column is an array, handle comparison for array elements
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

            # Compare each element in the array
            for array_item_index in range(len(result_value)):
                element_type: DataType = data_type_for_column.elementType
                result_array_item = result_value[array_item_index]

                # If the expected array is shorter than the result array, log an error
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

                # Handle nested structures like Rows within arrays
                if isinstance(result_array_item, Row):
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
                    my_errors.extend(column_errors)

        # Handle StructType comparison (nested columns)
        elif isinstance(data_type_for_column, StructType):
            column_error_count, column_errors = (
                SparkDataFrameComparerHelper.check_struct(
                    column_name=column_name,
                    error_count=error_count,
                    expected_value=expected_value,
                    result_value=result_value,
                    row_num=row_num,
                    data_type_for_column=data_type_for_column,
                    result_columns=result_columns,
                )
            )
            error_count += column_error_count
            my_errors.extend(column_errors)

        # Handle simple data types like int, float, string, etc.
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
            my_errors.extend(column_errors)

        return error_count, my_errors

    @staticmethod
    def check_struct(
        *,
        column_name: str,
        error_count: int,
        expected_value: Row,
        result_value: Row,
        result_columns: List[Tuple[str, str]],
        row_num: int,
        data_type_for_column: StructType,
    ) -> Tuple[int, List[SparkDataFrameError]]:
        """
        Compares values of a struct (nested columns) and accumulates errors.


        :param column_name: The name of the column being compared.
        :param error_count: The current error count.
        :param expected_value: The expected value in the column.
        :param result_value: The actual value in the column.
        :param result_columns: The list of column names and types.
        :param row_num: The current row number being compared.
        :param data_type_for_column: The data type of the column.
        :return: The updated error count and a list of identified errors.
        """
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

        my_errors: List[SparkDataFrameError] = []

        # Iterate through each field of the struct and compare the fields
        for struct_item_index in range(len(result_value)):
            result_struct_item = result_value[struct_item_index]
            expected_struct_item = expected_value[struct_item_index]
            struct_item_type: StructField = data_type_for_column.fields[
                struct_item_index
            ]
            struct_item_data_type: DataType = struct_item_type.dataType

            # Recursively compare nested structs
            column_error_count, column_errors = (
                SparkDataFrameComparerHelper.check_column_value(
                    column_name=f"{column_name}.{struct_item_type.name}",
                    error_count=error_count,
                    expected_value=expected_struct_item,
                    result_value=result_struct_item,
                    result_columns=result_columns,
                    row_num=row_num,
                    data_type_for_column=struct_item_data_type,
                )
            )
            error_count += column_error_count
            my_errors.extend(column_errors)

        return error_count, my_errors

    @staticmethod
    def check_column_simple_value(
        *,
        column_name: str,
        error_count: int,
        expected_value: Any,
        result_value: Any,
        row_num: int,
    ) -> Tuple[int, List[SparkDataFrameError]]:
        """
        Compares simple column values like strings, ints, or floats.
        Handles special cases like NaN values.


        :param column_name: The name of the column being compared.
        :param error_count: The current error count.
        :param expected_value: The expected value in the column.
        :param result_value: The actual value in the column.
        :param row_num: The current row number being compared.
        :return: The updated error count and a list of identified errors.
        """
        result_isnan = (
            isinstance(result_value, float)
            and isinstance(expected_value, float)
            and (isnan(result_value) == isnan(expected_value))
        )

        # Check if the scalar values are equal; consider NaN as equal
        if (
            not SparkDataFrameComparerHelper.compare_scalar(
                expected_value=expected_value, result_value=result_value
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
        *,
        expected_value: Union[int, float, bool, str, datetime, time],
        result_value: Union[int, float, bool, str, datetime, time],
    ) -> bool:
        """
        Compares scalar values, special handling for datetime to ignore microseconds.


        :param expected_value: The expected scalar value.
        :param result_value: The actual scalar value.
        :return: True if the values are equal, False otherwise.
        """
        if isinstance(expected_value, datetime) and isinstance(result_value, datetime):
            return expected_value.replace(microsecond=0) == result_value.replace(
                microsecond=0
            )
        return result_value == expected_value

    @staticmethod
    def normalize_dictionaries(d1: Dict[str, Any], d2: Dict[str, Any]) -> None:
        """
        Recursively adds missing keys with value None in both dictionaries.
        If a key is present in one dictionary but not the other, it adds the missing key with value None.


        :param d1: The first dictionary to normalize.
        :param d2: The second dictionary to normalize.
        :return: None
        """
        all_keys = set(d1.keys()).union(
            set(d2.keys())
        )  # Get all keys from both dictionaries

        for key in all_keys:
            # Add missing key with None in d1 if it exists in d2 but not in d1
            if key not in d1:
                d1[key] = None
            # Add missing key with None in d2 if it exists in d1 but not in d2
            if key not in d2:
                d2[key] = None

            # Recursively normalize nested dictionaries
            if isinstance(d1[key], dict) and isinstance(d2[key], dict):
                SparkDataFrameComparerHelper.normalize_dictionaries(d1[key], d2[key])
