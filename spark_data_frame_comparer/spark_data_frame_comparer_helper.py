from datetime import datetime, time
from math import isnan
from typing import List, Any, Union, Dict, Optional

from pyspark.sql.types import ArrayType, StructType, DataType, Row, StructField

# Custom error classes to handle dataframe comparison errors
from spark_data_frame_comparer.spark_data_frame_error import SparkDataFrameError
from spark_data_frame_comparer.spark_data_frame_exception_type import ExceptionType
from spark_data_frame_comparer.utilities.dictionary_joiner.dictionary_joiner import (
    DictionaryJoiner,
)
from spark_data_frame_comparer.utilities.dictionary_joiner.joined_result import (
    JoinedResult,
)


class SparkDataFrameComparerHelper:
    @staticmethod
    def check_data_frame(
        *,
        expected_rows: List[Row],
        my_errors: List[SparkDataFrameError],
        result_column_schemas: Dict[str, StructField],
        expected_column_schemas: Dict[str, StructField],
        result_rows: List[Row],
        row_num: int,
    ) -> List[SparkDataFrameError]:
        """
        Compares rows of the result DataFrame with expected rows and identifies mismatches.
        Returns the updated error count and a list of identified errors.


        :param expected_rows: The list of expected rows.
        :param my_errors: The list of errors identified so far.
        :param result_column_schemas: The schema of the result DataFrame.
        :param expected_column_schemas: The schema of the expected DataFrame.
        :param result_rows: The list of rows from the result DataFrame.
        :param row_num: The current row number being compared.
        :return: The updated error count and a list of identified errors.
        """

        combined_schema: Dict[str, JoinedResult[StructField]] = (
            DictionaryJoiner.join_dicts(
                dict_1=result_column_schemas, dict_2=expected_column_schemas
            )
        )
        # Iterate over each column in the result dataframe schema
        column_name: str
        combined_schema_item: JoinedResult[StructField]
        for column_name, combined_schema_item in combined_schema.items():
            # see if the column exists in both

            if combined_schema_item.index_1 is None:
                # column does not exist in result
                # check if column value is not None in expected
                if expected_rows[row_num][combined_schema_item.index_2] is not None:
                    my_errors.append(
                        SparkDataFrameError(
                            exception_type=ExceptionType.DataMismatch,
                            result=None,
                            expected=expected_rows[row_num][
                                combined_schema_item.index_2
                            ],
                            message=f"row {row_num}: column {column_name} "
                            f"expected: [{expected_rows[row_num][combined_schema_item.index_2]}] actual: [None]",
                        )
                    )
            elif combined_schema_item.index_2 is None:
                # column does not exit in expected
                # check if column value is not None in result
                if result_rows[row_num][combined_schema_item.index_1] is not None:
                    my_errors.append(
                        SparkDataFrameError(
                            exception_type=ExceptionType.DataMismatch,
                            result=result_rows[row_num][combined_schema_item.index_1],
                            expected=None,
                            message=f"row {row_num}: column {column_name} "
                            f"expected: [None] actual: [{result_rows[row_num][combined_schema_item.index_1]}]",
                        )
                    )
            else:
                # column exists in both
                result_value = result_rows[row_num][combined_schema_item.index_1]
                expected_value = expected_rows[row_num][combined_schema_item.index_2]

                # Check if both expected and result values are None or empty, and skip if true
                if (expected_value is None or expected_value == "") and (
                    result_value is None or result_value == ""
                ):
                    pass
                # Handle case where one value is None but not the other
                elif result_value is None or expected_value is None:
                    # Append error for mismatch
                    my_errors.append(
                        SparkDataFrameError(
                            exception_type=ExceptionType.DataMismatch,
                            result=str(result_value),
                            expected=str(expected_value),
                            message=f"row {row_num}: column {column_name} "
                            f"expected: [{expected_value}] actual: [{result_value}]",
                        )
                    )
                else:
                    assert (
                        combined_schema_item.value_1 is not None
                    ), "should always be set at this point"
                    # Check the individual column value and accumulate any column-specific errors
                    column_errors = SparkDataFrameComparerHelper.check_column_value(
                        column_name=column_name,
                        expected_value=expected_value,
                        result_value=result_value,
                        row_num=row_num,
                        combined_schema_item=combined_schema_item,
                        # data_type_for_column=combined_schema_item.value_1.dataType,
                    )
                    my_errors.extend(column_errors)

        return my_errors

    @staticmethod
    def check_column_value(
        *,
        column_name: str,
        expected_value: Any,
        result_value: Any,
        row_num: int,
        combined_schema_item: JoinedResult[StructField],
    ) -> List[SparkDataFrameError]:
        """
        Compares the values in a column and returns any mismatch errors.
        Handles complex data types like arrays and structs.


        :param column_name: The name of the column being compared.
        :param expected_value: The expected value in the column.
        :param result_value: The actual value in the column.
        :param row_num: The current row number being compared.
        :param combined_schema_item: The combined schema item for the column.
        :return: The updated error count and a list of identified errors.
        """
        my_errors: List[SparkDataFrameError] = []

        if expected_value is None and result_value is None:
            return []

        result_struct_field_for_column: Optional[StructField] = (
            combined_schema_item.value_1
        )
        if result_struct_field_for_column is None:
            return [
                SparkDataFrameError(
                    exception_type=ExceptionType.DataMismatch,
                    result=str(result_value),
                    expected=str(expected_value),
                    message=f"Expected row:{row_num}, col:{column_name} to be {expected_value} "
                    f"but actual is {result_value}",
                )
            ]
        expected_struct_field_for_column: Optional[StructField] = (
            combined_schema_item.value_2
        )
        if expected_struct_field_for_column is None:
            return [
                SparkDataFrameError(
                    exception_type=ExceptionType.DataMismatch,
                    result=str(result_value),
                    expected=str(expected_value),
                    message=f"Expected row:{row_num}, col:{column_name} to be {expected_value} "
                    f"but actual is {result_value}",
                )
            ]
        # If the column is an array, handle comparison for array elements
        if isinstance(result_struct_field_for_column.dataType, ArrayType):
            if isinstance(expected_struct_field_for_column.dataType, ArrayType):
                column_errors = SparkDataFrameComparerHelper.check_array(
                    row_num=row_num,
                    column_name=column_name,
                    result_value=result_value,
                    expected_value=expected_value,
                    result_data_type_for_column=result_struct_field_for_column.dataType,
                    expected_data_type_for_column=expected_struct_field_for_column.dataType,
                )
            else:
                # If the expected column is not an array, log an error
                column_errors = [
                    SparkDataFrameError(
                        exception_type=ExceptionType.SchemaMismatch,
                        result=str(result_value),
                        expected=str(expected_value),
                        message=f"Column {column_name} in expected output should be of type ArrayType"
                        f" but is {expected_struct_field_for_column.dataType}",
                    )
                ]
            my_errors.extend(column_errors)
        elif isinstance(result_struct_field_for_column.dataType, StructType):
            # Handle StructType comparison (nested columns)
            if isinstance(expected_struct_field_for_column.dataType, StructType):
                column_errors = SparkDataFrameComparerHelper.check_struct(
                    column_name=column_name,
                    expected_value=expected_value,
                    result_value=result_value,
                    row_num=row_num,
                    result_data_type_for_column=result_struct_field_for_column.dataType,
                    expected_data_type_for_column=expected_struct_field_for_column.dataType,
                )
            else:
                # If the expected column is not a struct, log an error
                column_errors = [
                    SparkDataFrameError(
                        exception_type=ExceptionType.SchemaMismatch,
                        result=str(result_value),
                        expected=str(expected_value),
                        message=f"Column {column_name} in expected output should be of type StructType "
                        f"but is {expected_struct_field_for_column.dataType}",
                    )
                ]
            my_errors.extend(column_errors)

        # Handle simple data types like int, float, string, etc.
        else:
            column_errors = SparkDataFrameComparerHelper.check_column_simple_value(
                expected_value=expected_value,
                result_value=result_value,
                row_num=row_num,
                column_name=column_name,
            )
            my_errors.extend(column_errors)

        return my_errors

    @staticmethod
    def check_array(
        row_num: int,
        column_name: str,
        result_value: List[Any],
        expected_value: List[Any],
        result_data_type_for_column: ArrayType,
        expected_data_type_for_column: ArrayType,
    ) -> List[SparkDataFrameError]:
        if result_value is None and expected_value is None:
            return []

        if result_value is None or expected_value is None:
            return [
                SparkDataFrameError(
                    exception_type=ExceptionType.DataMismatch,
                    result=str(result_value),
                    expected=str(expected_value),
                    message=f"Expected array in row:{row_num}, col:{column_name} to be {expected_value} "
                    f"but actual is {result_value}",
                )
            ]

        result_element_type: DataType = result_data_type_for_column.elementType
        expected_element_type: DataType = expected_data_type_for_column.elementType

        my_errors: List[SparkDataFrameError] = []
        # Compare each element in the array
        for array_item_index in range(len(result_value)):
            result_array_item = result_value[array_item_index]

            # If the expected array is shorter than the result array, log an error
            if len(expected_value) < array_item_index + 1:
                return [
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
                # dataType should be a StructType
                assert isinstance(result_element_type, StructType)
                assert isinstance(expected_element_type, StructType)

                column_errors = SparkDataFrameComparerHelper.check_struct(
                    column_name=column_name,
                    expected_value=expected_array_item,
                    result_value=result_array_item,
                    row_num=row_num,
                    result_data_type_for_column=result_element_type,
                    expected_data_type_for_column=expected_element_type,
                )
                my_errors.extend(column_errors)
            else:
                # Compare simple data types like int, float, string, etc.
                column_errors = SparkDataFrameComparerHelper.check_column_simple_value(
                    column_name=f"{column_name}[{array_item_index}]",
                    expected_value=expected_array_item,
                    result_value=result_array_item,
                    row_num=row_num,
                )
                my_errors.extend(column_errors)
        return my_errors

    @staticmethod
    def check_struct(
        *,
        column_name: str,
        expected_value: Row,
        result_value: Row,
        row_num: int,
        result_data_type_for_column: StructType,
        expected_data_type_for_column: StructType,
    ) -> List[SparkDataFrameError]:
        """
        Compares values of a struct (nested columns) and accumulates errors.


        :param column_name: The name of the column being compared.
        :param expected_value: The expected value in the column.
        :param result_value: The actual value in the column.
        :param row_num: The current row number being compared.
        :param result_data_type_for_column: The data type of the column.
        :param expected_data_type_for_column: The data type of the expected column.
        :return: The updated error count and a list of identified errors.
        """
        if expected_value is None and result_value is None:
            return []

        if result_value is None or expected_value is None:
            return [
                SparkDataFrameError(
                    exception_type=ExceptionType.DataMismatch,
                    result=str(result_value),
                    expected=str(expected_value),
                    message=f"Expected struct in row:{row_num}, col:{column_name} to be {expected_value} "
                    f"but actual is {result_value}",
                )
            ]

        my_errors: List[SparkDataFrameError] = []

        result_schema_dict: Dict[str, StructField] = {
            t.name: t for t in result_data_type_for_column.fields
        }
        expected_schema_dict: Dict[str, StructField] = {
            t.name: t for t in expected_data_type_for_column.fields
        }

        combined_schema: Dict[str, JoinedResult[StructField]] = (
            DictionaryJoiner.join_dicts(
                dict_1=result_schema_dict, dict_2=expected_schema_dict
            )
        )
        # Iterate through each field of the struct and compare the fields
        field_name: str
        combined_schema_item: JoinedResult[StructField]
        for field_name, combined_schema_item in combined_schema.items():
            if combined_schema_item.index_1 is None:
                # column does not exist in result
                # check if column value is not None in expected
                if expected_value[combined_schema_item.index_2] is not None:
                    my_errors.append(
                        SparkDataFrameError(
                            exception_type=ExceptionType.DataMismatch,
                            result=None,
                            expected=expected_value[combined_schema_item.index_2],
                            message=f"row {row_num}: column {column_name}.{field_name} "
                            f"expected: [{expected_value[combined_schema_item.index_2]}] actual: [None]",
                        )
                    )
            elif combined_schema_item.index_2 is None:
                # column does not exit in expected
                # check if column value is not None in result
                if result_value[combined_schema_item.index_1] is not None:
                    my_errors.append(
                        SparkDataFrameError(
                            exception_type=ExceptionType.DataMismatch,
                            result=result_value[combined_schema_item.index_1],
                            expected=None,
                            message=f"row {row_num}: column {column_name}.{field_name} "
                            f"expected: [None] actual: [{result_value[combined_schema_item.index_1]}]",
                        )
                    )
            else:
                result_struct_item = result_value[combined_schema_item.index_1]
                expected_struct_item = expected_value[combined_schema_item.index_2]
                result_struct_item_type: Optional[StructField] = (
                    combined_schema_item.value_1
                )

                # Recursively compare nested structs
                column_errors = SparkDataFrameComparerHelper.check_column_value(
                    column_name=(
                        f"{column_name}.{result_struct_item_type.name}"
                        if result_struct_item_type
                        else column_name
                    ),
                    expected_value=expected_struct_item,
                    result_value=result_struct_item,
                    combined_schema_item=combined_schema_item,
                    row_num=row_num,
                )
                my_errors.extend(column_errors)

        return my_errors

    @staticmethod
    def check_column_simple_value(
        *,
        column_name: str,
        expected_value: Any,
        result_value: Any,
        row_num: int,
    ) -> List[SparkDataFrameError]:
        """
        Compares simple column values like strings, ints, or floats.
        Handles special cases like NaN values.


        :param column_name: The name of the column being compared.
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
            return [
                SparkDataFrameError(
                    exception_type=ExceptionType.DataMismatch,
                    result=str(result_value),
                    expected=str(expected_value),
                    message=f"Expected row:{row_num}, col:{column_name} to be {expected_value} "
                    f"but actual is {result_value}",
                )
            ]

        return []

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
