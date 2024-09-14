import pytest
from typing import Dict, List, Any
from pyspark.sql.types import (
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    StructType,
    Row,
)
from spark_data_frame_comparer.spark_data_frame_comparer_helper import (
    SparkDataFrameComparerHelper,
)
from spark_data_frame_comparer.spark_data_frame_error import SparkDataFrameError
from spark_data_frame_comparer.utilities.dictionary_joiner.joined_result import (
    JoinedResult,
)


@pytest.fixture
def setup_schema() -> Dict[str, StructField]:
    return {
        "name": StructField("name", StringType(), True),
        "age": StructField("age", IntegerType(), True),
        "scores": StructField("scores", ArrayType(IntegerType()), True),
    }


@pytest.fixture
def setup_expected_rows() -> List[Row]:
    return [
        Row(name="John", age=30, scores=[80, 90]),
        Row(name="Jane", age=25, scores=[85, 95]),
    ]


@pytest.fixture
def setup_result_rows() -> List[Row]:
    return [
        Row(name="John", age=30, scores=[80, 90]),
        Row(name="Jane", age=26, scores=[85, 95]),  # Age mismatch
    ]


def test_check_data_frame_no_errors(
    setup_schema: Dict[str, StructField],
    setup_expected_rows: List[Row],
    setup_result_rows: List[Row],
) -> None:
    my_errors: List[SparkDataFrameError] = []

    my_errors = SparkDataFrameComparerHelper.check_data_frame(
        expected_rows=setup_expected_rows,
        my_errors=my_errors,
        result_column_schemas=setup_schema,
        expected_column_schemas=setup_schema,
        result_rows=setup_expected_rows,
        row_num=0,
    )

    assert len(my_errors) == 0


def test_check_data_frame_with_errors(
    setup_schema: Dict[str, StructField],
    setup_expected_rows: List[Row],
    setup_result_rows: List[Row],
) -> None:
    my_errors: List[SparkDataFrameError] = []

    my_errors = SparkDataFrameComparerHelper.check_data_frame(
        expected_rows=setup_expected_rows,
        my_errors=my_errors,
        result_column_schemas=setup_schema,
        expected_column_schemas=setup_schema,
        result_rows=setup_result_rows,
        row_num=1,
    )

    assert len(my_errors) == 1
    assert my_errors[0].message == "Expected row:1, col:age to be 25 but actual is 26"


def test_check_column_value_with_scalar() -> None:
    expected_value: int = 25
    result_value: int = 26
    row_num: int = 1
    column_name: str = "age"

    errors = SparkDataFrameComparerHelper.check_column_simple_value(
        column_name=column_name,
        expected_value=expected_value,
        result_value=result_value,
        row_num=row_num,
    )

    assert len(errors) == 1
    assert errors[0].message == "Expected row:1, col:age to be 25 but actual is 26"


def test_check_array_value() -> None:
    expected_value: List[int] = [80, 90]
    result_value: List[int] = [80, 85]  # Mismatch in the second element
    row_num: int = 1
    column_name: str = "scores"
    data_type: ArrayType = ArrayType(IntegerType())
    struct_field: StructField = StructField("scores", data_type, True)

    errors = SparkDataFrameComparerHelper.check_column_value(
        column_name=column_name,
        expected_value=expected_value,
        result_value=result_value,
        row_num=row_num,
        combined_schema_item=JoinedResult(
            0, 0, column_name, struct_field, struct_field
        ),
    )

    assert len(errors) == 1
    assert errors[0].message == (
        "Expected row:1, col:scores[1] to be 90 but actual is 85"
    )


def test_check_struct_value() -> None:
    expected_value: Row = Row(name="John", age=30)
    result_value: Row = Row(name="John", age=31)  # Mismatch in age
    row_num: int = 0
    column_name: str = "person"
    data_type: StructType = StructType(
        [StructField("name", StringType()), StructField("age", IntegerType())]
    )

    errors = SparkDataFrameComparerHelper.check_struct(
        column_name=column_name,
        expected_value=expected_value,
        result_value=result_value,
        row_num=row_num,
        result_data_type_for_column=data_type,
        expected_data_type_for_column=data_type,
    )

    assert len(errors) == 1
    assert (
        errors[0].message == "Expected row:0, col:person.age to be 30 but actual is 31"
    )


def test_normalize_dictionaries() -> None:
    d1: Dict[str, Any] = {"name": "John", "age": 30}
    d2: Dict[str, Any] = {"name": "John", "address": "123 Main St"}

    SparkDataFrameComparerHelper.normalize_dictionaries(d1, d2)

    assert d1["address"] is None
    assert d2["age"] is None
