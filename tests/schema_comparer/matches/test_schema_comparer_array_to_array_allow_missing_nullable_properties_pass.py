from pyspark.sql.types import ArrayType, FloatType, IntegerType, StructField, StructType

from spark_data_frame_comparer.schema_comparer import (
    SchemaComparer,
    SchemaComparerResult,
)


def test_schema_comparer_array_to_array_allow_missing_nullable_properties_pass() -> (
    None
):
    print("")
    source_schema: StructType = StructType(
        [
            StructField(
                "name", ArrayType(StructType([StructField("foo", FloatType())]))
            ),
            StructField("age", IntegerType()),
        ]
    )
    desired_schema: StructType = StructType(
        [
            StructField(
                "name", ArrayType(StructType([StructField("foo", FloatType())]))
            ),
            StructField("age", IntegerType()),
        ]
    )
    result: SchemaComparerResult = SchemaComparer.compare_schema(
        parent_column_name=None,
        source_schema=source_schema,
        desired_schema=desired_schema,
    )

    assert len(result.errors) == 0
