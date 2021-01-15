from pyspark.sql.types import ArrayType, FloatType, IntegerType, StructField, StructType

from spark_data_frame_comparer.schema_comparer import SchemaComparer


def test_schema_comparer_array_to_array() -> None:
    print("")
    source_schema: StructType = StructType(
        [StructField("name", ArrayType(FloatType())), StructField("age", IntegerType())]
    )
    desired_schema: StructType = StructType(
        [StructField("name", ArrayType(FloatType())), StructField("age", IntegerType())]
    )
    SchemaComparer.compare_schema(
        parent_column_name=None,
        source_schema=source_schema,
        desired_schema=desired_schema,
    )
