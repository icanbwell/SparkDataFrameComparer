from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

from spark_data_frame_comparer.schema_comparer import SchemaComparer


def test_schema_comparer_not_matches() -> None:
    source_schema: StructType = StructType(
        [StructField("name", StringType()), StructField("age", IntegerType())]
    )
    desired_schema: StructType = StructType(
        [StructField("name", FloatType()), StructField("age", IntegerType())]
    )
    SchemaComparer.compare_schema(
        parent_column_name=None,
        source_schema=source_schema,
        desired_schema=desired_schema,
    )
