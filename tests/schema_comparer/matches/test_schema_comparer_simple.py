from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from spark_data_frame_comparer.schema_comparer import SchemaComparer


def test_schema_comparer() -> None:
    source_schema: StructType = StructType(
        [StructField("name", StringType()), StructField("age", IntegerType())]
    )
    SchemaComparer.compare_schema(
        parent_column_name=None,
        source_schema=source_schema,
        desired_schema=source_schema,
    )
