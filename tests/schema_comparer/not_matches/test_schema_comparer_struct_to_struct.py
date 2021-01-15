from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

from spark_data_frame_comparer.schema_comparer import SchemaComparer


def test_schema_comparer_struct_to_struct() -> None:
    print("")
    source_schema: StructType = StructType(
        [
            StructField("name", StructType([StructField("given", FloatType())])),
            StructField("age", IntegerType()),
        ]
    )
    desired_schema: StructType = StructType(
        [
            StructField("name", StructType([StructField("given", StringType())])),
            StructField("age", IntegerType()),
        ]
    )
    SchemaComparer.compare_schema(
        parent_column_name=None,
        source_schema=source_schema,
        desired_schema=desired_schema,
    )
