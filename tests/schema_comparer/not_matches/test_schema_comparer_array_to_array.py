from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    ArrayType,
)

from spark_data_frame_comparer.schema_comparer import (
    SchemaComparer,
    SchemaComparerResult,
    SchemaCompareErrorType,
)


def test_schema_comparer_array_to_array_not_match() -> None:
    print("")
    source_schema: StructType = StructType(
        [
            StructField("name", ArrayType(StructField("given2", FloatType()))),
            StructField("age", IntegerType()),
        ]
    )
    desired_schema: StructType = StructType(
        [
            StructField("name", ArrayType(StructField("given", FloatType()))),
            StructField("age", StringType()),
        ]
    )
    result: SchemaComparerResult = SchemaComparer.compare_schema(
        parent_column_name="identifier",
        source_schema=source_schema,
        desired_schema=desired_schema,
    )

    for error in result.errors:
        print(error)

    assert len(result.errors) == 2, result.errors

    assert result.errors[0].error_type == SchemaCompareErrorType.ERROR
    assert result.errors[0].column == "identifier.name"

    assert result.errors[1].error_type == SchemaCompareErrorType.INFO
    assert result.errors[1].column == "identifier.age"
