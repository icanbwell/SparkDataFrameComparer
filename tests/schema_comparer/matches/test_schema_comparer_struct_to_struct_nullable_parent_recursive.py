from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

from spark_data_frame_comparer.schema_comparer import (
    SchemaComparer,
    SchemaComparerResult,
)


def test_schema_comparer_struct_to_struct_nullable_parent_recursive() -> None:
    print("")
    source_schema: StructType = StructType(
        [
            StructField("nameNullable", StructType([])),
        ]
    )
    desired_schema: StructType = StructType(
        [
            StructField(
                "nameNullable",
                StructType(
                    [
                        StructField(
                            "given",
                            StructType(
                                [StructField("foo", StringType(), nullable=False)]
                            ),
                            nullable=False,
                        )
                    ]
                ),
            ),
        ]
    )
    result: SchemaComparerResult = SchemaComparer.compare_schema(
        parent_column_name=None,
        source_schema=source_schema,
        desired_schema=desired_schema,
    )

    assert len(result.errors) == 0, ",".join([str(e) for e in result.errors])
