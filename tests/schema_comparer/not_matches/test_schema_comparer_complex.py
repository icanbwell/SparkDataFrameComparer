from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    ArrayType,
    BooleanType,
    TimestampType,
)

from spark_data_frame_comparer.schema_comparer import (
    SchemaComparer,
    SchemaComparerResult,
    SchemaCompareErrorType,
)


def test_schema_comparer_complex_not_match() -> None:
    print("")
    source_schema: StructType = StructType(
        [
            StructField("name", ArrayType(StructField("given2", FloatType()))),
            StructField("age", IntegerType()),
        ]
    )
    desired_schema: ArrayType = ArrayType(
        StructType(
            [
                StructField("id", StringType(), True),
                StructField("extension", StringType(), True),
                StructField(
                    "type",
                    ArrayType(
                        StructType(
                            [
                                StructField("id", StringType(), True),
                                StructField("extension", StringType(), True),
                                StructField(
                                    "coding",
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField("id", StringType(), True),
                                                StructField(
                                                    "extension", StringType(), True
                                                ),
                                                StructField(
                                                    "system", StringType(), True
                                                ),
                                                StructField(
                                                    "version", StringType(), True
                                                ),
                                                StructField("code", StringType(), True),
                                                StructField(
                                                    "display", StringType(), True
                                                ),
                                                StructField(
                                                    "userSelected", BooleanType(), True
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    True,
                                ),
                                StructField("text", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    True,
                ),
                StructField(
                    "actor",
                    StructType(
                        [
                            StructField("id", StringType(), True),
                            StructField("extension", StringType(), True),
                            StructField("reference", StringType(), True),
                            StructField("type", StringType(), True),
                            StructField(
                                "identifier",
                                StructType(
                                    [
                                        StructField("id", StringType(), True),
                                        StructField("extension", StringType(), True),
                                        StructField("use", StringType(), True),
                                        StructField(
                                            "type",
                                            StructType(
                                                [
                                                    StructField(
                                                        "id", StringType(), True
                                                    ),
                                                    StructField(
                                                        "extension", StringType(), True
                                                    ),
                                                    StructField(
                                                        "coding",
                                                        ArrayType(
                                                            StructType(
                                                                [
                                                                    StructField(
                                                                        "id",
                                                                        StringType(),
                                                                        True,
                                                                    ),
                                                                    StructField(
                                                                        "extension",
                                                                        StringType(),
                                                                        True,
                                                                    ),
                                                                    StructField(
                                                                        "system",
                                                                        StringType(),
                                                                        True,
                                                                    ),
                                                                    StructField(
                                                                        "version",
                                                                        StringType(),
                                                                        True,
                                                                    ),
                                                                    StructField(
                                                                        "code",
                                                                        StringType(),
                                                                        True,
                                                                    ),
                                                                    StructField(
                                                                        "display",
                                                                        StringType(),
                                                                        True,
                                                                    ),
                                                                    StructField(
                                                                        "userSelected",
                                                                        BooleanType(),
                                                                        True,
                                                                    ),
                                                                ]
                                                            ),
                                                            True,
                                                        ),
                                                        True,
                                                    ),
                                                    StructField(
                                                        "text", StringType(), True
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        StructField("system", StringType(), True),
                                        StructField("value", StringType(), True),
                                        StructField(
                                            "period",
                                            StructType(
                                                [
                                                    StructField(
                                                        "id", StringType(), True
                                                    ),
                                                    StructField(
                                                        "extension", StringType(), True
                                                    ),
                                                    StructField(
                                                        "start", TimestampType(), True
                                                    ),
                                                    StructField(
                                                        "end", TimestampType(), True
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        StructField(
                                            "assigner",
                                            StructType(
                                                [
                                                    StructField(
                                                        "id", StringType(), True
                                                    ),
                                                    StructField(
                                                        "extension", StringType(), True
                                                    ),
                                                    StructField(
                                                        "reference", StringType(), True
                                                    ),
                                                    StructField(
                                                        "type", StringType(), True
                                                    ),
                                                    StructField(
                                                        "identifier",
                                                        StructType(
                                                            [
                                                                StructField(
                                                                    "id",
                                                                    StringType(),
                                                                    True,
                                                                ),
                                                                StructField(
                                                                    "extension",
                                                                    StringType(),
                                                                    True,
                                                                ),
                                                                StructField(
                                                                    "use",
                                                                    StringType(),
                                                                    True,
                                                                ),
                                                                StructField(
                                                                    "type",
                                                                    StructType(
                                                                        [
                                                                            StructField(
                                                                                "id",
                                                                                StringType(),
                                                                                True,
                                                                            )
                                                                        ]
                                                                    ),
                                                                    True,
                                                                ),
                                                                StructField(
                                                                    "system",
                                                                    StringType(),
                                                                    True,
                                                                ),
                                                                StructField(
                                                                    "value",
                                                                    StringType(),
                                                                    True,
                                                                ),
                                                                StructField(
                                                                    "period",
                                                                    StructType(
                                                                        [
                                                                            StructField(
                                                                                "id",
                                                                                StringType(),
                                                                                True,
                                                                            )
                                                                        ]
                                                                    ),
                                                                    True,
                                                                ),
                                                                StructField(
                                                                    "assigner",
                                                                    StructType(
                                                                        [
                                                                            StructField(
                                                                                "id",
                                                                                StringType(),
                                                                                True,
                                                                            )
                                                                        ]
                                                                    ),
                                                                    True,
                                                                ),
                                                            ]
                                                        ),
                                                        True,
                                                    ),
                                                    StructField(
                                                        "display", StringType(), True
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                            StructField("display", StringType(), True),
                        ]
                    ),
                    True,
                ),
                StructField("required", StringType(), True),
                StructField("status", StringType(), True),
                StructField(
                    "period",
                    StructType(
                        [
                            StructField("id", StringType(), True),
                            StructField("extension", StringType(), True),
                            StructField("start", TimestampType(), True),
                            StructField("end", TimestampType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        ),
        True,
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
