from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    BooleanType,
    TimestampType,
    NullType,
)

from spark_data_frame_comparer.schema_comparer import (
    SchemaComparer,
    SchemaComparerResult,
    SchemaCompareErrorType,
)


def test_schema_comparer_complex_not_match() -> None:
    print("")
    source_schema: ArrayType = ArrayType(
        StructType(
            [
                StructField("id", NullType(), True),
                StructField("type", NullType(), True),
                StructField(
                    "actor",
                    StructType(
                        [
                            StructField("id", NullType(), True),
                            StructField("extension", NullType(), True),
                            StructField("reference", StringType(), False),
                            StructField("type", NullType(), True),
                            StructField("identifier", NullType(), True),
                            StructField("display", NullType(), True),
                        ]
                    ),
                    False,
                ),
                StructField("required", NullType(), True),
                StructField("status", StringType(), False),
                StructField("period", NullType(), True),
                StructField("extension", NullType(), True),
            ]
        ),
        False,
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

    assert len(result.errors) == 1, result.errors

    assert result.errors[0].error_type == SchemaCompareErrorType.ERROR
    assert result.errors[0].column == "identifier.extension"
