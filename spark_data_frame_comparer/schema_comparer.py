import traceback
from enum import Enum
from typing import Optional, List

# noinspection PyProtectedMember
import sys
from pyspark.sql.types import (
    StructType,
    StructField,
    ArrayType,
    DataType,
    NullType,
    StringType,
    BinaryType,
    IntegralType,
    BooleanType,
    DateType,
    TimestampType,
    NumericType,
)

from spark_data_frame_comparer.schema_comparison_item import SchemaComparisonItem
from spark_data_frame_comparer.sparjk_data_frame_comparer_generic_exception import (
    SparkDataFrameComparerGenericException,
)


class SchemaCompareErrorType(Enum):
    INFO = 1
    WARNING = 2
    ERROR = 3
    FATAL = 4


class SchemaCompareError:
    def __init__(
        self,
        column: Optional[str],
        error_type: SchemaCompareErrorType,
        error: str,
        source_schema: DataType,
        desired_schema: DataType,
    ) -> None:
        self.column: Optional[str] = column
        self.error_type: SchemaCompareErrorType = error_type
        self.error: str = error
        self.source_schema: DataType = source_schema
        self.desired_schema: DataType = desired_schema

    def __str__(self) -> str:
        return (
            (("column: [" + self.column + "]") if self.column else "")
            + "\n"
            + str(self.error_type)
            + ": "
            + self.error
            + "\n"
            + "source_schema: "
            + str(self.source_schema)
            + "\n"
            + "desired_schema: "
            + str(self.desired_schema)
            + "\n"
        )


class SchemaComparerResult:
    def __init__(self) -> None:
        self.errors: List[SchemaCompareError] = []

    def __str__(self) -> str:
        return "\n".join([str(e) for e in self.errors])

    def to_string(self, include_info: bool = False) -> str:
        if include_info is True:
            return "\n".join([str(e) for e in self.errors])
        else:
            return "\n".join(
                [
                    str(e)
                    for e in self.errors
                    if e.error_type != SchemaCompareErrorType.INFO
                ]
            )


class SchemaComparer:
    @staticmethod
    def compare_struct(
        parent_column_name: Optional[str],
        source_schema: DataType,
        desired_schema: StructType,
        schema_comparison: SchemaComparisonItem,
        allow_missing_nullable_properties: bool,
    ) -> List[SchemaCompareError]:
        if isinstance(source_schema, NullType):
            return []
        if not isinstance(source_schema, StructType):
            return [
                SchemaCompareError(
                    column=parent_column_name,
                    error_type=SchemaCompareErrorType.ERROR,
                    error=f"Type of {parent_column_name} does not match with struct.",
                    source_schema=source_schema,
                    desired_schema=desired_schema,
                )
            ]

        errors: List[SchemaCompareError] = []
        desired_field: StructField
        i: int = 0
        for desired_field in desired_schema.fields:
            # first see if the field exists in source_schema.
            # if destination field is nullable then it is fine if there is no source field
            if i >= len(source_schema.names):
                # nothing to match so skip
                if not desired_field.nullable or not allow_missing_nullable_properties:
                    errors.append(
                        SchemaCompareError(
                            column=f"{parent_column_name}.{desired_field.name}",
                            error_type=SchemaCompareErrorType.ERROR,
                            error=f"{parent_column_name}.{desired_field.name} not found in source"
                            + (
                                " and is not nullable"
                                if allow_missing_nullable_properties
                                else " and is within an array so even nullable properties must be present"
                            ),
                            source_schema=NullType(),
                            desired_schema=desired_field.dataType,
                        )
                    )
            elif desired_field.name != source_schema.names[i]:
                if desired_field.name in source_schema.names:
                    # it's in wrong place
                    errors.append(
                        SchemaCompareError(
                            column=f"{parent_column_name}.{desired_field.name}",
                            error_type=SchemaCompareErrorType.ERROR,
                            error=f"{parent_column_name}.{desired_field.name} is not in the same order as source."
                            f" source=[{','.join(source_schema.names)}]"
                            f" destination=[{','.join(desired_schema.names)}]",
                            source_schema=NullType(),
                            desired_schema=desired_field.dataType,
                        )
                    )
                # if field is nullable then it's ok
                elif desired_field.nullable and allow_missing_nullable_properties:
                    errors.append(
                        SchemaCompareError(
                            column=f"{parent_column_name}.{desired_field.name}",
                            error_type=SchemaCompareErrorType.INFO,
                            error=f"{parent_column_name}.{desired_field.name} not found in source"
                            + (
                                " and is not nullable"
                                if allow_missing_nullable_properties
                                else " and is within an array so even nullable properties must be present"
                            ),
                            source_schema=NullType(),
                            desired_schema=desired_field.dataType,
                        )
                    )
                else:
                    errors.append(
                        SchemaCompareError(
                            column=f"{parent_column_name}.{desired_field.name}",
                            error_type=SchemaCompareErrorType.ERROR,
                            error=f"{parent_column_name}.{desired_field.name} not found in source and is not nullable",
                            source_schema=NullType(),
                            desired_schema=desired_field.dataType,
                        )
                    )
            else:
                i += 1  # match found so increment source
                source_field: StructField = source_schema[desired_field.name]
                errors += SchemaComparer.compare_data_type(
                    parent_column_name=f"{parent_column_name}.{desired_field.name}",
                    source_schema=source_field.dataType,
                    desired_schema=desired_field.dataType,
                    schema_comparison=schema_comparison,
                    allow_missing_nullable_properties=allow_missing_nullable_properties,
                )

        # now check if source has extra columns
        for source_field in source_schema.fields:
            if source_field.name not in [
                desired_field.name for desired_field in desired_schema.fields
            ]:
                errors.append(
                    SchemaCompareError(
                        column=f"{parent_column_name}.{source_field.name}",
                        error_type=SchemaCompareErrorType.ERROR,
                        error=f"{parent_column_name}.{source_field.name} is in source schema"
                        " but not in destination schema",
                        source_schema=source_field.dataType,
                        desired_schema=NullType(),
                    )
                )

        return errors

    @staticmethod
    def compare_array(
        parent_column_name: Optional[str],
        source_schema: DataType,
        desired_schema: ArrayType,
        schema_comparison: SchemaComparisonItem,
        allow_missing_nullable_properties: bool,
    ) -> List[SchemaCompareError]:
        if isinstance(source_schema, NullType):
            return []

        if not isinstance(source_schema, ArrayType):
            return [
                SchemaCompareError(
                    column=parent_column_name,
                    error_type=SchemaCompareErrorType.ERROR,
                    error=f"Type of {parent_column_name} does not match with array.",
                    source_schema=source_schema,
                    desired_schema=desired_schema,
                )
            ]

        return SchemaComparer.compare_data_type(
            parent_column_name=parent_column_name,
            source_schema=source_schema.elementType,
            desired_schema=desired_schema.elementType,
            schema_comparison=schema_comparison,
            allow_missing_nullable_properties=False,  # properties inside an array have to match exactly
        )

    @staticmethod
    def compare_simple(
        parent_column_name: Optional[str],
        source_schema: DataType,
        desired_schema: DataType,
        schema_comparison: SchemaComparisonItem,
        allow_missing_nullable_properties: bool,
    ) -> List[SchemaCompareError]:
        if isinstance(source_schema, NullType):
            return []

        # compare the two
        if desired_schema != source_schema:
            if SchemaComparer.can_cast(
                source_type=source_schema, desired_type=desired_schema
            ):
                return [
                    SchemaCompareError(
                        column=parent_column_name,
                        error_type=SchemaCompareErrorType.INFO,
                        error=f"Type of {parent_column_name} does not match with simple type but can be casted.",
                        source_schema=source_schema,
                        desired_schema=desired_schema,
                    )
                ]
            else:
                return [
                    SchemaCompareError(
                        column=parent_column_name,
                        error_type=SchemaCompareErrorType.ERROR,
                        error=f"Type of {parent_column_name} does not match with simple type.",
                        source_schema=source_schema,
                        desired_schema=desired_schema,
                    )
                ]
        return []

    @staticmethod
    def compare_data_type(
        parent_column_name: Optional[str],
        source_schema: DataType,
        desired_schema: DataType,
        schema_comparison: SchemaComparisonItem,
        allow_missing_nullable_properties: bool,
    ) -> List[SchemaCompareError]:
        if isinstance(desired_schema, StructType):
            return SchemaComparer.compare_struct(
                parent_column_name=parent_column_name,
                source_schema=source_schema,
                desired_schema=desired_schema,
                schema_comparison=schema_comparison,
                allow_missing_nullable_properties=allow_missing_nullable_properties,
            )
        elif isinstance(desired_schema, ArrayType):
            return SchemaComparer.compare_array(
                parent_column_name=parent_column_name,
                source_schema=source_schema,
                desired_schema=desired_schema,
                schema_comparison=schema_comparison,
                allow_missing_nullable_properties=allow_missing_nullable_properties,
            )
        else:
            return SchemaComparer.compare_simple(
                parent_column_name=parent_column_name,
                source_schema=source_schema,
                desired_schema=desired_schema,
                schema_comparison=schema_comparison,
                allow_missing_nullable_properties=allow_missing_nullable_properties,
            )

    @staticmethod
    def can_cast(source_type: DataType, desired_type: DataType) -> bool:
        if isinstance(source_type, NullType):
            return True

        if isinstance(desired_type, StringType):
            return True

        if isinstance(source_type, StringType) and isinstance(desired_type, BinaryType):
            return True

        if isinstance(source_type, IntegralType) and isinstance(
            desired_type, BinaryType
        ):
            return True

        if isinstance(source_type, StringType) and isinstance(
            desired_type, BooleanType
        ):
            return True

        if isinstance(source_type, DateType) and isinstance(desired_type, BooleanType):
            return True

        if isinstance(source_type, TimestampType) and isinstance(
            desired_type, BooleanType
        ):
            return True

        if isinstance(source_type, NumericType) and isinstance(
            desired_type, BooleanType
        ):
            return True

        if isinstance(source_type, StringType) and isinstance(
            desired_type, TimestampType
        ):
            return True

        if isinstance(source_type, BooleanType) and isinstance(
            desired_type, TimestampType
        ):
            return True

        if isinstance(source_type, DateType) and isinstance(
            desired_type, TimestampType
        ):
            return True

        if isinstance(source_type, NumericType) and isinstance(
            desired_type, TimestampType
        ):
            return True

        if isinstance(source_type, StringType) and isinstance(desired_type, DateType):
            return True

        if isinstance(source_type, TimestampType) and isinstance(
            desired_type, DateType
        ):
            return True

        return False

    @staticmethod
    def compare_schema(
        parent_column_name: Optional[str],
        source_schema: DataType,
        desired_schema: DataType,
    ) -> SchemaComparerResult:
        try:
            result: SchemaComparerResult = SchemaComparerResult()
            errors: List[SchemaCompareError] = SchemaComparer.compare_data_type(
                parent_column_name=parent_column_name,
                source_schema=source_schema,
                desired_schema=desired_schema,
                schema_comparison=SchemaComparisonItem(),
                allow_missing_nullable_properties=True,
            )
            # sort the errors by type
            result.errors = (
                [e for e in errors if e.error_type == SchemaCompareErrorType.FATAL]
                + [e for e in errors if e.error_type == SchemaCompareErrorType.ERROR]
                + [e for e in errors if e.error_type == SchemaCompareErrorType.WARNING]
                + [e for e in errors if e.error_type == SchemaCompareErrorType.INFO]
            )

            return result
        except:
            e = sys.exc_info()[0]
            msg = traceback.format_exc()
            raise SparkDataFrameComparerGenericException(e, msg) from e
