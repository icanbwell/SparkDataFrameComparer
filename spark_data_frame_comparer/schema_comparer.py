from typing import Optional, List

from pyspark.sql.types import StructType, StructField, ArrayType, DataType


class SchemaCompareError:
    def __init__(
        self,
        column: Optional[str],
        error: str,
        source_schema: DataType,
        desired_schema: DataType,
    ) -> None:
        self.column: Optional[str] = column
        self.error: str = error
        self.source_schema: DataType = source_schema
        self.desired_schema: DataType = desired_schema

    def __str__(self) -> str:
        return (
            (("column: [" + self.column + "]") if self.column else "")
            + "\n"
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


class SchemaComparer:
    @staticmethod
    def compare_struct(
        parent_column_name: Optional[str],
        source_schema: DataType,
        desired_schema: StructType,
    ) -> List[SchemaCompareError]:
        if not isinstance(source_schema, StructType):
            return [
                SchemaCompareError(
                    column=parent_column_name,
                    error=f"ERROR: Type of {parent_column_name} does not match.",
                    source_schema=source_schema,
                    desired_schema=desired_schema,
                )
            ]

        errors: List[SchemaCompareError] = []
        desired_field: StructField
        for desired_field in desired_schema.fields:
            # first see if the field exists in source_schema
            if desired_field.name not in source_schema.names:
                # if field is nullable then it's ok
                if desired_field.nullable:
                    errors.append(
                        SchemaCompareError(
                            column=f"{parent_column_name}.{desired_field.name}",
                            error=f"INFO: {parent_column_name}.{desired_field.name} not found in source but is nullable so that's fine",
                            source_schema=source_schema,
                            desired_schema=desired_schema,
                        )
                    )
                else:
                    errors.append(
                        SchemaCompareError(
                            column=f"{parent_column_name}.{desired_field.name}",
                            error=f"ERROR: {parent_column_name}.{desired_field.name} not found in source and is not nullable",
                            source_schema=source_schema,
                            desired_schema=desired_schema,
                        )
                    )
                continue
            source_field: StructField = source_schema[desired_field.name]
            errors += SchemaComparer.compare_data_type(
                parent_column_name=f"{parent_column_name}.{desired_field.name}",
                source_schema=source_field.dataType,
                desired_schema=desired_field.dataType,
            )
        return errors

    @staticmethod
    def compare_array(
        parent_column_name: Optional[str],
        source_schema: DataType,
        desired_schema: ArrayType,
    ) -> List[SchemaCompareError]:
        if not isinstance(source_schema, ArrayType):
            return [
                SchemaCompareError(
                    column=parent_column_name,
                    error=f"ERROR: Type of {parent_column_name} does not match.",
                    source_schema=source_schema,
                    desired_schema=desired_schema,
                )
            ]

        return SchemaComparer.compare_data_type(
            parent_column_name=parent_column_name,
            source_schema=source_schema.elementType,
            desired_schema=desired_schema.elementType,
        )

    @staticmethod
    def compare_simple(
        parent_column_name: Optional[str],
        source_schema: DataType,
        desired_schema: DataType,
    ) -> List[SchemaCompareError]:
        # compare the two
        if desired_schema != source_schema:
            return [
                SchemaCompareError(
                    column=parent_column_name,
                    error=f"ERROR: Type of {parent_column_name} does not match.",
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
    ) -> List[SchemaCompareError]:
        if isinstance(desired_schema, StructType):
            return SchemaComparer.compare_struct(
                parent_column_name=parent_column_name,
                source_schema=source_schema,
                desired_schema=desired_schema,
            )
        elif isinstance(desired_schema, ArrayType):
            return SchemaComparer.compare_array(
                parent_column_name=parent_column_name,
                source_schema=source_schema,
                desired_schema=desired_schema,
            )
        else:
            return SchemaComparer.compare_simple(
                parent_column_name=parent_column_name,
                source_schema=source_schema,
                desired_schema=desired_schema,
            )

    @staticmethod
    def compare_schema(
        parent_column_name: Optional[str],
        source_schema: DataType,
        desired_schema: DataType,
    ) -> SchemaComparerResult:
        result: SchemaComparerResult = SchemaComparerResult()
        result.errors = SchemaComparer.compare_data_type(
            parent_column_name=parent_column_name,
            source_schema=source_schema,
            desired_schema=desired_schema,
        )
        return result
