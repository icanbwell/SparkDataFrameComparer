from typing import Optional

from pyspark.sql.types import StructType, StructField, ArrayType, DataType


class SchemaComparer:
    @staticmethod
    def compare_struct(
        parent_column_name: Optional[str],
        source_schema: DataType,
        desired_schema: StructType,
    ) -> None:
        if not isinstance(source_schema, StructType):
            print(
                f"ERROR: Type of {parent_column_name} does not match.  source={source_schema.dataType}, desired={desired_schema.dataType}"
            )
            return
        desired_field: StructField
        for desired_field in desired_schema.fields:
            # first see if the field exists in source_schema
            if desired_field.name not in source_schema.names:
                # if field is nullable then it's ok
                if desired_field.nullable:
                    print(
                        f"{parent_column_name}.{desired_field.name} not found in source but is nullable so that's fine"
                    )
                else:
                    print(
                        f"ERROR: {parent_column_name}.{desired_field.name} not found in source and is not nullable"
                    )
                continue
            source_field: StructField = source_schema[desired_field.name]
            SchemaComparer.compare_schema(
                parent_column_name=f"{parent_column_name}.{desired_field.name}",
                source_schema=source_field.dataType,
                desired_schema=desired_field.dataType,
            )

    @staticmethod
    def compare_array(
        parent_column_name: Optional[str],
        source_schema: DataType,
        desired_schema: ArrayType,
    ) -> None:
        if not isinstance(source_schema, ArrayType):
            print(
                f"ERROR: Type of {parent_column_name} does not match.  source={source_schema}, desired={desired_schema}"
            )
            return

        SchemaComparer.compare_schema(
            parent_column_name=parent_column_name,
            source_schema=source_schema.elementType,
            desired_schema=desired_schema.elementType,
        )

    @staticmethod
    def compare_simple(
        parent_column_name: Optional[str],
        source_schema: DataType,
        desired_schema: DataType,
    ) -> None:
        # compare the two
        if desired_schema != source_schema:
            print(
                f"ERROR: Type of {parent_column_name} does not match.  source={source_schema}, desired={desired_schema}"
            )

    @staticmethod
    def compare_schema(
        parent_column_name: Optional[str],
        source_schema: DataType,
        desired_schema: DataType,
    ) -> None:
        if isinstance(desired_schema, StructType):
            SchemaComparer.compare_struct(
                parent_column_name=parent_column_name,
                source_schema=source_schema,
                desired_schema=desired_schema,
            )
        elif isinstance(desired_schema, ArrayType):
            SchemaComparer.compare_array(
                parent_column_name=parent_column_name,
                source_schema=source_schema,
                desired_schema=desired_schema,
            )
        else:
            SchemaComparer.compare_simple(
                parent_column_name=parent_column_name,
                source_schema=source_schema,
                desired_schema=desired_schema,
            )
        return None
