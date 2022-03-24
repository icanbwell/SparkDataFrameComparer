from typing import Optional, List


class SchemaComparisonItem:
    def __init__(self) -> None:
        self.source_column: Optional[str] = None
        self.source_column_data_type = None
        self.source_column_is_nullable = None
        self.destination_column: Optional[str] = None
        self.destination_column_data_type = None
        self.destination_column_is_nullable = None

        self.children: Optional[List[SchemaComparisonItem]] = None
