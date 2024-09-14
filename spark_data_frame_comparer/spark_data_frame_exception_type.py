from enum import Enum


class ExceptionType(Enum):
    SchemaMismatch = 1
    RowMismatch = 2
    DataMismatch = 3
