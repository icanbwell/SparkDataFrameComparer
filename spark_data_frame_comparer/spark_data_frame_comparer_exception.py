from pathlib import Path
from typing import Optional, Union, Callable
from enum import Enum


class ExceptionType(Enum):
    SchemaMismatch = 1
    RowMismatch = 2
    DataMismatch = 3


class SparkDataFrameComparerException(Exception):
    def __init__(
        self,
        exception_type: ExceptionType,
        result: str,
        expected: str,
        message: str,
        additional_info: Optional[str],
        result_path: Optional[Union[Path, str]],
        expected_path: Optional[Union[Path, str]],
        compare_path: Optional[Union[Path, str]],
        func_path_modifier: Optional[
            Callable[[Union[Path, str]], Union[Path, str]]
        ] = None,
    ) -> None:
        self.exception_type: ExceptionType = exception_type
        self.result: str = result
        self.expected: str = expected
        self.message: str = message
        self.result_path: Optional[Union[Path, str]] = result_path
        self.expected_path: Optional[Union[Path, str]] = expected_path
        self.compare_path: Optional[Union[Path, str]] = compare_path
        if func_path_modifier and self.result_path:
            self.result_path = func_path_modifier(self.result_path)
        if func_path_modifier and self.expected_path:
            self.expected_path = func_path_modifier(self.expected_path)
        if func_path_modifier and self.compare_path:
            self.compare_path = func_path_modifier(self.compare_path)
        my_full_message: str = f"""{message}
        {result_path} compared to {expected_path},
        Expected:[{expected}],
        Actual:[{result}]
        Compare: {compare_path or 'No Compare File'}
        Info: {additional_info}
        """
        super().__init__(my_full_message)
