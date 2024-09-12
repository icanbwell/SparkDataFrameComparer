from pathlib import Path
from typing import Optional, Union, Callable, List

from pyspark.sql import DataFrame

from spark_data_frame_comparer.spark_data_frame_exception_type import ExceptionType
from spark_data_frame_comparer.spark_data_frame_error import (
    SparkDataFrameError,
)
from spark_data_frame_comparer.utilities.pretty_print.pretty_print import (
    get_pretty_data_frame,
)


class SparkDataFrameComparerException(Exception):
    def __init__(
        self,
        *,
        exception_type: ExceptionType,
        result_df: DataFrame,
        expected_df: DataFrame,
        message: str,
        additional_info: Optional[str] = None,
        result_path: Optional[Union[Path, str]],
        expected_path: Optional[Union[Path, str]],
        compare_path: Optional[Union[Path, str]],
        errors: Optional[List[SparkDataFrameError]] = None,
        func_path_modifier: Optional[
            Callable[[Union[Path, str]], Union[Path, str]]
        ] = None,
    ) -> None:
        self.exception_type: ExceptionType = exception_type
        self.result_df: DataFrame = result_df
        self.expected_df: DataFrame = expected_df
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

        self.full_message: str = f"""{message}
        {result_path} compared to {expected_path},
        Compare: {compare_path or 'No Compare File'}
        """
        if additional_info:
            self.full_message += f"Info: {additional_info}\n"
        if errors:
            error: SparkDataFrameError
            for index, error in enumerate(errors):
                self.full_message += f"\n------ DataFrame Compare Failure {index + 1} ------\n{error.message}"

        # add dataframes
        self.full_message += "\n------ Expected DataFrame ------\n"
        self.full_message += f"{get_pretty_data_frame(expected_df, limit=100)}\n"
        self.full_message += "------ Result DataFrame ------\n"
        self.full_message += f"{get_pretty_data_frame(result_df, limit=100)}\n"
        self.full_message += "------ End DataFrames ------\n"
        super().__init__(self.full_message)
