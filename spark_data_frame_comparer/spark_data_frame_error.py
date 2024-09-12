from typing import Optional

from spark_data_frame_comparer.spark_data_frame_exception_type import ExceptionType


class SparkDataFrameError(Exception):
    def __init__(
        self,
        *,
        exception_type: ExceptionType,
        result: str,
        expected: str,
        message: str,
        additional_info: Optional[str] = None,
    ) -> None:
        self.exception_type: ExceptionType = exception_type
        self.result: str = result
        self.expected: str = expected
        self.message: str = message
        self.full_message: str = f"""{message}
        Expected:[{expected}],
        Actual:[{result}]
        """
        if additional_info:
            self.full_message += f"Info: {additional_info}\n"
        super().__init__(self.full_message)
