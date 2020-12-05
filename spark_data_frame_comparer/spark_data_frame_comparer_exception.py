from pathlib import Path
from typing import Optional, Union


class SparkDataFrameComparerException(Exception):
    def __init__(
        self, result: str, expected: str, message: str,
        additional_info: Optional[str], result_path: Optional[Union[Path,
                                                                    str]],
        expected_path: Optional[Union[Path,
                                      str]], compare_path: Optional[Union[Path,
                                                                          str]]
    ) -> None:
        self.result: str = result
        self.expected: str = expected
        self.message: str = message
        self.result_path: Optional[Union[Path, str]] = result_path
        self.expected_path: Optional[Union[Path, str]] = expected_path
        self.compare_path: Optional[Union[Path, str]] = compare_path
        my_full_message: str = f"""{message}
        {result_path} compared to {expected_path},
        Expected:[{expected}],
        Actual:[{result}]
        Compare: {compare_path or 'No Compare File'}
        Info: {additional_info}
        """
        super().__init__(my_full_message)
