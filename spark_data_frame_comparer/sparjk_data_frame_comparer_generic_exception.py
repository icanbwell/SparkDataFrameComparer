from typing import Any


class SparkDataFrameComparerGenericException(Exception):
    def __init__(self, ex: Any, msg: str) -> None:
        self.ex: Any = ex
        super().__init__(msg)
