from typing import Any, Optional


# Define the custom class to hold the result
class JoinedResult:
    def __init__(
        self,
        index_1: Optional[int],
        index_2: Optional[int],
        key: str,
        value_1: Optional[Any],
        value_2: Optional[Any],
    ) -> None:
        self.index_1: Optional[int] = index_1
        self.index_2: Optional[int] = index_2
        self.key: str = key
        self.value_1: Optional[Any] = value_1
        self.value_2: Optional[Any] = value_2

    def __repr__(self) -> str:
        return f"JoinedResult(key={self.key}, index_1={self.index_1}, index_2={self.index_2}, value_1={self.value_1}, value_2={self.value_2})"
