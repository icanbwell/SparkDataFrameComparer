from typing import Dict, Optional, Set, TypeVar, Generic
from spark_data_frame_comparer.utilities.dictionary_joiner.joined_result import (
    JoinedResult,
)

# Define a type variable to represent the type of dictionary values
T = TypeVar("T")


class DictionaryJoiner(Generic[T]):
    @staticmethod
    def join_dicts(
        dict_1: Dict[str, T], dict_2: Dict[str, T]
    ) -> Dict[str, JoinedResult[T]]:
        """
        Join two dictionaries into a single dictionary with the same keys.

        :param dict_1: The first dictionary to join.
        :param dict_2: The second dictionary to join.
        :return: A dictionary with the same keys as the input dictionaries, with values from both dictionaries
        """
        combined_keys: Set[str] = set(dict_1.keys()).union(set(dict_2.keys()))

        results: Dict[str, JoinedResult[T]] = {}

        for key in combined_keys:
            index_1 = list(dict_1.keys()).index(key) if key in dict_1 else None
            index_2 = list(dict_2.keys()).index(key) if key in dict_2 else None
            value_1: Optional[T] = dict_1.get(key)
            value_2: Optional[T] = dict_2.get(key)
            result: JoinedResult[T] = JoinedResult(
                index_1, index_2, key, value_1, value_2
            )
            results[key] = result

        return results
