from typing import Dict, Any, Set

from spark_data_frame_comparer.utilities.dictionary_joiner.joined_result import (
    JoinedResult,
)


class DictionaryJoiner:
    @staticmethod
    def join_dicts(
        dict_1: Dict[str, Any], dict_2: Dict[str, Any]
    ) -> Dict[str, JoinedResult]:
        """
        Join two dictionaries into a single dictionary with the same keys.

        :param dict_1: The first dictionary to join.
        :param dict_2: The second dictionary to join.
        :return: A dictionary with the same keys as the input dictionaries, with values from both dictionaries
        """
        combined_keys: Set[str] = set(dict_1.keys()).union(set(dict_2.keys()))

        results: Dict[str, JoinedResult] = {}

        for key in combined_keys:
            index_1 = list(dict_1.keys()).index(key) if key in dict_1 else None
            index_2 = list(dict_2.keys()).index(key) if key in dict_2 else None
            value_1 = dict_1.get(key, None)
            value_2 = dict_2.get(key, None)
            result: JoinedResult = JoinedResult(index_1, index_2, key, value_1, value_2)
            results[key] = result

        return results
