from typing import Dict, Any

from spark_data_frame_comparer.utilities.dictionary_joiner.dictionary_joiner import (
    DictionaryJoiner,
)
from spark_data_frame_comparer.utilities.dictionary_joiner.joined_result import (
    JoinedResult,
)


# Define a helper function to compare JoinedResult instances
def compare_joined_results(
    result: JoinedResult[Any], expected: JoinedResult[Any]
) -> bool:
    return (
        result.index_1 == expected.index_1
        and result.index_2 == expected.index_2
        and result.key == expected.key
        and result.value_1 == expected.value_1
        and result.value_2 == expected.value_2
    )


# Test case 1: Test with two dictionaries that have the same keys
def test_join_dicts_same_keys() -> None:
    dict_1: Dict[str, Any] = {"a": 1, "b": 2}
    dict_2: Dict[str, Any] = {"a": 3, "b": 4}

    result: Dict[str, JoinedResult[Any]] = DictionaryJoiner.join_dicts(dict_1, dict_2)

    expected_result: Dict[str, JoinedResult[Any]] = {
        "a": JoinedResult(0, 0, "a", 1, 3),
        "b": JoinedResult(1, 1, "b", 2, 4),
    }

    for key, expected in expected_result.items():
        assert key in result
        assert compare_joined_results(result[key], expected)


# Test case 2: Test with two dictionaries that have different keys
def test_join_dicts_different_keys() -> None:
    dict_1: Dict[str, Any] = {"a": 1, "b": 2}
    dict_2: Dict[str, Any] = {"c": 3, "d": 4}

    result: Dict[str, JoinedResult[Any]] = DictionaryJoiner.join_dicts(dict_1, dict_2)

    expected_result: Dict[str, JoinedResult[Any]] = {
        "a": JoinedResult(0, None, "a", 1, None),
        "b": JoinedResult(1, None, "b", 2, None),
        "c": JoinedResult(None, 0, "c", None, 3),
        "d": JoinedResult(None, 1, "d", None, 4),
    }

    for key, expected in expected_result.items():
        assert key in result
        assert compare_joined_results(result[key], expected)


# Test case 3: Test with one empty dictionary
def test_join_dicts_one_empty() -> None:
    dict_1: Dict[str, Any] = {}
    dict_2: Dict[str, Any] = {"a": 1, "b": 2}

    result: Dict[str, JoinedResult[Any]] = DictionaryJoiner.join_dicts(dict_1, dict_2)

    expected_result: Dict[str, JoinedResult[Any]] = {
        "a": JoinedResult(None, 0, "a", None, 1),
        "b": JoinedResult(None, 1, "b", None, 2),
    }

    for key, expected in expected_result.items():
        assert key in result
        assert compare_joined_results(result[key], expected)


# Test case 4: Test with both dictionaries empty
def test_join_dicts_both_empty() -> None:
    dict_1: Dict[str, Any] = {}
    dict_2: Dict[str, Any] = {}

    result: Dict[str, JoinedResult[Any]] = DictionaryJoiner.join_dicts(dict_1, dict_2)

    assert result == {}


# Test case 5: Test with overlapping and non-overlapping keys
def test_join_dicts_overlap_and_non_overlap() -> None:
    dict_1: Dict[str, Any] = {"a": 1, "b": 2}
    dict_2: Dict[str, Any] = {"b": 3, "c": 4}

    result: Dict[str, JoinedResult[Any]] = DictionaryJoiner.join_dicts(dict_1, dict_2)

    expected_result: Dict[str, JoinedResult[Any]] = {
        "a": JoinedResult(0, None, "a", 1, None),
        "b": JoinedResult(1, 0, "b", 2, 3),
        "c": JoinedResult(None, 1, "c", None, 4),
    }

    for key, expected in expected_result.items():
        assert key in result
        assert compare_joined_results(result[key], expected)
