"""Placeholder dictionary encoding implementation."""
import sys

import numpy as np
import pandas as pd
from numpy.dtypes import StringDType


class DictionaryEncoding:
    def __init__(self, dictionary: dict[str, int], decoder: dict[int, str], encoded: np.ndarray):
        self.dictionary = dictionary
        self.decoder = decoder
        self.encoded = encoded

def build_index(column: np.ndarray, *_args, **_kwargs) -> DictionaryEncoding:
    ret: np.ndarray = np.zeros(column.shape, dtype=np.uint64)
    dictionary: dict[str, int] = {}
    decoder: dict[int, str] = {}
    # mapper is only used in this function to convert values in the indexed column to dictionary-encoded ones effectively.
    keys: set[str] = set()
    for value in column:
        if value not in keys:
            keys.add(value)
    keys: list[str] = list(keys)
    keys.sort()

    number = 0
    for value in keys:
        dictionary[value] = number
        decoder[number] = value
        number += 10

    for idx, value in enumerate(column):
        ret[idx] = dictionary[value]

    return DictionaryEncoding(dictionary, decoder, ret)


def filter_chunk(chunk, operator: str, target, column: str) -> list[int]:
    """Evaluate a predicate against one chunk's dictionary-enocded column."""
    ret: list[int] = []

    de: DictionaryEncoding = chunk.dictionary_indexes[column]
    target_val: int = de.dictionary[target]
    for idx, value in enumerate(de.encoded):
        if operator == "=":
            matches = int(target_val) == int(value)
        else:
            raise AssertionError(f"Unsupported operator '{operator}' for dictionary encoding.")
        if matches:
            ret += [idx]

    return ret

def get_tuple(de: DictionaryEncoding, index: list[int]) -> pd.Series:
    """Materialize string values from dictionary-encoded column."""
    ret: pd.Series = pd.Series(dtype=StringDType())

    for i in index:
        ret = pd.concat((ret, pd.Series([de.decoder[de.encoded[i]]])))

    return ret

def size_bytes(de: DictionaryEncoding):
    return int(de.encoded.nbytes) + int(sys.getsizeof(de.dictionary)) + int(sys.getsizeof(de.decoder))
