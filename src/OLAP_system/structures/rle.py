"""Run-length encoding support for clustered or sorted columns."""

from __future__ import annotations

import numpy as np
import pandas as pd


def build_index(column: np.ndarray) -> np.ndarray:
    """Encode a column as pairs of ``(value, run_length)``."""
    # step 1: initialize return values, pointer to start of column
    ret: np.ndarray = np.array([])
    # step 2: cur_val = column[ptr]; count = 0
    ptr = 0
    # step 5: repeat steps 2-4 while ptr < len(column)
    while ptr < len(column):
        cur_val = column[ptr]
        count = 0
        # step 3: while ptr < len(column) and column[ptr] == cur_val: { ptr++; count++ }
        while ptr < len(column) and column[ptr] == cur_val:
            ptr += 1
            count += 1
        # step 4: store (cur_val, count)
        ret = np.append(ret, [cur_val, count])

    ret = np.reshape(ret, (int(ret.size / 2), 2))
    return ret.astype(np.uint64)


def get_tuple(encoded_column: np.ndarray, index: list[int]) -> pd.Series:
    """Materialize tuple values from an RLE-encoded column."""
    ret: pd.Series = pd.Series(dtype=np.uint64)

    # algorithm: assumes index list is sorted
    # set ptr to 0, count to 0, index_ptr to 0
    ptr = 0
    count = 0
    index_ptr = 0

    while ptr < len(encoded_column) and index_ptr < len(index):
        # if count plus next is greater than index[index_ptr]:
        if count + encoded_column[ptr][1] > index[index_ptr]:
            # append value at index
            ret = pd.concat((ret, pd.Series([encoded_column[ptr][0]])), ignore_index=True)
            # inc index_ptr by 1
            index_ptr += 1
        else:
            # else: increment count by next, ptr by 1
            count += encoded_column[ptr][1]
            ptr += 1
    return ret


def filter_encoded(encoded_column: np.ndarray, operator: str, target) -> list[int]:
    """Evaluate a predicate against an RLE-encoded numeric column."""
    ret: list[int] = []
    start = 0
    for value, run_length in encoded_column:
        if operator == "=":
            matches = int(value) == int(target)
        elif operator == ">":
            matches = int(value) > int(target)
        elif operator == ">=":
            matches = int(value) >= int(target)
        elif operator == "<":
            matches = int(value) < int(target)
        elif operator == "<=":
            matches = int(value) <= int(target)
        else:
            raise AssertionError(f"Unsupported operator '{operator}' for RLE.")

        if matches:
            ret += range(start, start + int(run_length))
        start += int(run_length)
    return ret


def filter_chunk(chunk, operator: str, target, column: str) -> list[int]:
    """Evaluate a predicate against one chunk's RLE-encoded column."""
    return filter_encoded(chunk.rle_indexes[column], operator, target)


def size_bytes(encoded_column: np.ndarray) -> int:
    """Return the encoded size for one RLE column in one chunk."""
    return int(encoded_column.nbytes)


def count_matches(encoded_column: np.ndarray, operator: str, target) -> int:
    """Count matching tuples directly on runs."""
    count = 0
    for value, run_length in encoded_column:
        if operator == "=":
            matches = int(value) == int(target)
        elif operator == ">":
            matches = int(value) > int(target)
        elif operator == ">=":
            matches = int(value) >= int(target)
        elif operator == "<":
            matches = int(value) < int(target)
        elif operator == "<=":
            matches = int(value) <= int(target)
        else:
            raise AssertionError(f"Unsupported operator '{operator}' for RLE.")

        if matches:
            count += int(run_length)
    return count
