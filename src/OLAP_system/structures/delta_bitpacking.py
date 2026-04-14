"""Placeholder delta + bit packing implementation."""
import sys

import numpy as np
import pandas as pd


class DBPEncoding:
    def __init__(self, start: int, deltas: np.ndarray, count: int, bit_len: int):
        self.start = start
        self.deltas = deltas
        self.count = count
        self.bit_len = bit_len

def build_index(column: np.ndarray) -> DBPEncoding:
    start = column[0]
    deltas = column[1:] - column[:-1]
    count = len(deltas)

    max_delta = np.max(np.abs(deltas))
    if max_delta < 2 ** 7:
        deltas = np.astype(deltas, np.int8)
        bit_len = 8
    elif max_delta < 2 ** 15:
        deltas = np.astype(deltas, np.int16)
        bit_len = 16
    elif max_delta < 2 ** 31:
        deltas = np.astype(deltas, np.int32)
        bit_len = 32
    else:
        # horrible!
        deltas = np.astype(deltas, np.int64)
        bit_len = 64

    return DBPEncoding(start, deltas, count, bit_len)


def evaluate_operator(value, operator, target):
    if operator == "=":
        return int(value) == int(target)
    elif operator == ">":
        return int(value) > int(target)
    elif operator == ">=":
        return int(value) >= int(target)
    elif operator == "<":
        return int(value) < int(target)
    elif operator == "<=":
        return int(value) <= int(target)
    else:
        raise AssertionError(f"Unsupported operator '{operator}' for RLE.")


def filter_chunk(chunk, operator: str, target, column: str) -> list[int]:
    ret: list[int] = []

    dbp: DBPEncoding = chunk.dbp_indexes[column]
    value = dbp.start
    if evaluate_operator(value, operator, target):
        ret.append(0)

    for idx, delta in enumerate(dbp.deltas):
        value += delta
        if evaluate_operator(value, operator, target):
            # plus one here because we don't include index of "start" element
            ret.append(idx + 1)

    return ret


def get_tuple(dbp: DBPEncoding, index: list[int]) -> pd.Series:
    """Materialize values from delta bitpack."""
    ret: pd.Series = pd.Series([], dtype=int)
    # TODO: we are assuming that index is sorted!
    value = dbp.start
    if len(index) == 0:
        return ret

    index_ptr = 0

    if index[0] == 0:
        ret = pd.concat((ret, pd.Series([value])))
        index_ptr += 1

    for idx, delta in enumerate(dbp.deltas):
        value += delta
        if idx + 1 == index[index_ptr]:
            ret.append(value)
            index_ptr += 1

    return ret

def size_bytes(dbp: DBPEncoding) -> int:
    return dbp.deltas.nbytes + sys.getsizeof(dbp.start) + sys.getsizeof(dbp.count) + sys.getsizeof(dbp.bit_len)
