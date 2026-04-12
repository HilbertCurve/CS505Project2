from typing import Callable

import numpy as np
import pandas as pd

import OLAP_system.database as database
from database import ColumnChunk

def rle_index(column: np.ndarray) -> np.ndarray:
    # step 1: initialize return values, pointer to start
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
    ret = ret.astype(np.uint64)
    return ret

def rle_get_tuple(chunk: ColumnChunk, index: list[int], col: str) -> pd.Series:
    ret: pd.Series = pd.Series(dtype=np.uint64)

    # algorithm: assumes index list is sorted
    # set ptr to 0, count to 0, index_ptr to 0
    ptr = 0
    count = 0
    index_ptr = 0

    rle_column = chunk.columns[col]
    while ptr < len(rle_column) and index_ptr < len(index):
        # if count plus next is greater than index[index_ptr]:
        if count + rle_column[ptr][1] > index[index_ptr]:
            # append value at index
            ret = pd.concat((ret, pd.Series([rle_column[ptr][0] for _ in range(rle_column[ptr][1])])), ignore_index=True)
            # inc index_ptr by 1
            index_ptr += 1
        else:
            # else: increment count by next, ptr by 1
            count += rle_column[ptr][1]
            ptr += 1
    return ret

class ZoneMap:
    def __init__(self, minimum, maximum, count):
        self.minimum = minimum
        self.maximum = maximum
        self.count = count

def zone_map_index(chunk: ColumnChunk, col: str):
    maximum = chunk.columns[col].max()
    minimum = chunk.columns[col].min()
    count = len(chunk.columns[col])
    chunk.zone_map = ZoneMap(minimum, maximum, count)


def rle_filter(chunk: ColumnChunk, predicate: Callable[[object, object], bool], args: str) -> list[int]:
    ret: list[int] = []
    start = 0
    for value in chunk.columns[args]:
        if predicate(chunk, int(value[0])):
            ret += range(start, start + value[1])
        start += value[1]
    return ret