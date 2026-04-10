import numpy as np

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
