"""Placeholder mostly encoding implementation."""
import sys

import numpy as np
import pandas as pd


class MostlyEncoding:
    def __init__(self, encoded: np.ndarray, inlier_indices: np.ndarray, outlier_indices: np.ndarray, outliers: np.ndarray, width: int):
        self.encoded = encoded
        self.inlier_indices = inlier_indices
        self.outlier_indices = outlier_indices
        self.outliers = outliers
        self.width = width

def build_index(column: np.ndarray, width) -> MostlyEncoding:
    max_val = 2 ** (width - 1)

    packed = column[column < max_val]
    match width:
        case 8:
            packed = packed.astype(np.int8)
        case 16:
            packed = packed.astype(np.int16)
        case 32:
            packed = packed.astype(np.int32)
        case _:
            raise AssertionError(f"Unsupported width '{width}'.")

    outlier_indices: np.ndarray = np.nonzero(column >= max_val)[0]

    inlier_indices: np.ndarray = np.arange(column.size)[column < max_val]
    assert len(inlier_indices) == len(packed)

    outliers = np.array(column[outlier_indices])

    return MostlyEncoding(packed, inlier_indices, outlier_indices, outliers, width)



def filter_chunk(chunk, operator: str, target, column: str) -> list[int]:
    ret: list[int] = []

    mostly: MostlyEncoding = chunk.mostly_indexes[column]

    # TODO: smarter range checks?
    for idx, value in enumerate(mostly.encoded):
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
            ret.append(mostly.inlier_indices[idx])
    for idx, value in enumerate(mostly.outliers):
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
            ret.append(mostly.outlier_indices[idx])

    return ret


def get_tuple(mostly: MostlyEncoding, index: list[int]) -> pd.Series:
    """Materialize values from delta bitpack."""
    ret: pd.Series = pd.Series([], dtype=int)

    for idx, inlier in enumerate(mostly.inlier_indices):
        # TODO: linear time possible if we assume index is sorted!
        if inlier in index:
            ret = pd.concat((ret, pd.Series(mostly.encoded[idx])))

    for idx, outlier in enumerate(mostly.outlier_indices):
        # TODO: linear time possible if we assume index is sorted!
        if outlier in index:
            ret = pd.concat((ret, pd.Series(mostly.encoded[idx])))

    return ret

def size_bytes(mostly: MostlyEncoding) -> int:
    return mostly.encoded.nbytes + mostly.inlier_indices.nbytes + mostly.outlier_indices.nbytes + mostly.outliers.nbytes
