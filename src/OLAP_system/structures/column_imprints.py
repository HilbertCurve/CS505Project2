"""Approximate range-bit imprints for group pruning."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


IMPRINT_GROUP_SIZE = 16
IMPRINT_BIN_COUNT = 16


@dataclass
class ColumnImprints:
    """Range-bit imprints stored per group inside one chunk."""

    column: str
    minimum: int
    maximum: int
    bin_edges: np.ndarray
    group_masks: np.ndarray
    row_count: int
    group_size: int = IMPRINT_GROUP_SIZE


def _build_equal_width_edges(values: np.ndarray, bin_count: int) -> np.ndarray:
    minimum = int(values.min())
    maximum = int(values.max())
    if minimum == maximum:
        return np.array([minimum, maximum + 1], dtype=np.int64)
    return np.linspace(minimum, maximum + 1, num=bin_count + 1, dtype=np.int64)


def _value_bucket(edges: np.ndarray, value: int) -> int:
    bucket = int(np.searchsorted(edges, value, side="right") - 1)
    return max(0, min(bucket, len(edges) - 2))


def _group_mask(group_values: np.ndarray, edges: np.ndarray) -> int:
    mask = 0
    for value in group_values:
        bucket = _value_bucket(edges, int(value))
        mask |= 1 << bucket
    return mask


def _required_mask(edges: np.ndarray, operator: str, target: int) -> int:
    bucket = _value_bucket(edges, int(target))
    bucket_count = len(edges) - 1

    if operator == "=":
        return 1 << bucket
    if operator in (">", ">="):
        return sum(1 << idx for idx in range(bucket, bucket_count))
    if operator in ("<", "<="):
        return sum(1 << idx for idx in range(bucket + 1))

    raise AssertionError(f"Unsupported operator '{operator}' for column imprints.")


def build_index(chunk, column: str) -> ColumnImprints:
    """Build one imprint mask per fixed-size group."""
    values = chunk.columns[column]
    if not np.issubdtype(values.dtype, np.integer):
        raise AssertionError("Column imprints currently support integer columns only.")

    edges = _build_equal_width_edges(values, IMPRINT_BIN_COUNT)
    group_masks = []
    for start in range(0, len(values), IMPRINT_GROUP_SIZE):
        group_masks.append(_group_mask(values[start : start + IMPRINT_GROUP_SIZE], edges))

    index = ColumnImprints(
        column=column,
        minimum=int(values.min()),
        maximum=int(values.max()),
        bin_edges=edges,
        group_masks=np.array(group_masks, dtype=np.uint32),
        row_count=len(values),
    )
    chunk.imprint_indexes[column] = index
    return index


def filter_chunk(index: ColumnImprints, operator: str, target: int, base_values: np.ndarray) -> tuple[np.ndarray, int]:
    """Prune groups by imprint masks, then verify candidate rows exactly."""
    required_mask = _required_mask(index.bin_edges, operator, int(target))

    candidate_groups = [
        group_id for group_id, group_mask in enumerate(index.group_masks) if int(group_mask) & required_mask
    ]

    candidate_rows: list[int] = []
    for group_id in candidate_groups:
        start = group_id * index.group_size
        stop = min(start + index.group_size, index.row_count)
        candidate_rows.extend(range(start, stop))

    if candidate_rows:
        candidate_idx = np.array(candidate_rows, dtype=int)
        candidate_values = base_values[candidate_idx]
        if operator == "=":
            exact_matches = candidate_values == int(target)
        elif operator == ">":
            exact_matches = candidate_values > int(target)
        elif operator == ">=":
            exact_matches = candidate_values >= int(target)
        elif operator == "<":
            exact_matches = candidate_values < int(target)
        elif operator == "<=":
            exact_matches = candidate_values <= int(target)
        else:
            raise AssertionError(f"Unsupported operator '{operator}' for column imprints.")

        matched_rows = candidate_idx[exact_matches]
        false_positives = int(len(candidate_idx) - len(matched_rows))
        return matched_rows, false_positives

    return np.array([], dtype=int), 0


def size_bytes(index: ColumnImprints) -> int:
    """Return the bytes occupied by this imprint index."""
    return int(index.bin_edges.nbytes + index.group_masks.nbytes)
