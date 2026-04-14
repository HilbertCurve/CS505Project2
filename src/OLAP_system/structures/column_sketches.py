"""Histogram-based column sketches for approximate pruning."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


SKETCH_GROUP_SIZE = 16
SKETCH_BIN_COUNT = 8


@dataclass
class ColumnSketches:
    """Per-group masks over histogram-derived sketch codes."""

    column: str
    bin_edges: np.ndarray
    group_masks: np.ndarray
    row_count: int
    group_size: int = SKETCH_GROUP_SIZE


def _build_histogram_edges(values: np.ndarray, bin_count: int) -> np.ndarray:
    quantiles = np.linspace(0.0, 1.0, num=bin_count + 1)
    edges = np.quantile(values, quantiles)
    edges = np.unique(edges.astype(np.int64))
    if len(edges) == 1:
        edges = np.array([edges[0], edges[0] + 1], dtype=np.int64)
    elif len(edges) < 2:
        minimum = int(values.min())
        edges = np.array([minimum, minimum + 1], dtype=np.int64)
    return edges


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

    raise AssertionError(f"Unsupported operator '{operator}' for column sketches.")


def build_index(chunk, column: str) -> ColumnSketches:
    """Build histogram-based sketch masks per group."""
    values = chunk.columns[column]
    if not np.issubdtype(values.dtype, np.integer):
        raise AssertionError("Column sketches currently support integer columns only.")

    edges = _build_histogram_edges(values, SKETCH_BIN_COUNT)
    group_masks = []
    for start in range(0, len(values), SKETCH_GROUP_SIZE):
        group_masks.append(_group_mask(values[start : start + SKETCH_GROUP_SIZE], edges))

    index = ColumnSketches(
        column=column,
        bin_edges=edges,
        group_masks=np.array(group_masks, dtype=np.uint16),
        row_count=len(values),
    )
    chunk.sketch_indexes[column] = index
    return index


def filter_chunk(index: ColumnSketches, operator: str, target: int, base_values: np.ndarray) -> tuple[np.ndarray, int]:
    """Prune groups by sketch codes, then verify candidate rows exactly."""
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
            raise AssertionError(f"Unsupported operator '{operator}' for column sketches.")

        matched_rows = candidate_idx[exact_matches]
        false_positives = int(len(candidate_idx) - len(matched_rows))
        return matched_rows, false_positives

    return np.array([], dtype=int), 0


def size_bytes(index: ColumnSketches) -> int:
    """Return the bytes occupied by this sketch index."""
    return int(index.bin_edges.nbytes + index.group_masks.nbytes)
