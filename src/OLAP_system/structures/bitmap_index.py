"""Equality-encoded bitmap indexes for low-cardinality columns."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass
class BitmapIndex:
    """Bitmap index stored for one chunk column."""

    column: str
    bitmaps: dict[object, np.ndarray]
    row_count: int

    @property
    def cardinality(self) -> int:
        return len(self.bitmaps)


def build_index(chunk, column: str) -> BitmapIndex:
    """Build one bitmap per distinct value in a chunk column."""
    values = chunk.columns[column]
    bitmaps: dict[object, np.ndarray] = {}
    for value in np.unique(values):
        bitmaps[value.item() if hasattr(value, "item") else value] = values == value

    index = BitmapIndex(column=column, bitmaps=bitmaps, row_count=len(values))
    chunk.bitmap_indexes[column] = index
    return index


def filter_chunk(index: BitmapIndex, value) -> tuple[np.ndarray, int]:
    """Return the equality bitmap for one value plus lookup op count."""
    bitmap = index.bitmaps.get(value)
    if bitmap is None:
        return np.zeros(index.row_count, dtype=bool), 1
    return bitmap.copy(), 1


def size_bytes(index: BitmapIndex) -> int:
    """Return the bytes occupied by all bitmaps in this chunk index."""
    return sum(bitmap.nbytes for bitmap in index.bitmaps.values())
