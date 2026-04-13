"""Utilities for recording index-build and query-evaluation metrics."""

from __future__ import annotations

from dataclasses import dataclass


query_times = []
index_times = []
last_query_time = None
last_index_time = None


@dataclass
class QueryTime:
    """Metrics captured for one query execution."""

    id_qualify: float
    data_store: float
    total_time: float
    index_used: str
    segments_total: int
    segments_skipped: int
    rows_scanned: int
    rows_matched: int
    bytes_read: int
    bitmap_lookups: int = 0
    bitmap_ops: int = 0
    false_positives: int = 0

    @property
    def skip_ratio(self) -> float:
        if self.segments_total == 0:
            return 0.0
        return self.segments_skipped / self.segments_total


@dataclass
class IndexTime:
    """Metrics captured for one index build."""

    time: float
    index_type: str
    table_name: str
    column_name: str
    chunks_indexed: int
    bytes_used: int


def record_query(metric: QueryTime) -> QueryTime:
    """Store one query metric entry and expose it as the latest query."""
    global last_query_time
    query_times.append(metric)
    last_query_time = metric
    return metric


def record_index(metric: IndexTime) -> IndexTime:
    """Store one index metric entry and expose it as the latest build."""
    global last_index_time
    index_times.append(metric)
    last_index_time = metric
    return metric


def reset_metrics() -> None:
    """Clear all recorded metrics."""
    global last_query_time, last_index_time
    query_times.clear()
    index_times.clear()
    last_query_time = None
    last_index_time = None
