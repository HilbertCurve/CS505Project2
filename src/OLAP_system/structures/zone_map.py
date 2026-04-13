"""Zone map metadata for chunk-level pruning."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ZoneMap:
    """Simple min/max metadata for one chunk column."""

    minimum: object
    maximum: object
    count: int


def build_index(chunk, column: str) -> ZoneMap:
    """Attach zone-map metadata to a chunk column."""
    zone_map = ZoneMap(
        minimum=chunk.columns[column].min(),
        maximum=chunk.columns[column].max(),
        count=len(chunk.columns[column]),
    )
    chunk.zone_maps[column] = zone_map
    return zone_map


def can_skip(zone_map: ZoneMap, operator: str, value) -> bool:
    """Return whether a predicate cannot match any row in this chunk."""
    operator = operator.upper()

    if operator == "=":
        return value < zone_map.minimum or value > zone_map.maximum
    if operator == ">":
        return zone_map.maximum <= value
    if operator == ">=":
        return zone_map.maximum < value
    if operator == "<":
        return zone_map.minimum >= value
    if operator == "<=":
        return zone_map.minimum > value

    raise AssertionError(f"Unsupported operator '{operator}' for zone maps.")


def size_bytes(zone_map: ZoneMap) -> int:
    """Approximate the metadata footprint for one zone map."""
    return (
        zone_map.minimum.__sizeof__()
        + zone_map.maximum.__sizeof__()
        + zone_map.count.__sizeof__()
    )
