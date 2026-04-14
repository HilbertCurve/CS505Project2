"""Classes and routines for loading data into the in-memory OLAP store."""

from __future__ import annotations

from dataclasses import dataclass
import numpy as np
import pandas as pd

from OLAP_system.structures import bitmap_index, column_imprints, column_sketches, rle, zone_map, dictionary_encoding, delta_bitpacking, mostly_encoding
from OLAP_system.timer import IndexTime, QueryTime, record_index, record_query

# A mapping between table names and their respective tables.
system_information = {}


@dataclass
class PredicateClause:
    """One simple comparison predicate."""

    column: str
    operator: str
    value: object


@dataclass
class ClauseNode:
    clause: PredicateClause


@dataclass
class UnaryNode:
    operator: str
    child: object


@dataclass
class BinaryNode:
    operator: str
    left: object
    right: object


@dataclass
class QueryRuntime:
    """Mutable counters accumulated during query evaluation."""

    segments_total: int = 0
    segments_skipped: int = 0
    rows_scanned: int = 0
    bytes_read: int = 0
    bitmap_lookups: int = 0
    bitmap_ops: int = 0
    zone_map_prunes: int = 0
    zone_map_used: bool = False
    bitmap_used: bool = False
    rle_used: bool = False
    dictionary_used: bool = False
    dbp_used: bool = False
    mostly_used: bool = False
    imprints_used: bool = False
    sketches_used: bool = False
    false_positives: int = 0


def _parse_literal(token: str) -> object:
    if token.startswith(("'", '"')) and token.endswith(("'", '"')):
        return token[1:-1]

    try:
        return int(token)
    except ValueError:
        return token


def parse_predicate_tokens(tokens: list[str]) -> ClauseNode | UnaryNode | BinaryNode | None:
    """Parse a flat WHERE token list into a small expression tree."""
    if not tokens:
        return None

    index = 0

    def parse_factor():
        nonlocal index
        if index >= len(tokens):
            raise AssertionError("Unexpected end of predicate.")

        if tokens[index].upper() == "NOT":
            index += 1
            return UnaryNode("NOT", parse_factor())

        if index + 2 >= len(tokens):
            raise AssertionError("Incomplete predicate clause.")

        clause = ClauseNode(
            PredicateClause(
                column=tokens[index],
                operator=tokens[index + 1].upper(),
                value=_parse_literal(tokens[index + 2]),
            )
        )
        index += 3
        return clause

    def parse_term():
        nonlocal index
        node = parse_factor()
        while index < len(tokens) and tokens[index].upper() == "AND":
            index += 1
            node = BinaryNode("AND", node, parse_factor())
        return node

    def parse_expression():
        nonlocal index
        node = parse_term()
        while index < len(tokens) and tokens[index].upper() == "OR":
            index += 1
            node = BinaryNode("OR", node, parse_term())
        return node

    tree = parse_expression()
    if index != len(tokens):
        raise AssertionError(f"Unexpected predicate token '{tokens[index]}'.")
    return tree


def _compare_scalar(value, operator: str, target) -> bool:
    operator = operator.upper()
    if operator == "=":
        return value == target
    if operator == ">":
        return value > target
    if operator == ">=":
        return value >= target
    if operator == "<":
        return value < target
    if operator == "<=":
        return value <= target
    raise AssertionError(f"Unsupported operator '{operator}'.")


def _compare_array(values: np.ndarray, operator: str, target) -> np.ndarray:
    operator = operator.upper()
    if operator == "=":
        return values == target
    if operator == ">":
        return values > target
    if operator == ">=":
        return values >= target
    if operator == "<":
        return values < target
    if operator == "<=":
        return values <= target
    raise AssertionError(f"Unsupported operator '{operator}'.")


def _normalize_value(value):
    return value.item() if hasattr(value, "item") else value


def _expression_can_skip(chunk, node) -> bool:
    """Return whether the full predicate is impossible for this chunk."""
    if node is None:
        return False

    if isinstance(node, ClauseNode):
        clause = node.clause
        if clause.column in chunk.zone_maps:
            return zone_map.can_skip(chunk.zone_maps[clause.column], clause.operator, clause.value)
        return False

    if isinstance(node, UnaryNode):
        return False

    if isinstance(node, BinaryNode):
        if node.operator == "AND":
            return _expression_can_skip(chunk, node.left) or _expression_can_skip(chunk, node.right)
        if node.operator == "OR":
            return _expression_can_skip(chunk, node.left) and _expression_can_skip(chunk, node.right)

    return False


class ColumnChunk:
    """Represents a fixed-size chunk of a table."""

    MAX_CHUNK_SIZE = 1024

    def __init__(self, column_chunk: pd.DataFrame, types: list[str]):
        assert self.MAX_CHUNK_SIZE >= len(column_chunk) > 0
        self.columns: dict[str, np.ndarray] = {}
        for idx, col in enumerate(column_chunk.columns):
            if types[idx] == "INTEGER":
                self.columns[col] = column_chunk[col].to_numpy(dtype=int)
            else:
                self.columns[col] = column_chunk[col].to_numpy(dtype=str)

        self.length = len(column_chunk)
        self.indexes: dict[str, str] = {}
        self.index_sizes: dict[tuple[str, str], int] = {}
        self.zone_maps: dict[str, zone_map.ZoneMap] = {}
        self.bitmap_indexes: dict[str, bitmap_index.BitmapIndex] = {}
        self.rle_indexes: dict[str, np.ndarray] = {}
        self.dictionary_indexes: dict[str, dictionary_encoding.DictionaryEncoding] = {}
        self.dbp_indexes: dict[str, delta_bitpacking.DBPEncoding] = {}
        self.mostly_indexes: dict[str, mostly_encoding.MostlyEncoding] = {}
        self.imprint_indexes: dict[str, column_imprints.ColumnImprints] = {}
        self.sketch_indexes: dict[str, column_sketches.ColumnSketches] = {}

    def _scan_clause(self, clause: PredicateClause, runtime: QueryRuntime) -> np.ndarray:
        values = self.columns[clause.column]
        runtime.rows_scanned += self.length
        runtime.bytes_read += int(values.nbytes)
        return _compare_array(values, clause.operator, clause.value)

    def _evaluate_clause(self, clause: PredicateClause, runtime: QueryRuntime) -> np.ndarray:
        if clause.column not in self.columns:
            raise AssertionError(f"Column '{clause.column}' not in table.")

        if clause.column in self.bitmap_indexes and clause.operator == "=":
            bitmap, lookups = bitmap_index.filter_chunk(self.bitmap_indexes[clause.column], clause.value)
            runtime.bitmap_lookups += lookups
            runtime.bitmap_used = True
            return bitmap

        if clause.column in self.rle_indexes:
            match_indices = rle.filter_chunk(self, clause.operator, clause.value, clause.column)
            runtime.rle_used = True
            bitmap = np.zeros(self.length, dtype=bool)
            bitmap[match_indices] = True
            return bitmap

        if clause.column in self.dictionary_indexes:
            match_indices = dictionary_encoding.filter_chunk(self, clause.operator, clause.value, clause.column)
            runtime.dictionary_used = True
            bitmap = np.zeros(self.length, dtype=bool)
            bitmap[match_indices] = True
            return bitmap

        if clause.column in self.dbp_indexes:
            match_indices = delta_bitpacking.filter_chunk(self, clause.operator, clause.value, clause.column)
            runtime.delta_bitmap_used = True
            bitmap = np.zeros(self.length, dtype=bool)
            bitmap[match_indices] = True
            return bitmap

        if clause.column in self.mostly_indexes:
            match_indices = mostly_encoding.filter_chunk(self, clause.operator, clause.value, clause.column)
            runtime.mostly_used = True
            bitmap = np.zeros(self.length, dtype=bool)
            bitmap[match_indices] = True
            return bitmap

        if clause.column in self.imprint_indexes:
            match_indices, false_positives = column_imprints.filter_chunk(
                self.imprint_indexes[clause.column], clause.operator, clause.value, self.columns[clause.column]
            )
            runtime.imprints_used = True
            runtime.false_positives += false_positives
            runtime.rows_scanned += int(false_positives + len(match_indices))
            runtime.bytes_read += int((false_positives + len(match_indices)) * self.columns[clause.column].dtype.itemsize)
            bitmap = np.zeros(self.length, dtype=bool)
            bitmap[match_indices] = True
            return bitmap

        if clause.column in self.sketch_indexes:
            match_indices, false_positives = column_sketches.filter_chunk(
                self.sketch_indexes[clause.column], clause.operator, clause.value, self.columns[clause.column]
            )
            runtime.sketches_used = True
            runtime.false_positives += false_positives
            runtime.rows_scanned += int(false_positives + len(match_indices))
            runtime.bytes_read += int((false_positives + len(match_indices)) * self.columns[clause.column].dtype.itemsize)
            bitmap = np.zeros(self.length, dtype=bool)
            bitmap[match_indices] = True
            return bitmap

        if clause.column in self.zone_maps and zone_map.can_skip(
            self.zone_maps[clause.column], clause.operator, clause.value
        ):
            runtime.zone_map_prunes += 1
            runtime.zone_map_used = True
            return np.zeros(self.length, dtype=bool)

        return self._scan_clause(clause, runtime)

    def evaluate_predicate(self, node, runtime: QueryRuntime) -> np.ndarray:
        """Evaluate a parsed predicate tree for this chunk."""
        if node is None:
            return np.ones(self.length, dtype=bool)

        if isinstance(node, ClauseNode):
            return self._evaluate_clause(node.clause, runtime)

        if isinstance(node, UnaryNode):
            child = self.evaluate_predicate(node.child, runtime)
            runtime.bitmap_ops += 1
            return ~child

        if isinstance(node, BinaryNode):
            left = self.evaluate_predicate(node.left, runtime)
            right = self.evaluate_predicate(node.right, runtime)
            runtime.bitmap_ops += 1
            if node.operator == "AND":
                return left & right
            if node.operator == "OR":
                return left | right
            raise AssertionError(f"Unsupported boolean operator '{node.operator}'.")

        raise AssertionError("Unsupported predicate node.")

    def get_tuple(self, index: list[int], column_names: list[str]) -> pd.DataFrame:
        bad_cols = [col for col in column_names if col not in self.columns]
        if bad_cols:
            raise AssertionError(f"Column(s) '{bad_cols}' not in table.")

        col_matches = pd.DataFrame(index=index)

        for col in column_names:
            if self.indexes.get(col) == "rle":
                col_match = rle.get_tuple(self.rle_indexes[col], index)
            elif self.indexes.get(col) == "dictionary":
                col_match = dictionary_encoding.get_tuple(self.dictionary_indexes[col], index)
            elif self.indexes.get(col) == "dbp":
                col_match = delta_bitpacking.get_tuple(self.dbp_indexes[col], index)
            elif self.indexes.get(col) == "mostly":
                col_match = mostly_encoding.get_tuple(self.mostly_indexes[col], index)
            else:
                col_match = pd.Series(self.columns[col][index], index=index)
            col_matches[col] = col_match.to_list()

        return col_matches

    def make_index(self, column: str, indexer: str) -> int:
        assert column in self.columns
        indexer = indexer.lower()

        match indexer:
            case "rle":
                encoded = rle.build_index(self.columns[column])
                self.rle_indexes[column] = encoded
                size = rle.size_bytes(encoded)
            case "zone_map":
                metadata = zone_map.build_index(self, column)
                size = zone_map.size_bytes(metadata)
            case "bitmap":
                metadata = bitmap_index.build_index(self, column)
                size = bitmap_index.size_bytes(metadata)
            case "dictionary":
                de = dictionary_encoding.build_index(self.columns[column])
                self.dictionary_indexes[column] = de
                size = dictionary_encoding.size_bytes(de)
            case "dbp":
                dbp = delta_bitpacking.build_index(self.columns[column])
                self.dbp_indexes[column] = dbp
                size = delta_bitpacking.size_bytes(dbp)
            case "mostly8":
                mostly = mostly_encoding.build_index(self.columns[column], width=8)
                self.mostly_indexes[column] = mostly
                size = mostly_encoding.size_bytes(mostly)
            case "mostly16":
                mostly = mostly_encoding.build_index(self.columns[column], width=16)
                self.mostly_indexes[column] = mostly
                size = mostly_encoding.size_bytes(mostly)
            case "mostly32":
                mostly = mostly_encoding.build_index(self.columns[column], width=32)
                self.mostly_indexes[column] = mostly
                size = mostly_encoding.size_bytes(mostly)
            case "imprints" | "column_imprints":
                metadata = column_imprints.build_index(self, column)
                size = column_imprints.size_bytes(metadata)
            case "sketches" | "column_sketches":
                metadata = column_sketches.build_index(self, column)
                size = column_sketches.size_bytes(metadata)
            case _:
                raise AssertionError(f"Indexer {indexer} not found.")

        self.indexes[column] = indexer
        self.index_sizes[(column, indexer)] = size
        return size


class Table:
    """Class used for storing CSV table data."""

    def __len__(self):
        return self.length

    def __init__(self, table_name: str, column_schema: dict):
        self.name: str = table_name
        self.column_schema: dict[str, tuple[str, int]] = column_schema
        self.chunks: list[ColumnChunk] = []
        self.length: int = 0

    def load_csv(self, path: str):
        with open(path, encoding="utf-8") as f:
            headers = f.readline().strip().split(",")
            if len(headers) != len(self.column_schema):
                raise AssertionError(f"Expected CSV with {len(self.column_schema)} columns, got {len(headers)}!")

            curr_chunk = pd.DataFrame(columns=headers)
            types = []
            for col in self.column_schema:
                types.append(self.column_schema[col][0])
                if self.column_schema[col][0] == "INTEGER":
                    curr_chunk[col] = pd.to_numeric(curr_chunk[col])
                else:
                    curr_chunk[col] = curr_chunk[col].astype(str)

            curr_chunk_size = 0

            for line in f:
                cols = line.strip().split(",")
                curr_chunk.loc[curr_chunk_size] = cols

                curr_chunk_size += 1
                self.length += 1

                if curr_chunk_size == ColumnChunk.MAX_CHUNK_SIZE:
                    self.chunks.append(ColumnChunk(curr_chunk, types))
                    curr_chunk = pd.DataFrame(columns=headers)
                    for col in self.column_schema:
                        if self.column_schema[col][0] == "INTEGER":
                            curr_chunk[col] = pd.to_numeric(curr_chunk[col])
                        else:
                            curr_chunk[col] = curr_chunk[col].astype(str)
                    curr_chunk_size = 0

            if curr_chunk_size > 0:
                self.chunks.append(ColumnChunk(curr_chunk, types))

    def column_type(self, name: str) -> tuple[str, int]:
        return self.column_schema[name]


def create_table(table_name: str, columns: dict):
    """Create a table with preset columns."""
    system_information[table_name] = Table(table_name, columns)


def load_csv(table_name: str, filename: str):
    """Load a CSV file into the in-memory database."""
    if table_name not in system_information:
        raise AssertionError(f"Table '{table_name}' not created yet.")
    system_information[table_name].load_csv(filename)


def reset_database():
    """Clear all in-memory tables."""
    system_information.clear()


def print_summary():
    print(f"ColumnChunk.MAX_CHUNK_SIZE: {ColumnChunk.MAX_CHUNK_SIZE}.")
    for table in system_information.values():
        print(f"{table.name}: {len(table.chunks)} chunks.")
        print(f"\tcols: {table.column_schema}.")


def handle_select(table_name: str, column_names: list[str], predicate: list[str]):
    """Process a SELECT query and return qualifying tuples."""
    import time

    start = time.time()
    table = system_information[table_name]
    predicate_tree = parse_predicate_tokens(predicate)

    runtime = QueryRuntime(segments_total=len(table.chunks))
    qualified_ids: list[tuple[int, list[int]]] = []
    for chunk_id, chunk in enumerate(table.chunks):
        if _expression_can_skip(chunk, predicate_tree):
            runtime.segments_skipped += 1
            runtime.zone_map_used = True
            continue

        if predicate_tree is None:
            chunk_matches = np.ones(chunk.length, dtype=bool)
        else:
            chunk_matches = chunk.evaluate_predicate(predicate_tree, runtime)

        match_indices = np.flatnonzero(chunk_matches).tolist()
        if match_indices:
            qualified_ids.append((chunk_id, match_indices))

    id_qualify = time.time()

    if column_names == ["*"]:
        column_names = list(table.column_schema.keys())

    res = pd.DataFrame(columns=column_names)
    for chunk_id, indexes in qualified_ids:
        qualified_tuples = table.chunks[chunk_id].get_tuple(indexes, column_names)
        res = pd.concat((res, qualified_tuples))

    data_store = time.time()

    index_types_used = []
    if runtime.bitmap_used:
        index_types_used.append("bitmap")
    if runtime.imprints_used:
        index_types_used.append("imprints")
    if runtime.rle_used:
        index_types_used.append("rle")
    if runtime.sketches_used:
        index_types_used.append("sketches")
    if runtime.zone_map_used:
        index_types_used.append("zone_map")
    if runtime.dictionary_used:
        index_types_used.append("dictionary")
    if runtime.dbp_used:
        index_types_used.append("dbp")
    if runtime.mostly_used:
        index_types_used.append("mostly")

    metric = QueryTime(
        id_qualify=id_qualify - start,
        data_store=data_store - id_qualify,
        total_time=data_store - start,
        index_used=", ".join(index_types_used) if index_types_used else "baseline",
        segments_total=runtime.segments_total,
        segments_skipped=runtime.segments_skipped,
        rows_scanned=runtime.rows_scanned,
        rows_matched=len(res),
        bytes_read=runtime.bytes_read,
        bitmap_lookups=runtime.bitmap_lookups,
        bitmap_ops=runtime.bitmap_ops,
        false_positives=runtime.false_positives,
    )
    record_query(metric)

    return res


def handle_index(table_name: str, column_name: str, indexer: str):
    """Build one index structure across all chunks of a table."""
    import time

    table = system_information[table_name]
    start = time.time()

    total_bytes = 0
    for chunk in table.chunks:
        total_bytes += chunk.make_index(column_name, indexer)

    end = time.time()
    metric = IndexTime(
        time=end - start,
        index_type=indexer.lower(),
        table_name=table_name,
        column_name=column_name,
        chunks_indexed=len(table.chunks),
        bytes_used=total_bytes,
    )
    record_index(metric)
    return metric
