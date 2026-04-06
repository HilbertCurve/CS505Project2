"""Classes and routines for loading data from storage into the in-memory database.

The data is stored in a columnar format and only supports integer and string
types. The following will NOT be supported:
- Updates or post-load inserts
- Joins
- Off-disk storage

Here is the design specification for the database we are creating:

Column segments store a certain type. It is equivalent to an immutable list of a
certain type and length.

A column is a contiguous storage of column segments.

A table is a string-indexed map of columns whose length are all
equivalent.

A database is a string-indexed map of tables.
"""
from typing import Callable

import pandas as pd

from OLAP_system import query_lookup
from OLAP_system.timer import QueryTime, query_times

# A mapping between table names and their respective tables.
system_information = {}

class ColumnChunk:
    """Represents a fixed-size chunk of a column in a table.

    Attributes:
        MAX_CHUNK_SIZE (int): A constant for the max size of a chunk,
        columns (dict): An association between column names and their internal data,
        size (int): The number of values in the chunk.
    """
    MAX_CHUNK_SIZE = 1024
    def __init__(self, column_chunk: pd.DataFrame):
        assert self.MAX_CHUNK_SIZE >= len(column_chunk) > 0 # what is this python syntax O_o
        self.columns: dict[str, pd.Series] = {}
        for col in column_chunk.columns:
            self.columns[col] = column_chunk[col].copy()

        self.size = len(column_chunk)

    def filter(self, predicate: Callable[[object, object], bool], args: str):
        # the following is for a vanilla-encoded tuple (type-agnostic)
        # TODO: convert to np.ndarray
        qualified_ids: list[int] = []
        for idx, value in enumerate(self.columns[args]):
            if predicate(self, int(value)):
                qualified_ids.append(idx)
        return qualified_ids

    def get_tuple(self, index: list[int], column_names: list[str]) -> list[tuple[int, object]]:
        # the following is for a vanilla-encoded tuple (type-agnostic)
        ret = []
        bad_cols = []
        for col in column_names:
            if col not in self.columns:
                bad_cols.append(col)

        if bad_cols:
            raise AssertionError(f"Column(s) '{bad_cols}' not in table.")

        for col in column_names:
            for idx in index:
                ret.append((idx, self.columns[col].loc[idx]))
        return ret

class Table:
    """Class used for storing csv table data.

    Attributes:
        name (str): The table's identifier,
        column_schema (dict): The mapping from column names to the type of the column,
        chunks (dict): A mapping of column names to the internal storage of that column,
        length (int): The number of tuples in the table.
    """
    def __len__(self):
        return self.length

    def __init__(self, table_name: str, column_schema: dict):
        self.name: str = table_name
        self.column_schema: dict[str, tuple[str, int]] = column_schema
        self.chunks: list[ColumnChunk] = []
        self.length: int = 0

    def load_csv(self, path: str):
        with open(path) as f:
            # initialize empty column chunks
            headers = f.readline().strip().split(",")
            if len(headers) != len(self.column_schema):
                raise AssertionError(f"Expected CSV with {len(self.column_schema)} columns, got {len(headers)}!")

            curr_chunk = pd.DataFrame(columns=headers)
            for col in self.column_schema:
                if self.column_schema[col][0] == "INTEGER":
                    curr_chunk[col] = pd.to_numeric(curr_chunk[col])
                else:
                    curr_chunk[col] = curr_chunk[col].astype(str)

            curr_chunk_size = 0

            # populate columns
            for line in f:
                cols = line.strip().split(",")
                curr_chunk.loc[curr_chunk_size] = cols

                curr_chunk_size += 1
                self.length += 1

                # populate column chunks in database once max capacity is reached
                if curr_chunk_size == ColumnChunk.MAX_CHUNK_SIZE:
                    self.chunks.append(ColumnChunk(curr_chunk))
                    curr_chunk_size = 0

            # insert remaining stub if partially-filled chunk
            if curr_chunk_size > 0:
                self.chunks.append(ColumnChunk(curr_chunk))

    def column_type(self, name: str) -> tuple[str, int]:
        """Returns the type of a column.

        The type is stored as a tuple. The first entry in the tuple is a string that takes two values, "INTEGER" or
        "VARCHAR". If "VARCHAR" is stored, the second value is the length of the VARCHAR. Otherwise, None is stored.
        """
        return self.column_schema[name]

def create_table(table_name: str, columns: dict):
    """Create a table with preset columns.

    Args:
        table_name (str): The SQL-viable name of the table,
        columns (dict): A mapping of column names to the type of the column,
            which itself is a map, mapping "INTEGER" to None and "VARCHAR" to the
            column's length in characters.
    """
    system_information[table_name] = Table(table_name, columns)

def load_csv(table_name: str, filename: str):
    """Loads a csv file from disk into in-memory storage.

    Note: this does not perform any checking on the inputted column formats or
    names. Please do so yourself!
    """
    if table_name not in system_information:
        raise AssertionError(f"Table '{table_name}' not created yet.")
    system_information[table_name].load_csv(filename)

def print_summary():
    print(f"ColumnChunk.MAX_CHUNK_SIZE: {ColumnChunk.MAX_CHUNK_SIZE}.")
    for table in system_information:
        print(f"{table}: {len(system_information[table].chunks)} chunks.")
        print(f"\tcols: {system_information[table].column_schema}.")


def handle_select(table_name: str, column_names: list[str], predicate: list[str]):
    """Processes a select query, returning qualified values .

    Args:
        table_name (str): The name of the table previously made with CREATE TABLE,
        column_names (list[str]): A list of columns to be returned from this query,
        predicate: (list[str]): A list of tokens to be interpreted for this query.
    """

    import time

    start = time.time()
    table = system_information[table_name]
    predicate_key = " ".join(predicate)
    matcher = query_lookup.simple_query_lookup[predicate_key]
    args = matcher["args"]
    func = matcher["func"]
    # TODO: make this np.ndarray
    # In short, this IDs tuples by storing the ID of the chunk alongside the ids of the chunks that match.
    qualified_ids: list[tuple[int, object]] = []
    for idx, chunk in enumerate(table.chunks):
        chunk_matches = chunk.filter(func, args)
        if chunk_matches:
            qualified_ids.append((idx, chunk_matches))

    id_qualify = time.time()

    res = pd.DataFrame(columns=column_names)
    num_found = 0
    for chunk in qualified_ids:
        for idx, match in table.chunks[chunk[0]].get_tuple(chunk[1], column_names):
            res.loc[idx] = match

    data_store = time.time()

    t = QueryTime(id_qualify - start, data_store - id_qualify)

    query_times.append(t)

    return res