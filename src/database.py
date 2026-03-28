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
# A mapping between table names and their respective tables.
system_information = {}

class ColumnChunk:
    """Represents a fixed-size chunk of a column in a table.

    Attributes:
        MAX_CHUNK_SIZE (int): A constant for the max size of a chunk,
        column_chunk (list): A contiguous array of values,
        size (int): The number of values in the chunk.
    """
    MAX_CHUNK_SIZE = 1024
    def __init__(self, column_chunk: list):
        assert self.MAX_CHUNK_SIZE >= len(column_chunk) > 0 # what is this python syntax O_o
        self.column_chunk = column_chunk
        self.size = len(column_chunk)


class Table:
    """Class used for storing csv table data.

    Attributes:
        name (str): The table's identifier,
        column_schema (dict): The mapping from column names to the type of the column,
        columns (dict): A mapping of column names to the internal storage of that column,
        length (int): The number of tuples in the table.
    """
    def __len__(self):
        return self.length

    def __init__(self, table_name: str, column_schema: dict):
        self.name: str = table_name
        self.column_schema: dict = column_schema
        self.columns: dict[str, list[ColumnChunk]] = {}
        for h in self.column_schema:
            self.columns[h] = []

        self.length: int = 0

    def load_csv(self, path: str):
        with open(path) as f:
            # initialize empty column chunks
            headers = f.readline().strip().split(",")
            curr_chunks = {}
            curr_chunk_size = 0
            for h in headers:
                curr_chunks[h] = []

            # populate columns
            for line in f:
                cols = line.strip().split(",")
                if len(cols) != len(self.column_schema):
                    raise AssertionError(f"Expected CSV with {len(self.column_schema)} columns, got {len(cols)}!")

                # fill corresponding column chunks
                for idx, h in enumerate(headers):
                    curr_chunks[h].append(cols[idx])
                curr_chunk_size += 1
                self.length += 1

                # populate column chunks in database once max capacity is reached
                if curr_chunk_size == ColumnChunk.MAX_CHUNK_SIZE:
                    for h in headers:
                        self.columns[h].append(ColumnChunk(curr_chunks[h]))
                    curr_chunk_size = 0

            # insert remaining stub if partially-filled chunk
            if curr_chunk_size > 0:
                for h in headers:
                    self.columns[h].append(ColumnChunk(curr_chunks[h]))


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
        print(f"{table}: {len(system_information[table].columns)} chunks.")
        print(f"\tcols: {system_information[table].columns.keys()}.")
