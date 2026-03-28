"""Classes and routines for loading data from storage into the in-memory database.

The data is stored in a columnar format and only supports integer and string
types. The following will NOT be supported:
- Updates or post-load inserts
- Joins
- Off-disk storage

Here is the design specification for the database we are creating:

A column is a contiguous storage of a certain type. It is equivalent to an
immutable list of a certain type.

A table is a set of columns whose length are all equivalent.

A database is a set of tables.

"""

from abc import ABC

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
        self.name = table_name
        self.column_schema = column_schema
        self.columns = {}
        self.length = 0

    def load_csv(self, path: str):
        with open(path) as f:
            headers = f.readline().strip("\n").split(",")

def create_table(table_name: str, columns: dict):
    """Create a table with preset columns.

    Args:
        table_name (str): The SQL-viable name of the table,
        columns (dict): A mapping of column names to the type of the column,
            which itself is a map, mapping "INTEGER" to None and "VARCHAR" to the
            column's length in characters.
    """
    res = Table(table_name, columns)

def load_csv(table_name: str, filename: str):
    pass
