"""Tools for converting strings into SQL tokens.

The whole point of this file is to take SQL code as input and generate a token
sequence as output.

The only syntax we wish to support are:
- create_table => CREATE TABLE ( column[, column...] );
  - column => column_name datatype
    - column_name => identifier
    - datatype => type (int or varchar(n))
- select_from => SELECT data FROM table [predicate...];
  - data => column_name[, column_name...]
  - data => "*"
  - table => identifier
  - predicate => WHERE expr (evaluates to bool)
- load_from => LOAD file_name INTO table;
  - file_name => expr (string)
"""
import re

import OLAP_system.database as database

def tokenize(sql: str) -> list[str]:
    # Source: https://swanhart.livejournal.com/130191.html?
    matcher = re.compile(r"""[A-Za-z_.0-9/]+|\(|\)|[<=>]+|"(?:[^"]|\"|"")*"+|'[^'](?:|\'|'')*'+|-|=|;|,""")
    return matcher.findall(sql)

def parse(sql: str):
    """Convert strings to tokens and hand off to next parser.

    This function exclusively parses the tokens, deduces its function, then hands
    off control to the next subsystem in the process. For example, a CREATE
    TABLE instruction passes through this function, is turned into tokens, then
    handed to the table creation parser in `database.py`.
    """
    tokens = tokenize(sql)
    if tokens is None:
        raise ValueError("Error while tokenizing sql: regex couldn't match anything!")
    elif tokens[0] == "CREATE" and tokens[1] == "TABLE":
        table_name = tokens[2]
        columns = {}
        assert tokens[3] == "("

        mark = 4
        while tokens[mark] != ")":
            column_name = tokens[mark]
            column_type = tokens[mark + 1].upper()
            column_length = None
            mark += 2

            if column_type == "VARCHAR":
                assert tokens[mark] == "("
                column_length = int(tokens[mark + 1])
                assert tokens[mark + 2] == ")"
                mark += 3
            elif column_type != "INTEGER":
                raise AssertionError(f"INTEGER or VARCHAR(n) expected, found {column_type}.")

            columns[column_name] = (column_type, column_length)

        assert tokens[mark + 1] == ";"
        database.create_table(table_name, columns)
        print("CREATE TABLE")
    elif tokens[0] == "SELECT":
        column_names = []
        mark = 1
        while tokens[mark] != "FROM":
            if tokens[mark] == ",":
                mark += 1
            else:
                column_names.append(tokens[mark])
                mark += 1
        mark += 1
        table_name = tokens[mark]
        mark += 1

        predicate = []
        if tokens[mark] != ";":
            assert tokens[mark] == "WHERE"
            mark += 1
            assert tokens[mark] != ";"
            while tokens[mark] != ";":
                predicate.append(tokens[mark])
                mark += 1

        print(database.handle_select(table_name, column_names, predicate))
    elif tokens[0] == "COPY":
        table = tokens[1]
        assert tokens[2] == "FROM"
        filename = tokens[3]
        database.load_csv(table, filename)
        print("LOAD CSV")
    elif tokens[0] == "CREATE" and tokens[1] == "INDEX":
        index_name = tokens[2]
        assert tokens[3] == "ON"
        table_name = tokens[4]
        assert tokens[5] == "("
        column_name = tokens[6]
        assert tokens[7] == ")"
        assert tokens[8] == "USING"
        indexer = tokens[9]
        assert tokens[10] == ";"

        database.handle_index(table_name, column_name, indexer)
        print("CREATE INDEX " + index_name)
    else:
        raise AssertionError(f"Invalid operation string: found {tokens[0]}.")