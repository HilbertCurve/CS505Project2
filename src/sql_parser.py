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

import database

def tokenize(sql: str) -> list[str]:
    # Source: https://swanhart.livejournal.com/130191.html?
    matcher = re.compile(r"""[A-Za-z_.0-9]+|\(|\)|"(?:[^"]|\"|"")*"+|'[^'](?:|\'|'')*'+|-|=|;|,""")
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
        raise ValueError("Error in tokenizing sql.")
    elif tokens[0] == "CREATE":
        assert tokens[1] == "TABLE"
        table_name = tokens[2]
        columns = {}
        assert tokens[3] == "("

        mark = 4
        while tokens[mark] != ")":
            column_name = tokens[mark]
            column_type = tokens[mark + 1]
            column_length = None
            mark += 2

            if column_type.to_lower() == "varchar":
                assert tokens[mark] == "("
                column_length = int(tokens[mark + 1])
                assert tokens[mark + 2] == ")"
                mark += 3

            columns[column_name] = {column_type: column_length}

        assert tokens[mark + 1] == ";"
        database.create_table(table_name, columns)
    elif tokens[0] == "SELECT":
        # TODO: parse SELECT statement!
        #database.handle_select(tokens)
        pass
    elif tokens[0] == "COPY":
        table = tokens[1]
        assert tokens[2] == "FROM"
        filename = tokens[3]
        database.load_csv(table, filename)