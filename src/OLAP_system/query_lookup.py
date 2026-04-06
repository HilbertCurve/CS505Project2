"""File used for generating query parsers.

Note: for the sake of timeliness, testing things will be done by matching queries with a lookup table. Proper SQL query
parsing will be implemented later.
"""

simple_query_lookup = {
    "col1 > 100": {
        "args": "col1",
        "func": lambda _table, col1 : col1 > 100,
    },
    "name = 'john'": {
        "args": "name",
        "func": lambda _table, name : name == "john",
    }
}
