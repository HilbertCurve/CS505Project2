"""Utilities for timing query lookups, indexing, etc.
"""

query_times = []

class QueryTime:
    """Class for storing decomposed time data.

    Args:
        id_qualify (float): the time it takes to load qualifying tuples,
        data_store (float): the time it takes to store and return data.
    """
    def __init__(self, id_qualify: float, data_store: float):
        self.id_qualify = id_qualify
        self.data_store = data_store
