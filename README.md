# CS505: Intermediate Topics in Databases Project 2

A simple in-memory analytical database with a stripped-down SQL interpreter.

## How to use:

This is primarily a library-based application: we're taking care to write plenty of docs in the source code, so take a
gander for how it all works (or just email us through our emails on GitHub.)

The following commands are supported:

- `CREATE TABLE`: Note that constraints are not supported, and the only data types we currently support are INTEGER and
  VARCHAR(n).
- `COPY FROM`: Loads into a table from a CSV file. Example usage could be:
```sql
COPY table_name FROM /path/to/table.csv;
```
- `SELECT FROM`: This is currently a TODO!
- `\print_all`: Print metadata about current in-memory database.
- `\exit`: Quit the program.

## How to test:

There is currently only a single test case for the SQL parser and a small test query for loading data into memory.
Expanding upon this is necessary for this assignment :D

To execute an SQL query, run the following command in a linux terminal or a git Bash terminal:
```shell
cat /path/to/test - | python src/repl.py
```

## A tentative TODO list:
- Produce 4 test databases and 6 queries on each of them:
  - (or is it just six queries applied to all 4 test databases?)
  - [ ] DB 1 with queries
  - [ ] DB 2 with queries
  - [ ] DB 3 with queries
  - [ ] DB 4 with queries
- [x] Loader
- [ ] Baseline Scan
- [ ] Query Executor
- [ ] Metrics Layer
- Basic techniques:
  - [ ] Zone maps
  - [ ] Bitmap index
  - [ ] RLE Compression
  - [ ] Dictionary encoding
  - [ ] Delta + bit packing
  - [ ] Experimental comparison
- Advanced Techniques (select 3):
  - [ ] Bit-slicing
  - [ ] BitWeaving-lite
  - [ ] Column imprints
  - [ ] Column sketches
  - [ ] Bitmap encoding
  - [ ] Mostly encoding
