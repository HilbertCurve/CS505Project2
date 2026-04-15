# CS505: Intermediate Topics in Databases Project 2

A simple in-memory analytical database with a stripped-down SQL interpreter.

## How to use:

Simply run the query executor as follows:
```shell
python test/testHardCodedQueries.py
```
Make sure your working directory is in the same directory as this README file.

## A tentative TODO list:
- Produce 4 test databases and 6 queries on each of them:
  - (or is it just six queries applied to all 4 test databases?)
  - [x] DB 1 with queries
  - [x] DB 2 with queries
  - [x] DB 3 with queries
  - [x] DB 4 with queries
- [x] Loader
- [x] Baseline Scan
- [x] Query Executor
- [x] Metrics Layer
- Basic techniques:
  - [x] Zone maps - almost there!
  - [x] Bitmap index
  - [x] RLE Compression - almost there!
  - [x] Dictionary encoding
  - [x] Delta + bit packing
  - [x] Experimental comparison
- Advanced Techniques (select 3):
  - [ ] Bit-slicing
  - [ ] BitWeaving-lite
  - [x] Column imprints
  - [x] Column sketches
  - [ ] Bitmap encoding
  - [x] Mostly encoding
