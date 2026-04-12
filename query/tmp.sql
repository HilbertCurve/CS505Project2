CREATE TABLE foo (col1 INTEGER, name VARCHAR(20));
COPY foo FROM test/tmp.csv;
CREATE INDEX idx ON foo(col1) USING rle;
SELECT col1 FROM foo WHERE col1 > 100;
SELECT name FROM foo WHERE col1 > 100;
