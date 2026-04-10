CREATE TABLE foo (col1 INTEGER);
COPY foo FROM test/tmp.csv;
SELECT col1 FROM foo WHERE col1 > 100;
CREATE INDEX idx ON foo(col1) USING zone_map;
