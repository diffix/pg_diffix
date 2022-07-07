LOAD 'pg_diffix';

----------------------------------------------------------------
-- Setup
----------------------------------------------------------------

SET pg_diffix.strict = false;
SET pg_diffix.noise_layer_sd = 0;
SET pg_diffix.low_count_min_threshold = 2;
SET pg_diffix.low_count_layer_sd = 0;

CREATE TABLE count_histogram_test (id INTEGER, value TEXT);

PREPARE add_rows (integer, integer) AS
  INSERT INTO count_histogram_test (id, value)
  SELECT $1, 'value'
  FROM generate_series(1, $2);

EXECUTE add_rows(1, 1);
EXECUTE add_rows(2, 1);
EXECUTE add_rows(3, 1);
EXECUTE add_rows(4, 1);   -- 4 users contribute 1 row
EXECUTE add_rows(5, 2);
EXECUTE add_rows(6, 2);
EXECUTE add_rows(7, 2);   -- 3 users contribute 2 rows
EXECUTE add_rows(8, 6);   -- 1 user contributes 6 rows
EXECUTE add_rows(9, 7);   -- 1 user contributes 7 rows
EXECUTE add_rows(10, 13); -- 1 user contributes 13 rows

CALL diffix.mark_personal('count_histogram_test', 'id');

----------------------------------------------------------------
-- Non-anonymizing count_histogram
----------------------------------------------------------------

SELECT diffix.count_histogram(id) FROM count_histogram_test;
SELECT diffix.count_histogram(id, 5) FROM count_histogram_test;

----------------------------------------------------------------
-- Anonymizing count_histogram
----------------------------------------------------------------

SET ROLE diffix_test;
SET pg_diffix.session_access_level = 'anonymized_trusted';

SELECT diffix.count_histogram(id) FROM count_histogram_test;
SELECT diffix.count_histogram(id, 5) FROM count_histogram_test;

----------------------------------------------------------------
-- Misc
----------------------------------------------------------------

SELECT diffix.unnest_histogram(x.histogram)
FROM (
  SELECT diffix.count_histogram(id) AS histogram
  FROM count_histogram_test
) x;
