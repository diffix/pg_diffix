LOAD 'pg_diffix';

SET pg_diffix.strict = false;
SET pg_diffix.noise_layer_sd = 0;
SET pg_diffix.low_count_layer_sd = 0;
SET pg_diffix.outlier_count_min = 1;
SET pg_diffix.outlier_count_max = 1;
SET pg_diffix.top_count_min = 3;
SET pg_diffix.top_count_max = 3;

SET ROLE diffix_test;
SET pg_diffix.session_access_level = 'anonymized_trusted';

----------------------------------------------------------------
-- Sanity checks
----------------------------------------------------------------

SELECT diffix.access_level();

----------------------------------------------------------------
-- Basic queries
----------------------------------------------------------------

SELECT COUNT(*) FROM test_customers;
SELECT COUNT(*) FROM test_purchases;

SELECT COUNT(city), COUNT(DISTINCT city) FROM test_customers;

SELECT COUNT(DISTINCT cid) FROM test_purchases;

SELECT city, COUNT(DISTINCT id) FROM test_customers GROUP BY 1;

----------------------------------------------------------------
-- Basic queries - expanding constants in target expressions
----------------------------------------------------------------

SELECT 1 FROM test_patients;
SELECT cast(1 as real) FROM test_patients;
SELECT 1, COUNT(*) FROM test_patients;
SELECT 1, city FROM test_customers;
SELECT city, 'aaaa' FROM test_customers;

----------------------------------------------------------------
-- Multi-AID queries
----------------------------------------------------------------

SELECT city FROM test_patients GROUP BY 1;

SELECT COUNT(*), COUNT(city), COUNT(DISTINCT city) FROM test_patients;

----------------------------------------------------------------
-- LCF & Filtering
----------------------------------------------------------------

SELECT city FROM test_customers;

SELECT city FROM test_customers GROUP BY 1 HAVING length(city) <> 4;

SELECT COUNT(*), COUNT(city), COUNT(DISTINCT city) FROM london_customers;

----------------------------------------------------------------
-- Empty tables
----------------------------------------------------------------

SELECT COUNT(*), COUNT(city), COUNT(DISTINCT city) FROM empty_test_customers;

----------------------------------------------------------------
-- Minimal count
----------------------------------------------------------------

-- Zero for global aggregation
SELECT COUNT(DISTINCT planet) FROM test_customers;

-- `low_count_min_threshold` for queries with GROUP BY
SELECT city, COUNT(DISTINCT city) FROM test_customers GROUP BY 1;
SELECT discount, COUNT(DISTINCT id) FROM test_customers GROUP BY 1;
