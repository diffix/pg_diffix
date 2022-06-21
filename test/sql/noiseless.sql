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

SELECT COUNT(*) FROM test_customers WHERE planet = 'Earth';

----------------------------------------------------------------
-- Basic queries - sum
----------------------------------------------------------------

SELECT SUM(id), diffix.sum_noise(id) FROM test_customers;
SELECT SUM(discount), diffix.sum_noise(discount) FROM test_customers;
SELECT city, SUM(id), diffix.sum_noise(id) FROM test_customers GROUP BY 1;
SELECT city, SUM(discount), diffix.sum_noise(discount) FROM test_customers GROUP BY 1;

-- sum supports numeric type
SELECT city, SUM(discount::numeric), pg_typeof(SUM(discount::numeric)), diffix.sum_noise(discount::numeric)
FROM test_customers
GROUP BY 1;

----------------------------------------------------------------
-- Basic queries - avg
----------------------------------------------------------------

SELECT city, AVG(discount), diffix.avg_noise(discount) FROM test_customers GROUP BY 1
EXCEPT
SELECT city, SUM(discount) / COUNT(discount), diffix.sum_noise(discount) / COUNT(discount) FROM test_customers GROUP BY 1;

SELECT city, AVG(id), diffix.avg_noise(id) FROM test_customers GROUP BY 1
EXCEPT
SELECT city, SUM(id)::float8 / COUNT(id), diffix.sum_noise(id) / COUNT(id) FROM test_customers GROUP BY 1;

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

SELECT id FROM test_customers;

SELECT city FROM test_customers;

SELECT city FROM test_customers GROUP BY 1 HAVING length(city) <> 4;

SELECT COUNT(*), COUNT(city), COUNT(DISTINCT city) FROM test_customers WHERE city = 'London';

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
