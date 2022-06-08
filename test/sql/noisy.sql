LOAD 'pg_diffix';

SET pg_diffix.noise_layer_sd = 7;
SET pg_diffix.low_count_layer_sd = 2;
SET pg_diffix.low_count_min_threshold = 2;

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

----------------------------------------------------------------
-- Reporting noise
----------------------------------------------------------------

SELECT diffix.count_noise(*), diffix.count_noise(city), diffix.count_noise(DISTINCT city) FROM test_customers;

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

-- LCF doesn't depend on the bucket seed, both queries should have same noisy threshold.
SELECT diffix.floor_by(age, 30), COUNT(*) FROM test_patients GROUP BY 1;
SELECT diffix.floor_by(age, 30), diffix.floor_by(age, 106), COUNT(*) FROM test_patients GROUP BY 1, 2;

----------------------------------------------------------------
-- Empty tables
----------------------------------------------------------------

SELECT COUNT(*), COUNT(city), COUNT(DISTINCT city) FROM empty_test_customers;

----------------------------------------------------------------
-- WHERE clauses
----------------------------------------------------------------

-- Filtering and grouping produce identical results
(SELECT count(*) FROM test_customers GROUP BY city HAVING city = 'Berlin')
UNION
(SELECT count(*) FROM test_customers WHERE city = 'Berlin');

(SELECT count(*) FROM test_customers GROUP BY city, diffix.round_by(discount, 2)
  HAVING city = 'Berlin' AND diffix.round_by(discount, 2) = 0)
UNION
(SELECT count(*) FROM test_customers WHERE city = 'Berlin' AND diffix.round_by(discount, 2) = 0);

(SELECT count(*) FROM test_customers WHERE diffix.round_by(discount, 2) = 0 GROUP BY city HAVING city = 'Berlin')
UNION
(SELECT count(*) FROM test_customers WHERE city = 'Berlin' AND diffix.round_by(discount, 2) = 0);
