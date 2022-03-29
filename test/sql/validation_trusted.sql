LOAD 'pg_diffix';

SET ROLE diffix_test;
SET pg_diffix.session_access_level = 'publish_trusted';

----------------------------------------------------------------
-- Sanity checks
----------------------------------------------------------------

SELECT diffix.access_level();

----------------------------------------------------------------
-- Supported queries
----------------------------------------------------------------

-- Supported functions for defining buckets
SELECT COUNT(*) FROM empty_test_customers
  GROUP BY substring(city, 1, 1);

SELECT COUNT(*) FROM empty_test_customers
  GROUP BY width_bucket(id, 0, 1000, 10), width_bucket(id::float, 0.0, 1000.0, 10);

SELECT COUNT(*) FROM empty_test_customers
  GROUP BY round(id::float, -1), round(id::numeric, -1);

SELECT COUNT(*) FROM empty_test_customers
  GROUP BY round(id::float), ceil(id::float), ceiling(id::float), floor(id::float);

SELECT COUNT(*) FROM empty_test_customers
  GROUP BY round(id::numeric), ceil(id::numeric), ceiling(id::numeric), floor(id::numeric);

SELECT
  diffix.round_by(id::numeric, 5),
  diffix.round_by(id::double precision, 5),
  COUNT(*)
FROM empty_test_customers
GROUP BY 1, 2;

SELECT
  diffix.ceil_by(id::numeric, 5),
  diffix.ceil_by(id::double precision, 5),
  COUNT(*)
FROM empty_test_customers
GROUP BY 1, 2;

SELECT
  diffix.floor_by(id::numeric, 5),
  diffix.floor_by(id::double precision, 5),
  COUNT(*)
FROM empty_test_customers
GROUP BY 1, 2;

SELECT
  substring(cast(last_seen AS text), 1, 3), 
  substring(cast(birthday AS text), 2, 3),
  substring(cast(lunchtime AS varchar), 1, 4)
FROM empty_test_times
GROUP BY 1, 2, 3;

-- Allow all functions post-anonymization
SELECT 2 * length(city) FROM empty_test_customers GROUP BY city;

----------------------------------------------------------------
-- Unsupported queries
----------------------------------------------------------------

-- Get rejected because non SELECT queries are unsupported.
INSERT INTO empty_test_customers VALUES (NULL, NULL,NULL);

-- Get rejected because WITH is unsupported.
WITH c AS (SELECT 1 FROM empty_test_customers) SELECT 1 FROM empty_test_customers;

-- Get rejected because GROUPING SETS are unsupported.
SELECT city FROM empty_test_customers GROUP BY GROUPING SETS ((city), ());
SELECT city FROM empty_test_customers GROUP BY CUBE ((city));

-- Get rejected because UNIONs etc. are unsupported.
SELECT city FROM empty_test_customers EXCEPT SELECT city FROM empty_test_customers;

-- Get rejected because SRF functions are unsupported.
SELECT generate_series(1,4) FROM empty_test_customers;

-- Get rejected because sublinks are unsupported.
SELECT city, (SELECT 1 FROM empty_test_customers) FROM empty_test_customers GROUP BY 1;
SELECT EXISTS (SELECT * FROM empty_test_customers WHERE discount < 10);
SELECT 1 WHERE EXISTS (SELECT * FROM empty_test_customers);

-- Get rejected because DISTINCT is unsupported.
SELECT DISTINCT city FROM empty_test_customers;

-- Get rejected because window functions are unsupported.
SELECT avg(discount) OVER (PARTITION BY city) FROM empty_test_customers;

-- Get rejected because aggregators are unsupported.
SELECT SUM(id) FROM empty_test_customers;
SELECT MIN(id) + MAX(id) FROM empty_test_customers;
SELECT city FROM empty_test_customers GROUP BY 1 ORDER BY AVG(LENGTH(city));
SELECT count(city ORDER BY city) FROM empty_test_customers;
SELECT count(*) FILTER (WHERE true) FROM empty_test_customers;

-- Get rejected because only a subset of expressions is supported for defining buckets.
SELECT COUNT(*) FROM empty_test_customers GROUP BY LENGTH(city);
SELECT COUNT(*) FROM empty_test_customers GROUP BY city || 'xxx';
SELECT LENGTH(city) FROM empty_test_customers;
SELECT city, 'aaaa' FROM empty_test_customers GROUP BY 1, 2;
SELECT COUNT(*) FROM empty_test_customers GROUP BY round(floor(id));
SELECT COUNT(*) FROM empty_test_customers GROUP BY floor(cast(discount AS integer));
SELECT COUNT(*) FROM empty_test_customers GROUP BY substr(city, 1, id);
SELECT COUNT(*) FROM empty_test_customers GROUP BY substr('aaaa', 1, 2);

-- Get rejected because expression node type is unsupported.
SELECT COALESCE(discount, 20) FROM empty_test_customers;
SELECT NULLIF(discount, 20) FROM empty_test_customers;
SELECT GREATEST(discount, 20) FROM empty_test_customers;
SELECT LEAST(discount, 20) FROM empty_test_customers;

-- Get rejected because of subqueries
SELECT COUNT(*), COUNT(x.city), COUNT(DISTINCT x.id)
FROM (
  SELECT * FROM empty_test_customers
) x;

SELECT COUNT(*), COUNT(x.city), COUNT(DISTINCT x.user_id)
FROM (
  SELECT y.city as city, y.id as user_id
  FROM ( SELECT * FROM empty_test_customers ) y
) x;

SELECT x.user_city, COUNT(*), COUNT(DISTINCT x.id), COUNT(DISTINCT x.cid)
FROM (
  SELECT id, cid, city as user_city
  FROM empty_test_customers
  INNER JOIN test_purchases tp ON id = cid
) x
GROUP BY 1;

SELECT COUNT(DISTINCT x.modified_id) FROM ( SELECT id AS modified_id FROM empty_test_customers ) x;

-- Get rejected because of subqueries, but used to be rejected because of their inner aggregation
SELECT * FROM ( SELECT COUNT(*) FROM empty_test_customers ) x;

SELECT COUNT(city)
FROM (
  SELECT city FROM empty_test_customers
  GROUP BY 1
) x;

-- Get rejected because of JOINs
SELECT COUNT(*), COUNT(DISTINCT id), COUNT(DISTINCT cid) FROM empty_test_customers
  INNER JOIN test_purchases tp ON id = cid;

SELECT COUNT(c.city), COUNT(p.name) FROM empty_test_customers c
  LEFT JOIN test_purchases ON c.id = cid
  LEFT JOIN test_products p ON pid = p.id;

SELECT city, COUNT(price) FROM empty_test_customers, test_products GROUP BY 1;

SELECT city, COUNT(price) FROM test_products, empty_test_customers GROUP BY 1;

SELECT city, COUNT(price) FROM test_products CROSS JOIN empty_test_customers GROUP BY 1;

-- Get rejected because of WHERE
SELECT COUNT(*) FROM empty_test_customers WHERE city = 'London';

-- Get rejected because of non-datetime cast to text
SELECT cast(id AS text) FROM empty_test_customers GROUP BY 1;
SELECT cast(id AS varchar) FROM empty_test_customers GROUP BY 1;
SELECT substring(cast(id AS text), 1, 1) FROM empty_test_customers GROUP BY 1;
SELECT substring(cast(id AS varchar), 1, 1) FROM empty_test_customers GROUP BY 1;

-- Get rejected because of disallowed utility statement
COPY test_customers TO STDOUT;
ALTER TABLE empty_test_customers DROP COLUMN id;

-- Get rejected because of accessing pg_catalog tables with sensitive stats
SELECT * FROM pg_stats;
SELECT * FROM pg_statistic;
SELECT * FROM pg_stat_user_functions;
SELECT * FROM pg_stat_user_indexes;
SELECT * FROM pg_class;

-- Get rejected because of inheritance
SELECT x, y FROM subclass;
SELECT x FROM superclass;

-- Get rejected because attempt to use system columns
SELECT ctid FROM empty_test_customers;
SELECT tableoid FROM empty_test_customers;

-- EXPLAIN is censored
EXPLAIN SELECT city FROM test_customers LIMIT 4;
EXPLAIN (COSTS false) SELECT city FROM test_customers LIMIT 4;

-- EXPLAIN is blocked
EXPLAIN ANALYZE SELECT city FROM test_customers LIMIT 4;
EXPLAIN (ANALYZE true) SELECT city FROM test_customers LIMIT 4;
EXPLAIN (COSTS) SELECT city FROM test_customers LIMIT 4;
EXPLAIN (COSTS true) SELECT city FROM test_customers LIMIT 4;
EXPLAIN (VERBOSE) SELECT city FROM test_customers LIMIT 4;
EXPLAIN (VERBOSE true) SELECT city FROM test_customers LIMIT 4;

-- EXPLAIN is left intact for non-anonymizing queries
EXPLAIN SELECT name FROM test_products LIMIT 4;
EXPLAIN (ANALYZE, SUMMARY false, TIMING false, COSTS true) SELECT name FROM test_products LIMIT 4;
