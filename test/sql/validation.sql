LOAD 'pg_diffix';

CREATE TABLE test_validation (
  id INTEGER PRIMARY KEY,
  name TEXT,
  city TEXT,
  discount REAL,
  birthday DATE,
  lunchtime TIME,
  last_seen TIMESTAMP
);

SECURITY LABEL FOR pg_diffix ON TABLE test_validation IS 'sensitive';
SECURITY LABEL FOR pg_diffix ON COLUMN test_validation.id IS 'aid';

CREATE TABLE superclass (x INTEGER);
CREATE TABLE subclass (x INTEGER, y INTEGER);
INSERT INTO subclass VALUES (1, 2);

SECURITY LABEL FOR pg_diffix ON TABLE superclass IS 'sensitive';
SECURITY LABEL FOR pg_diffix ON COLUMN superclass.x IS 'aid';
SECURITY LABEL FOR pg_diffix ON TABLE subclass IS 'sensitive';
SECURITY LABEL FOR pg_diffix ON COLUMN subclass.y IS 'aid';

ALTER TABLE subclass INHERIT superclass;

-- No-op. Repeated to test the error on conflicting configuration
SECURITY LABEL FOR pg_diffix ON TABLE superclass IS 'sensitive';
SECURITY LABEL FOR pg_diffix ON TABLE subclass IS 'sensitive';

SET ROLE diffix_test;

----------------------------------------------------------------
-- Trusted mode query restrictions
----------------------------------------------------------------

SET pg_diffix.session_access_level = 'publish_trusted';
SELECT diffix.access_level();

----------------------------------------------------------------
-- Supported queries
----------------------------------------------------------------

-- Supported functions for defining buckets
SELECT COUNT(*) FROM test_validation
  GROUP BY substring(city, 1, 1);

SELECT COUNT(*) FROM test_validation
  GROUP BY width_bucket(id, 0, 1000, 10), width_bucket(id::float, 0.0, 1000.0, 10);

SELECT COUNT(*) FROM test_validation
  GROUP BY round(id::float, -1), round(id::numeric, -1);

SELECT COUNT(*) FROM test_validation
  GROUP BY round(id::float), ceil(id::float), ceiling(id::float), floor(id::float);

SELECT COUNT(*) FROM test_validation
  GROUP BY round(id::numeric), ceil(id::numeric), ceiling(id::numeric), floor(id::numeric);

SELECT
  diffix.round_by(id::numeric, 5),
  diffix.round_by(id::double precision, 5),
  COUNT(*)
FROM test_validation
GROUP BY 1, 2;

SELECT
  diffix.ceil_by(id::numeric, 5),
  diffix.ceil_by(id::double precision, 5),
  COUNT(*)
FROM test_validation
GROUP BY 1, 2;

SELECT
  diffix.floor_by(id::numeric, 5),
  diffix.floor_by(id::double precision, 5),
  COUNT(*)
FROM test_validation
GROUP BY 1, 2;

SELECT
  substring(cast(last_seen AS text), 1, 3),
  substring(cast(birthday AS text), 2, 3),
  substring(cast(lunchtime AS varchar), 1, 4)
FROM test_validation
GROUP BY 1, 2, 3;

-- Allow all functions post-anonymization.
SELECT 2 * length(city) FROM test_validation GROUP BY city;

-- Allow diffix.is_suppress_bin in non-direct access level.
SELECT city, count(*), diffix.is_suppress_bin(*) from test_validation GROUP BY 1;

-- Set operations between anonymizing queries.
SELECT city FROM test_validation EXCEPT SELECT city FROM test_validation;
SELECT city FROM test_validation UNION SELECT city FROM test_validation;

-- Anonymizing sublinks are supported.
SELECT EXISTS (SELECT city FROM test_validation);
SELECT 1 WHERE EXISTS (SELECT city FROM test_validation);

-- Anonymizing leaf subqueries are supported.
SELECT * FROM ( SELECT COUNT(*) FROM test_validation ) x;

SELECT COUNT(city)
FROM (
  SELECT city FROM test_validation
  GROUP BY 1
) x;

SELECT COUNT(*), COUNT(x.city), COUNT(DISTINCT x.id)
FROM (
  SELECT * FROM test_validation
) x;

SELECT COUNT(DISTINCT x.modified_id) FROM ( SELECT id AS modified_id FROM test_validation ) x;

SELECT COUNT(*), COUNT(x.city), COUNT(DISTINCT x.user_id)
FROM (
  SELECT y.city as city, y.id as user_id
  FROM ( SELECT * FROM test_validation ) y
) x;

WITH c AS (SELECT city FROM test_validation) SELECT * FROM c;

SELECT (SELECT city FROM test_validation);

----------------------------------------------------------------
-- Unsupported queries
----------------------------------------------------------------

-- Get rejected because non SELECT queries are unsupported.
INSERT INTO test_validation VALUES (NULL, NULL,NULL);

-- Get rejected because WITH is unsupported.
WITH c AS (SELECT 1 FROM test_validation) SELECT 1 FROM test_validation;

-- Get rejected because GROUPING SETS are unsupported.
SELECT city FROM test_validation GROUP BY GROUPING SETS ((city), ());
SELECT city FROM test_validation GROUP BY CUBE ((city));

-- Get rejected because SRF functions are unsupported.
SELECT generate_series(1,4) FROM test_validation;

-- Get rejected because sublinks are unsupported.
SELECT city, (SELECT 1 FROM test_validation) FROM test_validation GROUP BY 1;

-- Get rejected because DISTINCT is unsupported.
SELECT DISTINCT city FROM test_validation;

-- Get rejected because window functions are unsupported.
SELECT avg(discount) OVER (PARTITION BY city) FROM test_validation;

-- Get rejected because aggregators are unsupported.
SELECT SUM(id) FROM test_validation;
SELECT MIN(id) + MAX(id) FROM test_validation;
SELECT city FROM test_validation GROUP BY 1 ORDER BY AVG(LENGTH(city));
SELECT count(city ORDER BY city) FROM test_validation;
SELECT count(*) FILTER (WHERE true) FROM test_validation;
SELECT count(distinct id + 5) FROM test_validation;
SELECT count(distinct least(id, 5)) FROM test_validation;
SELECT count(id + 5) FROM test_validation;
SELECT count(least(id, 5)) FROM test_validation;

-- Get rejected because only a subset of expressions is supported for defining buckets.
SELECT COUNT(*) FROM test_validation GROUP BY LENGTH(city);
SELECT COUNT(*) FROM test_validation GROUP BY city || 'xxx';
SELECT LENGTH(city) FROM test_validation;
SELECT city, 'aaaa' FROM test_validation GROUP BY 1, 2;
SELECT COUNT(*) FROM test_validation GROUP BY round(floor(id));
SELECT COUNT(*) FROM test_validation GROUP BY floor(cast(discount AS integer));
SELECT COUNT(*) FROM test_validation GROUP BY substr(city, 1, id);
SELECT COUNT(*) FROM test_validation GROUP BY substr('aaaa', 1, 2);

-- Get rejected because expression node type is unsupported.
SELECT COALESCE(discount, 20) FROM test_validation;
SELECT NULLIF(discount, 20) FROM test_validation;
SELECT GREATEST(discount, 20) FROM test_validation;
SELECT LEAST(discount, 20) FROM test_validation;

-- Get rejected because of JOINs
SELECT COUNT(*), COUNT(DISTINCT id), COUNT(DISTINCT cid) FROM test_validation
  INNER JOIN test_purchases tp ON id = cid;

SELECT COUNT(c.city), COUNT(p.name) FROM test_validation c
  LEFT JOIN test_purchases ON c.id = cid
  LEFT JOIN test_products p ON pid = p.id;

SELECT city, COUNT(price) FROM test_validation, test_products GROUP BY 1;

SELECT city, COUNT(price) FROM test_products, test_validation GROUP BY 1;

SELECT city, COUNT(price) FROM test_products CROSS JOIN test_validation GROUP BY 1;

-- Get rejected because of WHERE
SELECT COUNT(*) FROM test_validation WHERE city = 'London';

-- Get rejected because of non-datetime cast to text
SELECT cast(id AS text) FROM test_validation GROUP BY 1;
SELECT cast(id AS varchar) FROM test_validation GROUP BY 1;
SELECT substring(cast(id AS text), 1, 1) FROM test_validation GROUP BY 1;
SELECT substring(cast(id AS varchar), 1, 1) FROM test_validation GROUP BY 1;

-- Invalid subqueries are rejected.
SELECT * FROM (SELECT length(city) FROM test_validation) x;
SELECT EXISTS (SELECT length(city) FROM test_validation);
SELECT 1 WHERE EXISTS (SELECT length(city) FROM test_validation);
SELECT 1 UNION SELECT length(city) FROM test_validation;
SELECT * FROM (SELECT 1) t1, (SELECT length(city) FROM test_validation) t2;
WITH c AS (SELECT length(city) FROM test_validation) SELECT * FROM c;
SELECT (SELECT length(city) FROM test_validation);

-- Get rejected because of disallowed utility statement
COPY test_customers TO STDOUT;
ALTER TABLE test_validation DROP COLUMN id;

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
SELECT ctid FROM test_validation;
SELECT tableoid FROM test_validation;
SELECT count(ctid) FROM test_validation;
SELECT count(tableoid) FROM test_validation;
SELECT count(distinct ctid) FROM test_validation;
SELECT count(distinct tableoid) FROM test_validation;

-- EXPLAIN is censored
EXPLAIN SELECT city FROM test_customers LIMIT 4;
EXPLAIN (COSTS false) SELECT city FROM test_customers LIMIT 4;

-- EXPLAIN is blocked
EXPLAIN ANALYZE SELECT city FROM test_customers LIMIT 4;
EXPLAIN (COSTS) SELECT city FROM test_customers LIMIT 4;
EXPLAIN (VERBOSE) SELECT city FROM test_customers LIMIT 4;

-- EXPLAIN is left intact for non-anonymizing queries
EXPLAIN SELECT name FROM test_products LIMIT 4;
EXPLAIN (ANALYZE, SUMMARY false, TIMING false, COSTS true) SELECT name FROM test_products LIMIT 4;


----------------------------------------------------------------
-- Untrusted mode query restrictions
----------------------------------------------------------------

SET pg_diffix.session_access_level = 'publish_untrusted';
SELECT diffix.access_level();

-- Get accepted
SELECT substring(city, 1, 2) from test_validation;
SELECT floor(discount) from test_validation;
SELECT ceil(discount) from test_validation;
SELECT round(discount) from test_validation;
SELECT discount from test_validation;
SELECT diffix.floor_by(discount, 2) from test_validation;
SELECT diffix.round_by(discount, 2) from test_validation;
SELECT diffix.ceil_by(discount, 2) from test_validation;
SELECT diffix.floor_by(discount, 20) from test_validation;
SELECT diffix.floor_by(discount, 2.0) from test_validation;
SELECT diffix.floor_by(discount, 0.2) from test_validation;
SELECT diffix.floor_by(discount, 20.0) from test_validation;
SELECT diffix.floor_by(discount, 50.0) from test_validation;

-- Get rejected because of invalid generalization parameters
SELECT substring(city, 2, 2) from test_validation;
SELECT diffix.floor_by(discount, 3) from test_validation;
SELECT diffix.floor_by(discount, 3.0) from test_validation;
SELECT diffix.floor_by(discount, 5000000000.1) from test_validation;

-- Get rejected because of invalid generalizing functions
SELECT width_bucket(discount, 2, 200, 5) from test_validation;
