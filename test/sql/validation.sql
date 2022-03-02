LOAD 'pg_diffix';
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
SELECT cast(id AS text) FROM test_customers GROUP BY 1;
SELECT cast(id AS varchar) FROM test_customers GROUP BY 1;
SELECT substring(cast(id AS text), 1, 1) FROM test_customers GROUP BY 1;
SELECT substring(cast(id AS varchar), 1, 1) FROM test_customers GROUP BY 1;

----------------------------------------------------------------
-- Untrusted mode query restrictions
----------------------------------------------------------------

SET pg_diffix.session_access_level = 'publish_untrusted';
SELECT diffix.access_level();

-- Get accepted
SELECT substring(city, 1, 2) from empty_test_customers;
SELECT floor(discount) from empty_test_customers;
SELECT ceil(discount) from empty_test_customers;
SELECT round(discount) from empty_test_customers;
SELECT discount from empty_test_customers;
SELECT diffix.floor_by(discount, 2) from empty_test_customers;
SELECT diffix.round_by(discount, 2) from empty_test_customers;
SELECT diffix.ceil_by(discount, 2) from empty_test_customers;
SELECT diffix.floor_by(discount, 20) from empty_test_customers;
SELECT diffix.floor_by(discount, 2.0) from empty_test_customers;
SELECT diffix.floor_by(discount, 0.2) from empty_test_customers;
SELECT diffix.floor_by(discount, 20.0) from empty_test_customers;
SELECT diffix.floor_by(discount, 50.0) from empty_test_customers;

-- Get rejected because of invalid generalization parameters
SELECT substring(city, 2, 2) from empty_test_customers;
SELECT diffix.floor_by(discount, 3) from empty_test_customers;
SELECT diffix.floor_by(discount, 3.0) from empty_test_customers;
SELECT diffix.floor_by(discount, 5000000000.1) from empty_test_customers;

-- Get rejected because of invalid generalizing functions
SELECT width_bucket(discount, 2, 200, 5) from empty_test_customers;
