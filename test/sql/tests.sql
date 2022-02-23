CREATE EXTENSION IF NOT EXISTS pg_diffix;
LOAD 'pg_diffix';

SET pg_diffix.session_access_level = 'publish_trusted';

-- Create test data.
CREATE TABLE test_customers (id INTEGER PRIMARY KEY, city TEXT, discount REAL);
INSERT INTO test_customers VALUES
  (0, NULL, NULL), (1, 'Berlin', 1.0), (2, 'Berlin', 1.0), (3, 'Rome', 0.5), (4, 'London', 2.0), (5, 'Berlin', 2.0),
  (6, 'Rome', 1.0), (7, 'Rome', 0.5), (8, 'Berlin', 0.0), (9, 'Rome', 1.0), (10, 'Berlin', 2.0), (11, 'Rome', 1.5),
  (12, 'Rome', 0.5), (13, 'Rome', 0.5), (14, 'Berlin', 1.5), (15, 'Berlin', 1.0);

CREATE TABLE test_products (id INTEGER PRIMARY KEY, name TEXT, price REAL);
INSERT INTO test_products VALUES (0, NULL, NULL), (1, 'Food', 1.5),
  (2, 'Car', 100.0), (3, 'House', 400.0), (4, 'Movie', 10.0);

CREATE TABLE test_purchases (cid INTEGER, pid INTEGER);
INSERT INTO test_purchases VALUES (0, 0), (0, 1), (0, 3), (1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1),
  (7, 1), (8, 2), (9, 1), (10, 2), (11, 1), (12, 1), (13, 2), (1, 2), (2, 2), (3, 2), (4, 2), (5, 2), (6, 2),
  (7, 1), (8, 1), (9, 2), (1, 2), (1, 2), (2, 1), (3, 0), (NULL, NULL), (4, 3), (5, 3), (6, 4), (7, -1),
  (7, 4), (8, -2), (9, -3), (10, -4);

CREATE TABLE test_patients (id INTEGER PRIMARY KEY, name TEXT, city TEXT);
INSERT INTO test_patients VALUES
  (0, NULL, 'Berlin'), (1, 'John', 'Berlin'), (2, 'Alice', 'Berlin'), (3, 'Bob', 'Berlin'), (4, 'Emma', 'Berlin'),
  (5, 'John', 'Berlin'), (6, 'Bob', 'Berlin'), (7, 'Alice', 'Rome'), (8, 'Dan', 'Rome'), (9, 'Anna', 'Rome'),
  (10, 'Mike', 'London'), (11, 'Mike', 'London'), (12, 'Mike', 'London'), (13, 'Mike', 'London');

CREATE TABLE empty_test_customers (id INTEGER PRIMARY KEY, name TEXT, city TEXT);

-- Pre-filtered table to maintain LCF tests which relied on WHERE clause.
CREATE TABLE london_customers (id INTEGER PRIMARY KEY, name TEXT, city TEXT);
INSERT INTO london_customers (SELECT * FROM test_customers WHERE city = 'London');

-- Config tables.
SECURITY LABEL FOR pg_diffix ON TABLE test_customers IS 'sensitive';
SECURITY LABEL FOR pg_diffix ON COLUMN test_customers.id IS 'aid';
SECURITY LABEL FOR pg_diffix ON TABLE test_purchases IS 'sensitive';
SECURITY LABEL FOR pg_diffix ON COLUMN test_purchases.cid IS 'aid';
SECURITY LABEL FOR pg_diffix ON TABLE test_patients IS 'sensitive';
SECURITY LABEL FOR pg_diffix ON COLUMN test_patients.id IS 'aid';
SECURITY LABEL FOR pg_diffix ON COLUMN test_patients.name IS 'aid';
SECURITY LABEL FOR pg_diffix ON TABLE empty_test_customers IS 'sensitive';
SECURITY LABEL FOR pg_diffix ON COLUMN empty_test_customers.id IS 'aid';
SECURITY LABEL FOR pg_diffix ON TABLE london_customers IS 'sensitive';
SECURITY LABEL FOR pg_diffix ON COLUMN london_customers.id IS 'aid';

----------------------------------------------------------------
-- Utilities
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

SELECT COUNT(*), COUNT(city), COUNT(DISTINCT city) FROM london_customers;

----------------------------------------------------------------
-- Empty tables
----------------------------------------------------------------

SELECT COUNT(*), COUNT(city), COUNT(DISTINCT city) FROM empty_test_customers;

----------------------------------------------------------------
-- Supported functions for defining buckets
----------------------------------------------------------------

SELECT COUNT(*) FROM test_customers
  GROUP BY substring(city, 1, 1);

SELECT COUNT(*) FROM test_customers
  GROUP BY width_bucket(id, 0, 1000, 10), width_bucket(id::float, 0.0, 1000.0, 10);

SELECT COUNT(*) FROM test_customers
  GROUP BY round(id::float, -1), round(id::numeric, -1);

SELECT COUNT(*) FROM test_customers
  GROUP BY round(id::float), ceil(id::float), ceiling(id::float), floor(id::float);

SELECT COUNT(*) FROM test_customers
  GROUP BY round(id::numeric), ceil(id::numeric), ceiling(id::numeric), floor(id::numeric);

SELECT 
  diffix.round_by(id::numeric, 5),
  diffix.round_by(id::double precision, 5),
  COUNT(*)
FROM test_customers
GROUP BY 1, 2;

SELECT 
  diffix.ceil_by(id::numeric, 5),
  diffix.ceil_by(id::double precision, 5),
  COUNT(*)
FROM test_customers
GROUP BY 1, 2;

SELECT 
  diffix.floor_by(id::numeric, 5),
  diffix.floor_by(id::double precision, 5),
  COUNT(*)
FROM test_customers
GROUP BY 1, 2;

----------------------------------------------------------------
-- Allow all functions post-anonymization
----------------------------------------------------------------

SELECT 2 * length(city) FROM test_customers GROUP BY city;

----------------------------------------------------------------
-- Unsupported queries
----------------------------------------------------------------

-- Get rejected because aggregators are unsupported.
SELECT SUM(id) FROM test_customers;
SELECT MIN(id) + MAX(id) FROM test_customers;
SELECT city FROM test_customers GROUP BY 1 ORDER BY AVG(LENGTH(city));
SELECT count(city ORDER BY city) FROM test_customers;
SELECT count(*) FILTER (WHERE true) FROM test_customers;

-- Get rejected because only a subset of expressions is supported for defining buckets.
SELECT COUNT(*) FROM test_customers GROUP BY LENGTH(city);
SELECT COUNT(*) FROM test_customers GROUP BY city || 'xxx';
SELECT LENGTH(city) FROM test_customers;
SELECT city, 'aaaa' FROM test_customers;
SELECT COUNT(*) FROM test_customers GROUP BY round(floor(id));
SELECT COUNT(*) FROM test_customers GROUP BY floor(cast(discount AS integer));
SELECT COUNT(*) FROM test_customers GROUP BY substr(city, 1, id);
SELECT COUNT(*) FROM test_customers GROUP BY substr('aaaa', 1, 2);

-- Get rejected because of subqueries
SELECT COUNT(*), COUNT(x.city), COUNT(DISTINCT x.id)
FROM (
  SELECT * FROM test_customers
) x;

SELECT COUNT(*), COUNT(x.city), COUNT(DISTINCT x.user_id)
FROM (
  SELECT y.city as city, y.id as user_id
  FROM ( SELECT * FROM test_customers ) y
) x;

SELECT x.user_city, COUNT(*), COUNT(DISTINCT x.id), COUNT(DISTINCT x.cid)
FROM (
  SELECT id, cid, city as user_city
  FROM test_customers
  INNER JOIN test_purchases tp ON id = cid
) x
GROUP BY 1;

SELECT COUNT(DISTINCT x.modified_id) FROM ( SELECT id AS modified_id FROM test_customers ) x;

-- Get rejected because of subqueries, but used to be rejected because of their inner aggregation
SELECT * FROM ( SELECT COUNT(*) FROM test_customers ) x;

SELECT COUNT(city)
FROM (
  SELECT city FROM test_customers
  GROUP BY 1
) x;

-- Get rejected because of JOINs
SELECT COUNT(*), COUNT(DISTINCT id), COUNT(DISTINCT cid) FROM test_customers
  INNER JOIN test_purchases tp ON id = cid;

SELECT COUNT(c.city), COUNT(p.name) FROM test_customers c
  LEFT JOIN test_purchases ON c.id = cid
  LEFT JOIN test_products p ON pid = p.id;

SELECT city, COUNT(price) FROM test_customers, test_products GROUP BY 1;

SELECT city, COUNT(price) FROM test_products, test_customers GROUP BY 1;

SELECT city, COUNT(price) FROM test_products CROSS JOIN test_customers GROUP BY 1;

-- Get rejected because of WHERE
SELECT COUNT(*) FROM test_customers WHERE city = 'London';

----------------------------------------------------------------
-- Untrusted mode query restrictions
----------------------------------------------------------------

SET pg_diffix.session_access_level = 'publish_untrusted';
SELECT diffix.access_level();

-- Get accepted
SELECT substring(city, 1, 2) from test_customers;
SELECT floor(discount) from test_customers;
SELECT diffix.floor_by(discount, 2) from test_customers;
SELECT diffix.floor_by(discount, 20) from test_customers;
SELECT diffix.floor_by(discount, 2.0) from test_customers;
SELECT diffix.floor_by(discount, 0.2) from test_customers;
SELECT diffix.floor_by(discount, 20.0) from test_customers;
SELECT diffix.floor_by(discount, 50.0) from test_customers;

-- Get rejected because of invalid generalization parameters
SELECT substring(city, 2, 2) from test_customers;
SELECT diffix.floor_by(discount, 3) from test_customers;
SELECT diffix.floor_by(discount, 3.0) from test_customers;
SELECT diffix.floor_by(discount, 5000000000.1) from test_customers;
SELECT diffix.round_by(discount, 2) from test_customers;
SELECT diffix.ceil_by(discount, 2) from test_customers;

-- Get rejected because of invalid generalizing functions
SELECT width_bucket(discount, 2, 200, 5) from test_customers;
