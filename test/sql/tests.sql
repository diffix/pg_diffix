CREATE EXTENSION IF NOT EXISTS pg_diffix;
LOAD 'pg_diffix';

SET pg_diffix.session_access_level = 'publish';

-- Create test data.
CREATE TABLE test_customers (id INTEGER PRIMARY KEY, city TEXT);
INSERT INTO test_customers VALUES
  (0, NULL), (1, 'Berlin'), (2, 'Berlin'), (3, 'Rome'), (4, 'London'), (5, 'Berlin'), (6, 'Rome'),
  (7, 'Rome'), (8, 'Berlin'), (9, 'Rome'), (10, 'Berlin'), (11, 'Rome'), (12, 'Rome'), (13, 'Rome'),
  (14, 'Berlin'), (15, 'Berlin');

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
-- `JOIN` queries
----------------------------------------------------------------

SELECT COUNT(*), COUNT(DISTINCT id), COUNT(DISTINCT cid) FROM test_customers
  INNER JOIN test_purchases tp ON id = cid;

SELECT COUNT(c.city), COUNT(p.name) FROM test_customers c
  LEFT JOIN test_purchases ON c.id = cid
  LEFT JOIN test_products p ON pid = p.id;

SELECT city, COUNT(price) FROM test_customers, test_products GROUP BY 1;

SELECT city, COUNT(price) FROM test_products, test_customers GROUP BY 1;

----------------------------------------------------------------
-- LCF & Filtering
----------------------------------------------------------------

SELECT id FROM test_customers;

SELECT city FROM test_customers;

SELECT city FROM test_customers GROUP BY 1 HAVING length(city) <> 4;

SELECT COUNT(*), COUNT(city), COUNT(DISTINCT city) FROM test_customers WHERE city = 'London';

----------------------------------------------------------------
-- Non-aggregating subqueries
----------------------------------------------------------------

-- Reference result
SELECT COUNT(*), COUNT(x.city), COUNT(DISTINCT x.id) FROM test_customers x;

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
  SELECT id, cid, 'City: ' || city as user_city
  FROM test_customers
  INNER JOIN test_purchases tp ON id = cid
) x
GROUP BY 1;

SELECT COUNT(DISTINCT x.modified_id) FROM ( SELECT id + 1 AS modified_id FROM test_customers ) x;

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

-- Get rejected because aggregating subqueries are not supported.
SELECT * FROM ( SELECT COUNT(*) FROM test_customers ) x;

SELECT COUNT(city)
FROM (
  SELECT city FROM test_customers
  GROUP BY 1
) x;

-- Get rejected because only a subset of functions is supported for defining buckets.
SELECT COUNT(*) FROM test_customers GROUP BY length(city);
SELECT COUNT(*) FROM test_customers GROUP BY city || 'xxx';
