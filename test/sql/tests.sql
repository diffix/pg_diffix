CREATE EXTENSION IF NOT EXISTS pg_diffix;
LOAD 'pg_diffix';

SET pg_diffix.default_access_level = 'publish';

CREATE TABLE test_customers (id INTEGER PRIMARY KEY, city TEXT);
INSERT INTO test_customers VALUES
  (0, NULL), (1, 'Berlin'), (2, 'Berlin'), (3, 'Rome'), (4, 'London'), (5, 'Berlin'), (6, 'Rome'),
  (7, 'Rome'), (8, 'Berlin'), (9, 'Rome'), (10, 'Berlin'), (11, 'Rome'), (12, 'Rome'), (13, 'Rome');

CREATE TABLE test_products (id INTEGER PRIMARY KEY, name TEXT, price REAL);
INSERT INTO test_products VALUES (0, NULL, NULL), (1, 'Food', 1.5), (2, 'Car', 100.0);

CREATE TABLE test_purchases (cid INTEGER, pid INTEGER);
INSERT INTO test_purchases VALUES (0, 0), (0, 1), (0, 3), (1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1),
  (7, 1), (8, 2), (9, 1), (10, 2), (11, 1), (12, 1), (13, 2), (1, 2), (2, 2), (3, 2), (4, 2), (5, 2), (6, 2),
  (7, 1), (8, 1), (9, 2), (10, 2), (11, 2), (12, 1), (13, 0), (NULL, NULL);

INSERT INTO diffix_config (rel_namespace_name, rel_name, aid_attname) VALUES
  ('public', 'test_customers', 'id'), ('public', 'test_purchases', 'cid');

-- Supported queries.
SELECT COUNT(*) FROM test_customers;
SELECT COUNT(*) FROM test_purchases;

SELECT COUNT(city) FROM test_customers;

SELECT COUNT(DISTINCT cid) FROM test_purchases;

SELECT city, COUNT(DISTINCT id) FROM test_customers GROUP BY 1;

-- Gets rejected because `city` is not the AID.
SELECT COUNT(DISTINCT city) FROM test_customers;

SELECT city FROM test_customers;

SELECT city FROM test_customers GROUP BY 1 HAVING length(city) <> 4;

SELECT COUNT(*) FROM test_customers WHERE city = 'London';

-- Get rejected because aggregators are unsupported.
SELECT SUM(id) FROM test_customers;
SELECT MIN(id) + MAX(id) FROM test_customers;
SELECT city FROM test_customers GROUP BY 1 ORDER BY AVG(LENGTH(city));
