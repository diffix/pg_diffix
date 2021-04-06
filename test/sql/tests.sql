CREATE EXTENSION IF NOT EXISTS pg_diffix;
LOAD 'pg_diffix';

SET pg_diffix.session_access_level = 'publish';

-- Create test data.
CREATE TABLE test_customers (id INTEGER PRIMARY KEY, city TEXT);
INSERT INTO test_customers VALUES
  (0, NULL), (1, 'Berlin'), (2, 'Berlin'), (3, 'Rome'), (4, 'London'), (5, 'Berlin'), (6, 'Rome'),
  (7, 'Rome'), (8, 'Berlin'), (9, 'Rome'), (10, 'Berlin'), (11, 'Rome'), (12, 'Rome'), (13, 'Rome');

CREATE TABLE test_products (id INTEGER PRIMARY KEY, name TEXT, price REAL);
INSERT INTO test_products VALUES (0, NULL, NULL), (1, 'Food', 1.5), (2, 'Car', 100.0);

CREATE TABLE test_purchases (cid INTEGER, pid INTEGER);
INSERT INTO test_purchases VALUES (0, 0), (0, 1), (0, 3), (1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1),
  (7, 1), (8, 2), (9, 1), (10, 2), (11, 1), (12, 1), (13, 2), (1, 2), (2, 2), (3, 2), (4, 2), (5, 2), (6, 2),
  (7, 1), (8, 1), (9, 2), (1, 2), (1, 2), (2, 1), (3, 0), (NULL, NULL);

-- Config tables.
SECURITY LABEL FOR pg_diffix ON TABLE test_customers IS 'sensitive';
SECURITY LABEL FOR pg_diffix ON COLUMN test_customers.id IS 'aid';
SECURITY LABEL FOR pg_diffix ON TABLE test_purchases IS 'sensitive';
SECURITY LABEL FOR pg_diffix ON COLUMN test_purchases.cid IS 'aid';

-- Supported queries.
SELECT COUNT(*) FROM test_customers;
SELECT COUNT(*) FROM test_purchases;

SELECT COUNT(city) FROM test_customers;

SELECT COUNT(DISTINCT cid) FROM test_purchases;

SELECT city, COUNT(DISTINCT id) FROM test_customers GROUP BY 1;

SELECT COUNT(*), COUNT(DISTINCT id), COUNT(DISTINCT cid) FROM test_customers
  INNER JOIN test_purchases tp ON id = cid;

SELECT COUNT(c.city), COUNT(p.name) FROM test_customers c
  LEFT JOIN test_purchases ON c.id = cid
  LEFT JOIN test_products p ON pid = p.id;

SELECT city, COUNT(price) FROM test_customers, test_products GROUP BY 1;
SELECT city, COUNT(price) FROM test_products, test_customers GROUP BY 1;

-- Gets rejected because `city` is not the AID.
SELECT COUNT(DISTINCT city) FROM test_customers;

SELECT city FROM test_customers;

SELECT city FROM test_customers GROUP BY 1 HAVING length(city) <> 4;

SELECT COUNT(*) FROM test_customers WHERE city = 'London';

-- Get rejected because aggregators are unsupported.
SELECT SUM(id) FROM test_customers;
SELECT MIN(id) + MAX(id) FROM test_customers;
SELECT city FROM test_customers GROUP BY 1 ORDER BY AVG(LENGTH(city));
