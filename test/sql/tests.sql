CREATE EXTENSION IF NOT EXISTS pg_diffix;
LOAD 'pg_diffix';

SET pg_diffix.default_access_level = 'publish';

CREATE TABLE test_customers (id INTEGER PRIMARY KEY, city TEXT);
INSERT INTO test_customers VALUES
  (0, NULL), (1, 'Berlin'), (2, 'Berlin'), (3, 'Rome'), (4, 'London'), (5, 'Berlin'), (6, 'Rome'),
  (7, 'Rome'), (8, 'Berlin'), (9, 'Rome'), (10, 'Berlin'), (11, 'Rome'), (12, 'Rome'), (13, 'Rome');

INSERT INTO diffix_config (rel_namespace_name, rel_name, aid_attname) VALUES
  ('public', 'test_customers', 'id');

SELECT COUNT(*) FROM test_customers;

SELECT COUNT(city) FROM test_customers;

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
