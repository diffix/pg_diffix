CREATE TABLE test_customers (id INTEGER PRIMARY KEY, city TEXT);
INSERT INTO test_customers VALUES
 (0, NULL), (1, 'Berlin'), (2, 'Berlin'), (3, 'Rome'), (4, 'London'), (5, 'Berlin'), (6, 'Rome'),
 (7, 'Rome'), (8, 'Berlin'), (9, 'Rome'), (10, 'Berlin'), (11, 'Rome'), (12, 'Rome'), (13, 'Rome');

CREATE EXTENSION IF NOT EXISTS pg_diffix;
LOAD 'pg_diffix';
SELECT diffix_reload_config();

SELECT COUNT(*) FROM test_customers;
SELECT DIFFIX_COUNT(city) FROM test_customers;
SELECT city, DIFFIX_COUNT(DISTINCT id) FROM test_customers GROUP BY 1 HAVING DIFFIX_LCF(id);

SELECT city FROM test_customers;

SELECT city FROM test_customers GROUP BY 1 HAVING length(city) <> 4;

SELECT COUNT(*) FROM test_customers WHERE city = 'London';
