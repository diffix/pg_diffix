CREATE EXTENSION pg_diffix;

CREATE TABLE test_customers (id INTEGER PRIMARY KEY, city TEXT);
INSERT INTO test_customers VALUES
 (0, NULL), (1, 'Berlin'), (2, 'Berlin'), (3, 'Rome'), (4, 'London'), (5, 'Berlin'), (6, 'Rome'),
 (7, 'Rome'), (8, 'Berlin'), (9, 'Rome'), (10, 'Berlin'), (11, 'Rome'), (12, 'Rome');

SELECT DIFFIX_COUNT(city) FROM test_customers;
SELECT city, DIFFIX_COUNT(DISTINCT id) FROM test_customers GROUP BY 1 HAVING DIFFIX_LCF(id);
