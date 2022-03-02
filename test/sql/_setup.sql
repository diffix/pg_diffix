CREATE EXTENSION IF NOT EXISTS pg_diffix;
LOAD 'pg_diffix';

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

CREATE TABLE empty_test_customers (id INTEGER PRIMARY KEY, name TEXT, city TEXT, discount REAL);

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