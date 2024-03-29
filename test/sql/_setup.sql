CREATE EXTENSION IF NOT EXISTS pg_diffix;
LOAD 'pg_diffix';

DO $$ BEGIN
  -- Fix salt to get consistent results.
  EXECUTE 'ALTER DATABASE ' || current_database() || ' SET pg_diffix.salt TO ''diffix''';
END $$ LANGUAGE plpgsql;

-- Create test data.
CREATE TABLE test_customers (id INTEGER PRIMARY KEY, city TEXT, discount REAL, planet TEXT);
INSERT INTO test_customers VALUES
  (0, NULL, NULL, NULL), (1, 'Berlin', 1.0, 'Earth'), (2, 'Berlin', 1.0, 'Earth'), (3, 'Rome', 0.5, 'Earth'),
  (4, 'London', 2.0, 'Earth'), (5, 'Berlin', 2.0, 'Earth'), (6, 'Rome', 1.0, 'Earth'), (7, 'Rome', 0.5, 'Earth'),
  (8, 'Berlin', 0.0, 'Earth'), (9, 'Rome', 1.0, 'Earth'), (10, 'Berlin', 2.0, 'Earth'), (11, 'Rome', 1.5, 'Earth'),
  (12, 'Rome', 0.5, 'Earth'), (13, 'Rome', 0.5, 'Earth'), (14, 'Berlin', 1.5, 'Earth'), (15, 'Berlin', 1.0, 'Earth'),
  (16, 'Berlin', 1.0, 'Earth'), (17, 'Madrid', 2.0, 'Earth');

CREATE TABLE test_products (id INTEGER PRIMARY KEY, name TEXT, price REAL);
INSERT INTO test_products VALUES (0, NULL, NULL), (1, 'Food', 1.5),
  (2, 'Car', 100.0), (3, 'House', 400.0), (4, 'Movie', 10.0);

CREATE TABLE test_purchases (cid INTEGER, pid INTEGER);
INSERT INTO test_purchases VALUES (0, 0), (0, 1), (0, 3), (1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1),
  (7, 1), (8, 2), (9, 1), (10, 2), (11, 1), (12, 1), (13, 2), (1, 2), (2, 2), (3, 2), (4, 2), (5, 2), (6, 2),
  (7, 1), (8, 1), (9, 2), (1, 2), (1, 2), (2, 1), (3, 0), (NULL, NULL), (4, 3), (5, 3), (6, 4), (7, -1),
  (7, 4), (8, -2), (9, -3), (10, -4);

CREATE TABLE test_patients (id INTEGER PRIMARY KEY, name TEXT, city TEXT, age INTEGER);
INSERT INTO test_patients VALUES
  (0, NULL, 'Berlin', 37), (1, 'John', 'Berlin', 37), (2, 'Alice', 'Berlin', 37), (3, 'Bob', 'Berlin', 37), (4, 'Emma', 'Berlin', 64),
  (5, 'John', 'Berlin', 57), (6, 'Bob', 'Berlin', 19), (7, 'Alice', 'Rome', 37), (8, 'Dan', 'Rome', 37), (9, 'Anna', 'Rome', 64),
  (10, 'Mike', 'London', 64), (11, 'Mike', 'London', 57), (12, 'Mike', 'London', 16), (13, 'Mike', 'London', 17);

CREATE TABLE empty_test_customers (id INTEGER PRIMARY KEY, name TEXT, city TEXT, discount REAL);

-- Config tables (and also check handling of namespaces).
CALL diffix.mark_personal('public.test_customers', 'id');
CALL diffix.mark_personal('public.test_purchases', 'cid');
CALL diffix.mark_personal('public.test_patients', 'id', 'name');
CALL diffix.mark_personal('public.empty_test_customers', 'id');
CALL diffix.mark_public('public.test_products');

-- There is no CREATE USER IF NOT EXISTS, we need to wrap and silence the output
DO $$
BEGIN
  CREATE ROLE diffix_test WITH NOSUPERUSER;
EXCEPTION WHEN duplicate_object THEN
END
$$;

GRANT CONNECT ON DATABASE contrib_regression TO diffix_test;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO diffix_test;

-- Allow SELECT on future test-specific tables.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO diffix_test;
