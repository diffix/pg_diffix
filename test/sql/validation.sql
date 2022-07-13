LOAD 'pg_diffix';

CREATE TABLE test_validation (
  id INTEGER PRIMARY KEY,
  name TEXT,
  city TEXT,
  discount REAL,
  birthday DATE,
  lunchtime TIME,
  last_seen TIMESTAMP
);

CALL diffix.mark_personal('test_validation', 'id');
CALL diffix.mark_not_filterable('test_validation', 'birthday');
CALL diffix.mark_not_filterable('test_validation', 'last_seen');
CALL diffix.mark_filterable('test_validation', 'last_seen');

CREATE TABLE superclass (x INTEGER);
CREATE TABLE subclass (x INTEGER, y INTEGER);
INSERT INTO subclass VALUES (1, 2);

CALL diffix.mark_personal('superclass', 'x');
CALL diffix.mark_personal('subclass', 'y');

ALTER TABLE subclass INHERIT superclass;

-- No-op. Repeated to test the error on conflicting configuration
CALL diffix.mark_personal('superclass', 'x');
CALL diffix.mark_personal('subclass', 'y');

SET ROLE diffix_test;

----------------------------------------------------------------
-- Trusted mode query restrictions
----------------------------------------------------------------

SET pg_diffix.session_access_level = 'anonymized_trusted';
SELECT diffix.access_level();

----------------------------------------------------------------
-- Supported queries
----------------------------------------------------------------

-- Supported functions for defining buckets
SELECT COUNT(*) FROM test_validation
  GROUP BY substring(city, 1, 1);

SELECT COUNT(*) FROM test_validation
  GROUP BY width_bucket(id, 0, 1000, 10), width_bucket(id::float, 0.0, 1000.0, 10);

SELECT COUNT(*) FROM test_validation
  GROUP BY round(id::float, -1), round(id::numeric, -1);

SELECT COUNT(*) FROM test_validation
  GROUP BY round(id::float), ceil(id::float), ceiling(id::float), floor(id::float);

SELECT COUNT(*) FROM test_validation
  GROUP BY round(id::numeric), ceil(id::numeric), ceiling(id::numeric), floor(id::numeric);

SELECT COUNT(*) FROM test_validation
  GROUP BY round(id::integer), ceil(id::integer), ceiling(id::integer), floor(id::integer);

SELECT COUNT(*) FROM test_validation
  GROUP BY round(id::bigint), ceil(id::bigint), ceiling(id::bigint), floor(id::bigint);

SELECT
  diffix.round_by(id::numeric, 5),
  diffix.round_by(id::double precision, 5),
  COUNT(*)
FROM test_validation
GROUP BY 1, 2;

SELECT
  diffix.ceil_by(id::numeric, 5),
  diffix.ceil_by(id::double precision, 5),
  COUNT(*)
FROM test_validation
GROUP BY 1, 2;

SELECT
  diffix.floor_by(id::numeric, 5),
  diffix.floor_by(id::double precision, 5),
  COUNT(*)
FROM test_validation
GROUP BY 1, 2;

SELECT
  substring(cast(last_seen AS text), 1, 3),
  substring(cast(birthday AS text), 2, 3),
  substring(cast(lunchtime AS varchar), 1, 4)
FROM test_validation
GROUP BY 1, 2, 3;

-- Allow all functions post-anonymization.
SELECT 2 * length(city) FROM test_validation GROUP BY city;

-- Allow diffix.is_suppress_bin in non-direct access level.
SELECT city, count(*), diffix.is_suppress_bin(*) from test_validation GROUP BY 1;

-- Allow simple equality filters in WHERE clauses
SELECT COUNT(*) FROM test_validation WHERE city = 'London';
SELECT COUNT(*) FROM test_validation WHERE substring(city, 1, 1) = 'L' AND discount = 10.0 AND discount = 20.0;

-- Set operations between anonymizing queries.
SELECT city FROM test_validation EXCEPT SELECT city FROM test_validation;
SELECT city FROM test_validation UNION SELECT city FROM test_validation;

-- Anonymizing sublinks are supported.
SELECT EXISTS (SELECT city FROM test_validation);
SELECT 1 WHERE EXISTS (SELECT city FROM test_validation);
SELECT 'London' IN (SELECT city FROM test_validation);

-- Anonymizing leaf subqueries are supported.
SELECT * FROM ( SELECT COUNT(*) FROM test_validation ) x;

SELECT COUNT(city)
FROM (
  SELECT city FROM test_validation
  GROUP BY 1
) x;

SELECT COUNT(*), COUNT(x.city), COUNT(DISTINCT x.city)
FROM (
  SELECT name, city, discount, birthday, lunchtime, last_seen FROM test_validation
) x;

SELECT COUNT(DISTINCT x.modified_name) FROM ( SELECT name AS modified_name FROM test_validation ) x;

SELECT COUNT(*), COUNT(x.city), COUNT(DISTINCT x.user_name)
FROM (
  SELECT y.city as city, y.name as user_name
  FROM ( SELECT name, city, discount, birthday, lunchtime, last_seen FROM test_validation ) y
) x;

SELECT * FROM (SELECT name FROM test_validation) x, (SELECT city FROM test_validation) y;

WITH c AS (SELECT city FROM test_validation) SELECT * FROM c;

SELECT (SELECT city FROM test_validation);

-- Allow discovery commands
\dt empty_test_customers
\d empty_test_customers
\dt+ empty_test_customers
\d+ empty_test_customers

-- Allow discovery statements
SELECT EXISTS (SELECT FROM PG_Catalog.pg_tables WHERE schemaname='public' AND tablename='test_customers');
SELECT EXISTS (SELECT FROM Information_Schema.tables WHERE table_schema='public' AND table_name='test_customers');

-- Settings and labels UDFs work
SELECT * FROM diffix.show_settings() LIMIT 2;
SELECT * FROM diffix.show_labels() WHERE objname LIKE 'public.test_customers%';

-- Allow prepared statements
PREPARE prepared(float) AS SELECT discount, count(*) FROM empty_test_customers WHERE discount = $1 GROUP BY 1;
EXECUTE prepared(1.0);

----------------------------------------------------------------
-- Unsupported queries
----------------------------------------------------------------

-- Get rejected because non SELECT queries are unsupported.
INSERT INTO test_validation VALUES (NULL, NULL,NULL);

-- Get rejected because WITH is unsupported.
WITH c AS (SELECT 1 FROM test_validation) SELECT 1 FROM test_validation;

-- Get rejected because GROUPING SETS are unsupported.
SELECT city FROM test_validation GROUP BY GROUPING SETS ((city), ());
SELECT city FROM test_validation GROUP BY CUBE ((city));

-- Get rejected because SRF functions are unsupported.
SELECT generate_series(1,4) FROM test_validation;

-- Get rejected because sublinks are unsupported.
SELECT city, (SELECT 1 FROM test_validation) FROM test_validation GROUP BY 1;

-- Get rejected because DISTINCT is unsupported.
SELECT DISTINCT city FROM test_validation;

-- Get rejected because window functions are unsupported.
SELECT avg(discount) OVER (PARTITION BY city) FROM test_validation;

-- Get rejected because aggregators are unsupported.
SELECT SUM(distinct id) FROM test_validation;
SELECT SUM(id + 5) FROM test_validation;
SELECT AVG(distinct id) FROM test_validation;
SELECT AVG(id + 5) FROM test_validation;
SELECT diffix.avg_noise(distinct id) FROM test_validation;
SELECT diffix.avg_noise(id + 5) FROM test_validation;
SELECT MIN(id) + MAX(id) FROM test_validation;
SELECT city FROM test_validation GROUP BY 1 ORDER BY AVG(LENGTH(city));
SELECT count(city ORDER BY city) FROM test_validation;
SELECT count(*) FILTER (WHERE true) FROM test_validation;
SELECT count(distinct id + 5) FROM test_validation;
SELECT count(distinct least(id, 5)) FROM test_validation;
SELECT count(id + 5) FROM test_validation;
SELECT count(least(id, 5)) FROM test_validation;
SELECT diffix.count_histogram(city) FROM test_validation;

-- Get rejected because only a subset of expressions is supported for defining buckets.
SELECT COUNT(*) FROM test_validation GROUP BY LENGTH(city);
SELECT COUNT(*) FROM test_validation GROUP BY city || 'xxx';
SELECT LENGTH(city) FROM test_validation;
SELECT city, 'aaaa' FROM test_validation GROUP BY 1, 2;
PREPARE prepared_param_as_label(text) AS SELECT city, $1 FROM test_validation GROUP BY 1, 2;
EXECUTE prepared_param_as_label('aaaa');
SELECT COUNT(*) FROM test_validation GROUP BY round(floor(id));
SELECT COUNT(*) FROM test_validation GROUP BY floor(cast(discount AS integer));
SELECT COUNT(*) FROM test_validation GROUP BY substr(city, 1, id);
SELECT COUNT(*) FROM test_validation GROUP BY substr('aaaa', 1, 2);

-- Get rejected because expression node type is unsupported.
SELECT COALESCE(discount, 20) FROM test_validation;
SELECT NULLIF(discount, 20) FROM test_validation;
SELECT GREATEST(discount, 20) FROM test_validation;
SELECT LEAST(discount, 20) FROM test_validation;

-- Get rejected because of JOINs
SELECT COUNT(*), COUNT(DISTINCT id), COUNT(DISTINCT cid) FROM test_validation
  INNER JOIN test_purchases tp ON id = cid;

SELECT COUNT(c.city), COUNT(p.name) FROM test_validation c
  LEFT JOIN test_purchases ON c.id = cid
  LEFT JOIN test_products p ON pid = p.id;

SELECT city, COUNT(price) FROM test_validation, test_products GROUP BY 1;

SELECT city, COUNT(price) FROM test_products, test_validation GROUP BY 1;

SELECT city, COUNT(price) FROM test_products CROSS JOIN test_validation GROUP BY 1;

-- Get rejected because of invalid WHERE clauses
SELECT COUNT(*) FROM test_validation WHERE city <> 'London';
SELECT COUNT(*) FROM test_validation WHERE city = 'London' OR discount = 10;
SELECT COUNT(*) FROM test_validation WHERE diffix.round_by(id, 5) = 0;

-- Get rejected because of non-datetime cast to text
SELECT cast(id AS text) FROM test_validation GROUP BY 1;
SELECT cast(id AS varchar) FROM test_validation GROUP BY 1;
SELECT substring(cast(id AS text), 1, 1) FROM test_validation GROUP BY 1;
SELECT substring(cast(id AS varchar), 1, 1) FROM test_validation GROUP BY 1;

-- Invalid subqueries are rejected.
SELECT * FROM (SELECT length(city) FROM test_validation) x;
SELECT EXISTS (SELECT length(city) FROM test_validation);
SELECT 1 WHERE EXISTS (SELECT length(city) FROM test_validation);
SELECT 1 UNION SELECT length(city) FROM test_validation;
SELECT * FROM (SELECT 1) t1, (SELECT length(city) FROM test_validation) t2;
WITH c AS (SELECT length(city) FROM test_validation) SELECT * FROM c;
SELECT (SELECT length(city) FROM test_validation);

-- Get rejected because of accessing pg_catalog tables with sensitive stats
SELECT * FROM pg_stats LIMIT 10;
SELECT * FROM pg_statistic LIMIT 10;
SELECT * FROM pg_stat_user_functions LIMIT 10;
SELECT * FROM pg_stat_user_indexes LIMIT 10;
SELECT * FROM pg_class LIMIT 10;

-- Get rejected because of inheritance
SELECT x, y FROM subclass;
SELECT x FROM superclass;

-- Get rejected because attempt to use system columns
SELECT ctid FROM test_validation;
SELECT tableoid FROM test_validation;
SELECT count(ctid) FROM test_validation;
SELECT count(tableoid) FROM test_validation;
SELECT count(distinct ctid) FROM test_validation;
SELECT count(distinct tableoid) FROM test_validation;

-- Get rejected because of selecting AID columns
SELECT id FROM test_validation;
SELECT 1 FROM test_validation GROUP BY id;
SELECT * FROM (SELECT id FROM test_validation) z;

-- Get accepted because of selecting AID with generalization
SELECT diffix.floor_by(id, 2), count(*) FROM test_validation GROUP BY 1;

----------------------------------------------------------------
-- Untrusted mode query restrictions
----------------------------------------------------------------

SET pg_diffix.session_access_level = 'anonymized_untrusted';
SELECT diffix.access_level();

-- Get accepted
SELECT substring(city, 1, 2) from test_validation;
SELECT floor(discount) from test_validation;
SELECT round(discount) from test_validation;
SELECT discount from test_validation;
SELECT diffix.floor_by(discount, 2) from test_validation;
SELECT diffix.round_by(discount, 2) from test_validation;
SELECT diffix.floor_by(discount, 20) from test_validation;
SELECT diffix.floor_by(discount, 2.0) from test_validation;
SELECT diffix.floor_by(discount, 0.2) from test_validation;
SELECT diffix.floor_by(discount, 20.0) from test_validation;
SELECT diffix.floor_by(discount, 50.0) from test_validation;
SELECT diffix.count_histogram(id, 5) from test_validation;
SELECT count(*) FROM test_validation WHERE discount = 3;

-- Get rejected because of invalid generalization parameters
SELECT substring(city, 2, 2) from test_validation;
SELECT diffix.floor_by(discount, 3) from test_validation;
SELECT diffix.floor_by(discount, 3.0) from test_validation;
SELECT diffix.floor_by(discount, 5000000000.1) from test_validation;
SELECT diffix.count_histogram(id, 3) from test_validation;

-- Get rejected because of invalid generalizing functions
SELECT width_bucket(discount, 2, 200, 5) from test_validation;
SELECT ceil(discount) from test_validation;
SELECT diffix.ceil_by(discount, 2) from test_validation;

-- Marking columns as `not_filterable` works.
SELECT COUNT(*) FROM test_validation WHERE substring(cast(birthday as text), 4) = '2000';
-- Marking columns as `filterable` works.
SELECT COUNT(*) FROM test_validation WHERE substring(cast(last_seen as text), 4) = '2000';
-- Allow prepared statements with generalization constants as params, and validate them
PREPARE prepared_floor_by(numeric) AS SELECT diffix.floor_by(discount, $1) FROM test_validation GROUP BY 1;
EXECUTE prepared_floor_by(2.0);
EXECUTE prepared_floor_by(2.1);
PREPARE prepared_substring(int, int) AS SELECT substring(city, $1, $2) FROM test_validation GROUP BY 1;
EXECUTE prepared_substring(1, 2);
EXECUTE prepared_substring(2, 3);
