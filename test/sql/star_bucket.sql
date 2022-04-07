LOAD 'pg_diffix';

----------------------------------------------------------------
-- Setup
----------------------------------------------------------------

SET pg_diffix.noise_layer_sd = 0;
SET pg_diffix.low_count_min_threshold = 3;
SET pg_diffix.low_count_layer_sd = 0;
SET pg_diffix.outlier_count_min = 1;
SET pg_diffix.outlier_count_max = 1;
SET pg_diffix.top_count_min = 3;
SET pg_diffix.top_count_max = 3;

CREATE TABLE star_bucket_base (
  id INTEGER PRIMARY KEY,
  dept TEXT,
  gender TEXT,
  title TEXT
);

INSERT INTO star_bucket_base VALUES
  (1, 'math', 'm', 'prof'),
  (2, 'math', 'm', 'prof'),
  (3, 'math', 'm', 'prof'),
  (4, 'math', 'm', 'prof'),
  (5, 'math', 'f', 'prof'),
  (6, 'math', 'f', 'prof'),
  (7, 'math', 'f', 'prof'),
  (8, 'math', 'f', 'prof'),
  (9, 'history', 'm', 'prof'),
  (10, 'history', 'm', 'prof'),
  (11, 'history', 'm', 'prof'),
  (12, 'history', 'm', 'prof'),
  (13, 'history', 'f', 'prof'),
  (14, 'history', 'f', 'prof'),
  (15, 'history', 'f', 'prof'),
  (16, 'history', 'f', 'prof');

CREATE TABLE star_bucket AS TABLE star_bucket_base;
INSERT INTO star_bucket VALUES
  (17, 'phys', 'm', 'prof'),
  (18, 'chem', 'f', 'asst'),
  (20, 'cs', 'f', 'prof');

CREATE TABLE star_bucket_suppressed_1 AS TABLE star_bucket_base;
INSERT INTO star_bucket_suppressed_1 VALUES
  (17, 'phys', 'm', 'prof'),
  (18, 'phys', 'm', 'prof');

CREATE TABLE star_bucket_suppressed_2 AS TABLE star_bucket_base;
INSERT INTO star_bucket_suppressed_2 VALUES
  (17, 'chem', 'f', 'prof'),
  (18, 'phys', 'm', 'asst');

CREATE TABLE star_bucket_empty AS TABLE star_bucket_base WITH NO DATA;

CREATE TABLE star_bucket_only AS TABLE star_bucket_empty;
INSERT INTO star_bucket_only VALUES
  (1, 'math', 'm', 'asst'),
  (2, 'math', 'f', 'prof'),
  (3, 'phys', 'm', 'asst'),
  (4, 'cs', 'f', 'prof'),
  (5, 'history', 'f', 'asst');

CALL diffix.make_personal('star_bucket_base', '646966666978', 'id');
CALL diffix.make_personal('star_bucket', '646966666978', 'id');
CALL diffix.make_personal('star_bucket_suppressed_1', '646966666978', 'id');
CALL diffix.make_personal('star_bucket_suppressed_2', '646966666978', 'id');
CALL diffix.make_personal('star_bucket_empty', '646966666978', 'id');
CALL diffix.make_personal('star_bucket_only', '646966666978', 'id');

SET ROLE diffix_test;
SET pg_diffix.session_access_level = 'publish_trusted';

----------------------------------------------------------------
-- Grouping queries with count
----------------------------------------------------------------

SELECT dept, gender, title, count(*)
FROM star_bucket_base
GROUP BY 1, 2, 3;

SELECT dept, gender, title, count(*)
FROM star_bucket
GROUP BY 1, 2, 3;

SELECT dept, gender, title, count(*)
FROM star_bucket_suppressed_1
GROUP BY 1, 2, 3;

SELECT dept, gender, title, count(*)
FROM star_bucket_suppressed_2
GROUP BY 1, 2, 3;

SELECT dept, gender, title, count(*)
FROM star_bucket_empty
GROUP BY 1, 2, 3;

SELECT dept, gender, title, count(*)
FROM star_bucket_only
GROUP BY 1, 2, 3;

----------------------------------------------------------------
-- Other queries
----------------------------------------------------------------

SELECT *
FROM star_bucket;

SELECT dept, gender, title
FROM star_bucket;

SELECT 1
FROM star_bucket;

SELECT dept, gender, title
FROM star_bucket_empty;

SELECT 1
FROM star_bucket_empty;

SELECT dept, gender, title
FROM star_bucket_only;

SELECT 1
FROM star_bucket_only;
