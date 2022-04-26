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

CREATE TABLE led_base (
  id INTEGER PRIMARY KEY,
  dept TEXT,
  gender TEXT,
  title TEXT
);

INSERT INTO led_base VALUES
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
  (16, 'history', 'f', 'prof'),
  (17, 'cs', 'm', 'prof'),
  (18, 'cs', 'm', 'prof'),
  (19, 'cs', 'm', 'prof'),
  (20, 'cs', 'm', 'prof');

CREATE TABLE led_with_victim AS TABLE led_base;
INSERT INTO led_with_victim VALUES (21, 'cs', 'f', 'prof');

CREATE TABLE led_with_two_victims AS TABLE led_with_victim;
INSERT INTO led_with_two_victims VALUES (22, 'cs', 'f', 'prof');

CREATE TABLE led_with_three_cs_women AS TABLE led_with_two_victims;
INSERT INTO led_with_three_cs_women VALUES (23, 'cs', 'f', 'prof');

CREATE TABLE led_with_different_titles AS TABLE led_with_victim;
INSERT INTO led_with_different_titles VALUES (22, 'cs', 'f', 'asst');

CREATE TABLE led_with_star_bucket AS TABLE led_with_victim;
INSERT INTO led_with_star_bucket VALUES
  (22, 'biol', 'f', 'asst'), (23, 'chem', 'm', 'asst'), (24, 'biol', 'f', 'prof');

CALL diffix.mark_personal('public', 'led_base', 'id');
CALL diffix.mark_personal('public', 'led_with_victim', 'id');
CALL diffix.mark_personal('public', 'led_with_two_victims', 'id');
CALL diffix.mark_personal('public', 'led_with_three_cs_women', 'id');
CALL diffix.mark_personal('public', 'led_with_different_titles', 'id');
CALL diffix.mark_personal('public', 'led_with_star_bucket', 'id');

SET ROLE diffix_test;
SET pg_diffix.session_access_level = 'anonymized_trusted';

----------------------------------------------------------------
-- Grouping queries with count
----------------------------------------------------------------

SELECT dept, gender, title, count(*)
FROM led_base
GROUP BY 1, 2, 3;

SELECT dept, gender, title, count(*)
FROM led_with_victim
GROUP BY 1, 2, 3;

SELECT dept, gender, title, count(*)
FROM led_with_two_victims
GROUP BY 1, 2, 3;

SELECT dept, gender, title, count(*)
FROM led_with_three_cs_women
GROUP BY 1, 2, 3;

SELECT dept, gender, title, count(*)
FROM led_with_different_titles
GROUP BY 1, 2, 3;

SELECT dept, gender, title, count(*)
FROM led_with_star_bucket
GROUP BY 1, 2, 3;

----------------------------------------------------------------
-- Other queries
----------------------------------------------------------------

SELECT dept, gender, title, count(distinct id)
FROM led_base
GROUP BY 1, 2, 3;

SELECT dept, gender, title, count(distinct id)
FROM led_with_victim
GROUP BY 1, 2, 3;

SELECT count(*) FROM led_with_victim;
