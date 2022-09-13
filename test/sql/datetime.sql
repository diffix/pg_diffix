LOAD 'pg_diffix';

CREATE TABLE test_datetime (
  id INTEGER,
  d DATE,
  t TIME,
  ts TIMESTAMP WITHOUT TIME ZONE,
  tz TIMESTAMP WITH TIME ZONE,
  i INTERVAL
);

INSERT INTO test_datetime VALUES
  (1, '2012-05-14', '14:59', '2012-05-14', '2012-05-14', '1 years'),
  (2, '2012-05-14', '14:59', '2012-05-14', '2012-05-14', '1 years'),
  (3, '2012-05-14', '14:59', '2012-05-14', '2012-05-14', '1 years'),
  (4, '2012-05-14', '14:59', '2012-05-14', '2012-05-14', '1 years'),
  (5, '2012-05-14', '14:59', '2012-05-14', '2012-05-14', '1 years'),
  (6, '2012-05-14', '14:59', '2012-05-14', '2012-05-14', '1 years'),
  (7, '2012-05-14', '14:59', '2012-05-14', '2012-05-14', '1 years');

CALL diffix.mark_personal('test_datetime', 'id');

GRANT ALL PRIVILEGES ON TABLE test_datetime TO diffix_test; 
SET ROLE diffix_test;
SET pg_diffix.session_access_level = 'anonymized_trusted';

----------------------------------------------------------------
-- Sanity checks
----------------------------------------------------------------

SELECT diffix.access_level();

----------------------------------------------------------------
-- Seeding
----------------------------------------------------------------

-- Datetime values are seeded in UTC the same regardless of global `datestyle` setting
SET datestyle = 'SQL';
SELECT ts, count(*) FROM test_datetime GROUP BY 1;
SET datestyle = 'ISO';
SELECT ts, count(*) FROM test_datetime GROUP BY 1;

SET TIMEZONE TO 'EST';
SELECT tz, count(*) FROM test_datetime GROUP BY 1;
SET TIMEZONE TO 'UTC';
SELECT tz, count(*) FROM test_datetime GROUP BY 1;

SET pg_diffix.session_access_level = 'direct';
UPDATE test_datetime SET tz = '2012-05-14T07:00+00:00' WHERE true;
SET pg_diffix.session_access_level = 'anonymized_trusted';
SELECT tz, count(*) FROM test_datetime GROUP BY 1;
SET pg_diffix.session_access_level = 'direct';
UPDATE test_datetime SET tz = '2012-05-14T08:00+01:00' WHERE true;
SET pg_diffix.session_access_level = 'anonymized_trusted';
SELECT tz, count(*) FROM test_datetime GROUP BY 1;

-- Datetime filtering
SELECT count(*) FROM test_datetime WHERE date_trunc('year', ts) = '2012-01-01'::timestamp;
SELECT count(*) FROM test_datetime WHERE extract(century from ts) = 21;
SELECT count(*) FROM test_datetime WHERE date_part('century', ts) = 21;
