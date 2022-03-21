LOAD 'pg_diffix';

SET ROLE diffix_test;
SET pg_diffix.session_access_level = 'publish_untrusted';

----------------------------------------------------------------
-- Sanity checks
----------------------------------------------------------------

SELECT diffix.access_level();

----------------------------------------------------------------
-- Untrusted mode query restrictions
----------------------------------------------------------------

-- Get accepted
SELECT substring(city, 1, 2) from empty_test_customers;
SELECT floor(discount) from empty_test_customers;
SELECT ceil(discount) from empty_test_customers;
SELECT round(discount) from empty_test_customers;
SELECT discount from empty_test_customers;
SELECT diffix.floor_by(discount, 2) from empty_test_customers;
SELECT diffix.round_by(discount, 2) from empty_test_customers;
SELECT diffix.ceil_by(discount, 2) from empty_test_customers;
SELECT diffix.floor_by(discount, 20) from empty_test_customers;
SELECT diffix.floor_by(discount, 2.0) from empty_test_customers;
SELECT diffix.floor_by(discount, 0.2) from empty_test_customers;
SELECT diffix.floor_by(discount, 20.0) from empty_test_customers;
SELECT diffix.floor_by(discount, 50.0) from empty_test_customers;

-- Get rejected because of invalid generalization parameters
SELECT substring(city, 2, 2) from empty_test_customers;
SELECT diffix.floor_by(discount, 3) from empty_test_customers;
SELECT diffix.floor_by(discount, 3.0) from empty_test_customers;
SELECT diffix.floor_by(discount, 5000000000.1) from empty_test_customers;

-- Get rejected because of invalid generalizing functions
SELECT width_bucket(discount, 2, 200, 5) from empty_test_customers;
