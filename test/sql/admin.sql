LOAD 'pg_diffix';

----------------------------------------------------------------
-- Admin functions
----------------------------------------------------------------

-- Multiple calls should give consistent results.
SELECT diffix.hash_record(test_customers) FROM test_customers;
SELECT diffix.hash_record(test_customers), diffix.hash_record(test_customers) FROM test_customers;
SELECT encode(diffix.hash_record(test_customers), 'hex') FROM test_customers;

-- Can hash empty table.
SELECT diffix.hash_record(empty_test_customers) FROM empty_test_customers;
SELECT encode(diffix.hash_record(empty_test_customers), 'hex') FROM empty_test_customers;

