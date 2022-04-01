LOAD 'pg_diffix';

SET ROLE diffix_test;
SET pg_diffix.session_access_level = 'publish_trusted';

----------------------------------------------------------------
-- Miscellaneous queries
----------------------------------------------------------------

DO $$
BEGIN
  PERFORM count(*) FROM test_customers;
END;
$$;
