LOAD 'pg_diffix';

-- Mark, unmark objects, show their labels
CALL diffix.mark_role('diffix_test', 'direct');
SELECT * FROM diffix.show_labels() WHERE objname IN ('diffix_test', 'public.test_products');
CALL diffix.unmark_table('public.test_products');
CALL diffix.unmark_role('diffix_test');
SELECT * FROM diffix.show_labels() WHERE objname IN ('diffix_test', 'public.test_products');
CALL diffix.mark_role('diffix_test', 'anonymized_trusted');
CALL diffix.mark_public('public.test_products');
SELECT * FROM diffix.show_labels() WHERE objname IN ('diffix_test', 'public.test_products');

-- Strict checking of anonymization parameters
SET pg_diffix.strict = false;
SET pg_diffix.top_count_max = 0;
SET pg_diffix.top_count_max = 3;
SET pg_diffix.strict = true;
SET pg_diffix.top_count_max = 4;
SET pg_diffix.strict = true;
SET pg_diffix.top_count_max = 3;
SET pg_diffix.top_count_max = 4;

-- Reject unsupported column types during AID labeling
SECURITY LABEL FOR pg_diffix ON COLUMN test_customers.discount IS 'aid';

-- Restriction on users with access level below `direct`
SET ROLE diffix_test;
SET pg_diffix.session_access_level = 'anonymized_trusted';

-- Non-superuser allowed settings
SET pg_diffix.text_label_for_suppress_bin = '*';

-- Non-superuser restricted settings
SET pg_diffix.salt = '';
CALL diffix.mark_public('public.test_customers');
SET pg_diffix.noise_layer_sd = 0.0;
