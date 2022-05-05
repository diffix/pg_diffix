LOAD 'pg_diffix';

-- Mark, unmark objects, show their labels
CALL diffix.mark_role('diffix_test', 'direct');
SELECT * FROM diffix.show_labels();
CALL diffix.unmark_table('public.test_products');
CALL diffix.unmark_role('diffix_test');
SELECT * FROM diffix.show_labels();
CALL diffix.mark_role('diffix_test', 'anonymized_trusted');
CALL diffix.mark_public('public.test_products');
SELECT * FROM diffix.show_labels();
