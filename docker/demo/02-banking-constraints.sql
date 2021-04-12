--
-- accounts
--
SECURITY LABEL FOR pg_diffix ON TABLE accounts IS 'sensitive';

SECURITY LABEL FOR pg_diffix ON COLUMN accounts.account_id IS 'aid';

ALTER TABLE accounts ADD CONSTRAINT accounts_pkey PRIMARY KEY (account_id);

--
-- accounts_receivables
--
SECURITY LABEL FOR pg_diffix ON TABLE accounts_receivables IS 'sensitive';

SECURITY LABEL FOR pg_diffix ON COLUMN accounts_receivables.customerid IS 'aid';

--
-- credit_cards
--
SECURITY LABEL FOR pg_diffix ON TABLE credit_cards IS 'sensitive';

SECURITY LABEL FOR pg_diffix ON COLUMN credit_cards.disp_id IS 'aid';

--
-- clients
--
SECURITY LABEL FOR pg_diffix ON TABLE clients IS 'sensitive';

SECURITY LABEL FOR pg_diffix ON COLUMN clients.client_id IS 'aid';

ALTER TABLE clients ADD CONSTRAINT clients_pkey PRIMARY KEY (client_id);

--
-- dispositions
--
SECURITY LABEL FOR pg_diffix ON TABLE dispositions IS 'sensitive';

SECURITY LABEL FOR pg_diffix ON COLUMN dispositions.client_id IS 'aid';
SECURITY LABEL FOR pg_diffix ON COLUMN dispositions.account_id IS 'aid';

ALTER TABLE dispositions ADD CONSTRAINT dispositions_pkey PRIMARY KEY (disp_id);

--
-- loans
--
SECURITY LABEL FOR pg_diffix ON TABLE loans IS 'sensitive';

SECURITY LABEL FOR pg_diffix ON COLUMN loans.account_id IS 'aid';

ALTER TABLE loans ADD CONSTRAINT loans_pkey PRIMARY KEY (loan_id);

--
-- loss_events
--
SECURITY LABEL FOR pg_diffix ON TABLE loss_events IS 'public';

--
-- orders
--
SECURITY LABEL FOR pg_diffix ON TABLE orders IS 'sensitive';

SECURITY LABEL FOR pg_diffix ON COLUMN orders.account_id IS 'aid';
SECURITY LABEL FOR pg_diffix ON COLUMN orders.account_to IS 'aid';

ALTER TABLE orders ADD CONSTRAINT orders_pkey PRIMARY KEY (order_id);

--
-- transactions
--
SECURITY LABEL FOR pg_diffix ON TABLE transactions IS 'sensitive';

SECURITY LABEL FOR pg_diffix ON COLUMN transactions.account_id IS 'aid';

ALTER TABLE transactions ADD CONSTRAINT transactions_pkey PRIMARY KEY (trans_id);
