--
-- accounts
--
CALL diffix.mark_personal('public', 'accounts', 'diffix', 'account_id');
ALTER TABLE accounts ADD CONSTRAINT accounts_pkey PRIMARY KEY (account_id);

--
-- accounts_receivables
--
CALL diffix.mark_personal('public', 'accounts_receivables', 'diffix', 'customerid');

--
-- credit_cards
--
CALL diffix.mark_personal('public', 'credit_cards', 'diffix', 'disp_id');

--
-- clients
--
CALL diffix.mark_personal('public', 'clients', 'diffix', 'client_id');

ALTER TABLE clients ADD CONSTRAINT clients_pkey PRIMARY KEY (client_id);

--
-- dispositions
--
CALL diffix.mark_personal('public', 'dispositions', 'diffix', 'client_id', 'account_id');

ALTER TABLE dispositions ADD CONSTRAINT dispositions_pkey PRIMARY KEY (disp_id);

--
-- loans
--
CALL diffix.mark_personal('public', 'loans', 'diffix', 'account_id');

ALTER TABLE loans ADD CONSTRAINT loans_pkey PRIMARY KEY (loan_id);

--
-- loss_events
--
SECURITY LABEL FOR pg_diffix ON TABLE loss_events IS 'public';

--
-- orders
--
CALL diffix.mark_personal('public', 'orders', 'diffix', 'account_id', 'account_to');

ALTER TABLE orders ADD CONSTRAINT orders_pkey PRIMARY KEY (order_id);

--
-- transactions
--
CALL diffix.mark_personal('public', 'transactions', 'diffix', 'account_id');

ALTER TABLE transactions ADD CONSTRAINT transactions_pkey PRIMARY KEY (trans_id);
