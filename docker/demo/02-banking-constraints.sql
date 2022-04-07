--
-- accounts
--
CALL diffix.make_personal('public', 'accounts', '646966666978', 'account_id');
ALTER TABLE accounts ADD CONSTRAINT accounts_pkey PRIMARY KEY (account_id);

--
-- accounts_receivables
--
CALL diffix.make_personal('public', 'accounts_receivables', '646966666978', 'customerid');

--
-- credit_cards
--
CALL diffix.make_personal('public', 'credit_cards', '646966666978', 'disp_id');

--
-- clients
--
CALL diffix.make_personal('public', 'clients', '646966666978', 'client_id');

ALTER TABLE clients ADD CONSTRAINT clients_pkey PRIMARY KEY (client_id);

--
-- dispositions
--
CALL diffix.make_personal('public', 'dispositions', '646966666978', 'client_id', 'account_id');

ALTER TABLE dispositions ADD CONSTRAINT dispositions_pkey PRIMARY KEY (disp_id);

--
-- loans
--
CALL diffix.make_personal('public', 'loans', '646966666978', 'account_id');

ALTER TABLE loans ADD CONSTRAINT loans_pkey PRIMARY KEY (loan_id);

--
-- loss_events
--
SECURITY LABEL FOR pg_diffix ON TABLE loss_events IS 'public';

--
-- orders
--
CALL diffix.make_personal('public', 'orders', '646966666978', 'account_id', 'account_to');

ALTER TABLE orders ADD CONSTRAINT orders_pkey PRIMARY KEY (order_id);

--
-- transactions
--
CALL diffix.make_personal('public', 'transactions', '646966666978', 'account_id');

ALTER TABLE transactions ADD CONSTRAINT transactions_pkey PRIMARY KEY (trans_id);
