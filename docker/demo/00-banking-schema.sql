CREATE TABLE accounts (
    account_id    integer NOT NULL,
    district_id   integer,
    frequency     text,
    date          integer
);

CREATE TABLE accounts_receivables (
    countrycode   character varying,
    customerid    character varying,
    paperlessdate character varying,
    invoicenumber character varying,
    invoicedate   character varying,
    duedate       character varying,
    invoiceamount character varying,
    disputed      character varying,
    settleddate   character varying,
    paperlessbill character varying,
    daystosettle  character varying,
    dayslate      character varying
);

CREATE TABLE credit_cards (
    card_id       integer,
    disp_id       integer,
    type          text,
    issues        text
);

CREATE TABLE clients (
    client_id     integer NOT NULL,
    birth_number  text,
    district_id   integer
);

CREATE TABLE dispositions (
    disp_id       integer NOT NULL,
    client_id     integer,
    account_id    integer,
    type          text
);

CREATE TABLE loans (
  loan_id         integer NOT NULL,
  account_id      integer,
  date            text,
  amount          integer,
  duration        integer,
  payments        double precision,
  status text
);

CREATE TABLE loss_events (
  region              character varying,
  business            character varying,
  name                character varying,
  status              character varying,
  riskcategory        character varying,
  risksubcategory     character varying,
  discoverydate       character varying,
  occurrencestartdate character varying,
  year                character varying,
  netloss             character varying,
  recoveryamount      character varying,
  estimatedgrossloss  character varying,
  recoveryamountpct   character varying
);

CREATE TABLE orders (
  order_id        integer NOT NULL,
  account_id      integer,
  bank_to         text,
  account_to      text,
  amount          double precision,
  k_symbol        text
);

CREATE TABLE transactions (
  trans_id        integer NOT NULL,
  account_id      integer,
  date            text,
  type            text,
  operation       text,
  amount          double precision,
  balance         double precision,
  k_symbol        text,
  bank            text,
  account         text
);
