# Admin case study

This document provides several case studies of `pg_diffix` configuration. The examples are from real databases that are used for the Diffix bounty programs. The databases can be found at https://db001.gda-score.org/.

## The banking database

The banking database is taken from a dataset originally used in a machine learning competition. The data itself is from a Czech bank, and had been pseudonymized to protect privacy. The database is named `banking0`. It has seven tables: `accounts`, `cards`, `clients`, `disp`, `loans`, `orders`, and `transactions`.

### Database preparation

The banking data contains clients and their banking accounts. In its original form, most tables contained a column `account_id` that identifies the account associated with the corresponding row of data. A table `clients` contains information about each client, and a table `accounts` links clients and accounts. An analyst would have to use `JOIN` to include both client and account information in a query.

Diffix Elm does not support SQL JOIN, so the `banking0` tables at https://db001.gda-score.org are built from joins of `accounts` and `clients` with the other tables (`cards`, `orders`, and so on). In other words, all tables contain both account and client information.

The `banking0` database contains both individual and joint accounts. Data about both clients are therefore included in every row of data. The orders table, for instance, has the following columns:

```
      Column      |       Type       |
------------------+------------------+
 order_id         | integer          |
 bank_to          | text             |
 account_to       | text             |
 amount           | double precision |
 k_symbol         | text             |
 account_id       | integer          |
 acct_district_id | integer          |
 frequency        | text             |
 acct_date        | text             |
 client_id1       | integer          |
 cli_district_id1 | integer          |
 disp_type1       | text             |
 birth_number1    | text             |
 lastname1        | text             |
 client_id2       | integer          |
 cli_district_id2 | integer          |
 disp_type2       | text             |
 birth_number2    | text             |
 lastname2        | text             |
```

The original table contained only the first six columns (ending with `account_id`). During table preparation, we added columns containing account information (`acct_district_id`, `frequency`, and `acct_date`), and columns containing information about both clients. The values for the second client are `NULL` for individual accounts. (Note that the database from which the `banking0` database is derived is `banking`, which can also be viewed at db001.gda-score.org.)

### Activating pg_diffix

The pg_diffix extension must be activated for every database. After connecting to the `banking0` database, the superuser executes:

```sql
CREATE EXTENSION pg_diffix;
ALTER DATABASE banking SET session_preload_libraries TO 'pg_diffix';
```

### Selecting the AID columns

The clients (as identified by the `client_idX` columns) are persons, and we wish to protect their anonymity. As such, the `client_id1` and `client_id2` columns needs to be tagged as being AID columns.

It is also important to protect data about individual accounts, since an analyst could potentially otherwise link a joint account to its two clients. In principle, the `account_id` column could also be tagged as an AID column, and this would certainly protect accounts. Doing so is not necessary for the `banking0` database. This is because every table that contains `account_id` also contains the corresponding `client_id`'s. Protecting the `client_id` automatically protects the `account_id`.

There are two exceptions to this, the `orders` table, shown above, and the `transactions` table. Both of these tables have an additional account column (`account_to` in the `orders` table, and `account` in the `transactions` table). Assuming that we wish to protect accounts as well as clients, then these two columns should be tagged as AID columns.

Unfortunately, this creates a problem in the case of the `transactions.account` column. This column happens to contain a single value that appears in roughly 15% of all transactions. This in turn leads to poor utility because additional distortion is needed to mask the presence of that single value.

Rather than tag `account` as an AID, we instead chose to simply delete the column.

By contrast, no value in the `orders.account_to` column appears in more than 4 orders, and these account for roughly 0.1% of all `account_to` values. Tagging `account_to` as an AID column will therefore not unduly hurt utility.

The AID columns for all seven tables are configured as follows:

```sql
CALL diffix.mark_personal('accounts', 'client_id1', 'client_id2');
CALL diffix.mark_personal('cards', 'client_id');
CALL diffix.mark_personal('clients', 'client_id');
CALL diffix.mark_personal('disp', 'client_id1', 'client_id2');
CALL diffix.mark_personal('loans', 'client_id1', 'client_id2');
CALL diffix.mark_personal('orders', 'client_id1', 'client_id2', 'account_to');
CALL diffix.mark_personal('transactions', 'client_id1', 'client_id2');
```

We can check that this succeeded with:

```sql
SELECT * FROM diffix.show_labels();
```

```
 objtype |            objname             |        label
---------+--------------------------------+----------------------
 table   | public.accounts                | personal
 table   | public.cards                   | personal
 table   | public.clients                 | personal
 table   | public.disp                    | personal
 table   | public.loans                   | personal
 table   | public.orders                  | personal
 table   | public.transactions            | personal
 column  | public.accounts.client_id1     | aid
 column  | public.accounts.client_id2     | aid
 column  | public.cards.client_id         | aid
 column  | public.clients.client_id       | aid
 column  | public.disp.client_id1         | aid
 column  | public.disp.client_id2         | aid
 column  | public.loans.client_id1        | aid
 column  | public.loans.client_id2        | aid
 column  | public.orders.account_to       | aid
 column  | public.orders.client_id1       | aid
 column  | public.orders.client_id2       | aid
 column  | public.transactions.client_id1 | aid
 column  | public.transactions.client_id2 | aid
```

### Create users

Following the convention of the [demo notebook](banking.ipynb), we create three users with direct, trusted, and untrusted access to the database. The user names are `direct_user`, `trusted_user`, and `untrusted_user` respectively, all with password `demo`:

```sql
CREATE USER direct_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE banking TO direct_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO direct_user;
CALL diffix.mark_role('direct_user', 'direct');
```

```sql
CREATE USER trusted_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE banking TO trusted_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO trusted_user;
CALL diffix.mark_role('trusted_user', 'anonymized_trusted');
```

```sql
CREATE USER untrusted_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE banking TO untrusted_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO untrusted_user;
CALL diffix.mark_role('untrusted_user', 'anonymized_untrusted');
```

And check:

```
SELECT * FROM diffix.show_labels();
```

```
 objtype |            objname             |        label
---------+--------------------------------+----------------------
 table   | public.accounts                | personal
 table   | public.cards                   | personal
 table   | public.clients                 | personal
 table   | public.disp                    | personal
 table   | public.loans                   | personal
 table   | public.orders                  | personal
 table   | public.transactions            | personal
 column  | public.accounts.client_id1     | aid
 column  | public.accounts.client_id2     | aid
 column  | public.cards.client_id         | aid
 column  | public.clients.client_id       | aid
 column  | public.disp.client_id1         | aid
 column  | public.disp.client_id2         | aid
 column  | public.loans.client_id1        | aid
 column  | public.loans.client_id2        | aid
 column  | public.orders.account_to       | aid
 column  | public.orders.client_id1       | aid
 column  | public.orders.client_id2       | aid
 column  | public.transactions.client_id1 | aid
 column  | public.transactions.client_id2 | aid
 role    | direct_user                    | direct
 role    | trusted_user                   | anonymized_trusted
 role    | untrusted_user                 | anonymized_untrusted
```

## NYC Taxi Database

The NYC Taxi database is named `taxi`. It has a single table `jan08`. The table `jan08` has 22 columns:

```
    column_name    |          data_type
-------------------+-----------------------------
 surcharge         | double precision
 mta_tax           | double precision
 tip_amount        | double precision
 tolls_amount      | double precision
 total_amount      | double precision
 pickup_datetime   | timestamp without time zone
 dropoff_datetime  | timestamp without time zone
 passenger_count   | smallint
 trip_time_in_secs | bigint
 trip_distance     | double precision
 pickup_longitude  | numeric
 pickup_latitude   | numeric
 dropoff_longitude | numeric
 dropoff_latitude  | numeric
 rate_code         | smallint
 fare_amount       | double precision
 lastname          | text
 hack              | character varying
 vendor_id         | character varying
 sf_flag           | character varying
 payment_type      | character varying
 med               | character varying
(22 rows)
```

### Generalizable types

Of these columns, three of them are `datetime` related. Diffix Elm has no generalization function for these types, and so to generalize them, `substring()` can be used after they are `CAST` to text in the query. (Alternatively, one could build a materialized view that casts these three columns to text. This would simplify query writing, but we are choosing not to do that for pedagogic reasons.)

### Activating pg_diffix

The pg_diffix extension must be activated for every database:

```sql
CREATE EXTENSION pg_diffix;
ALTER DATABASE taxi SET session_preload_libraries TO 'pg_diffix';
```

### Selecting the AID columns

The `med` column is the taxi medallion. It identifies the taxi itself. The `hack` column identifies the driver. We would like to provide anonymity for the driver. (No passenger data is in the table: otherwise we'd certainly like to protect the passenger as well.)

We configure the AID column as follows:

```sql
CALL diffix.mark_personal('jan08', 'hack');
```

We can check that this succeeded with:

```sql
SELECT * FROM diffix.show_labels();
```

which outputs:

```
 objtype |      objname      |        label
---------+-------------------+----------------------
 table   | public.jan08      | personal
 column  | public.jan08.hack | aid
```

### Create users

Following the convention of the [demo notebook](banking.ipynb), we create three users with direct, trusted, and untrusted access to the database. The user names are `direct_user`, `trusted_user`, and `untrusted_user` respectively, all with password `demo`:

Here we create the same three user roles as we did with banking:

```sql
CREATE USER direct_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE taxi TO direct_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO direct_user;
CALL diffix.mark_role('direct_user', 'direct');
```

```sql
CREATE USER trusted_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE taxi TO trusted_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO trusted_user;
CALL diffix.mark_role('trusted_user', 'anonymized_trusted');
```

```sql
CREATE USER untrusted_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE taxi TO untrusted_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO untrusted_user;
CALL diffix.mark_role('untrusted_user', 'anonymized_untrusted');
```

And check:

```
SELECT * FROM diffix.show_labels();

 objtype |      objname      |        label
---------+-------------------+----------------------
 table   | public.jan08      | personal
 column  | public.jan08.hack | aid
 role    | direct_user       | direct
 role    | trusted_user      | anonymized_trusted
 role    | untrusted_user    | anonymized_untrusted
(5 rows)
```

## The Sci-Hub database

Sci-Hub is a service that downloads scientific articles free of charge. The associated database is named `scihub`. It has a single table `sep2015`.

Each entry represents one download from an IP address (which has been hashed). We wish to protect the downloading individuals, which are best represented by the hashed IP address. The database has named this column `uid`.

Here we simply list the complete set of commands needed to label the AID column and create the three users.


```sql
CREATE EXTENSION pg_diffix;
ALTER DATABASE scihub SET session_preload_libraries TO 'pg_diffix';

CALL diffix.mark_personal('sep2015', 'uid');

CREATE USER direct_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE scihub TO direct_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO direct_user;
CALL diffix.mark_role('direct_user', 'direct');

CREATE USER trusted_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE scihub TO trusted_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO trusted_user;
CALL diffix.mark_role('trusted_user', 'anonymized_trusted');

CREATE USER untrusted_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE scihub TO untrusted_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO untrusted_user;
CALL diffix.mark_role('untrusted_user', 'anonymized_untrusted');
```

And check:

```
SELECT * FROM diffix.show_labels();
```

```
 objtype |      objname       |        label
---------+--------------------+----------------------
 table   | public.sep2015     | personal
 column  | public.sep2015.uid | aid
 role    | direct_user        | direct
 role    | trusted_user       | anonymized_trusted
 role    | untrusted_user     | anonymized_untrusted
(5 rows)
```

## The Census database

The `census0` database has a single table `uidperhousehold` with 119 columns. Each row refers to one person, and the column `uid` has a unique identifier per row.

Here again we simply list the complete set of commands needed to label the AID column and create the three users.

```sql
CREATE EXTENSION pg_diffix;
ALTER DATABASE census SET session_preload_libraries TO 'pg_diffix';

CALL diffix.mark_personal('uidperhousehold', 'uid');

CREATE USER direct_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE census TO direct_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO direct_user;
CALL diffix.mark_role('direct_user', 'direct');

CREATE USER trusted_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE census TO trusted_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO trusted_user;
CALL diffix.mark_role('trusted_user', 'anonymized_trusted');

CREATE USER untrusted_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE census TO untrusted_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO untrusted_user;
CALL diffix.mark_role('untrusted_user', 'anonymized_untrusted');
```

And check:

```
SELECT * FROM diffix.show_labels();
```

```
 objtype |          objname           |        label
---------+----------------------------+----------------------
 table   | public.uidperhousehold     | personal
 column  | public.uidperhousehold.uid | aid
 role    | direct_user                | direct
 role    | trusted_user               | anonymized_trusted
 role    | untrusted_user             | anonymized_untrusted
(5 rows)
```