# Admin tutorial

This document provides examples on how to install and configure `pg_diffix` to expose a simple dataset for anonymized querying.  It assumes that an existing installation of PostgreSQL 14 on a Linux system is available.

It provides multiple examples. The first is a [simple example](#simple-example) that assumes a database named `test_db` was created, and the personal data is in the table `test_table`, containing a column named `id`, which uniquely identifies protected entities (the anonymization ID).

Following this are [several examples of real datasets](#real-examples) which discuss the rationale behind how to determine which columns contain anonymization IDs.

# Simple Example

## Installation (per PostgreSQL installation)

1\. Install the packages required for building the extension:

```
sudo apt-get install make jq gcc postgresql-server-dev-14
```

2\. Install PGXN Client tools:

```
sudo apt-get install pgxnclient
```

3\. Install the extension:

```
sudo pgxn install pg_diffix
```

## Activation (per database)

1\. Connect to the database as a superuser:

```
sudo -u postgres psql test_db
```

2\. Activate the extension for the current database:

```
CREATE EXTENSION pg_diffix;
```

3\. Automatically load the extension for all users connecting to the database:

```
ALTER DATABASE test_db SET session_preload_libraries TO 'pg_diffix';
```

## User configuration (per database)

1\. Create a user account for the analyst:

```
CREATE USER analyst_role WITH PASSWORD 'some_password';
```

2\. Give the analyst read-only access to the test database:

```
GRANT CONNECT ON DATABASE test_db TO analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst_role;
```

3\. Label the analyst as restricted and trusted:

```
CALL diffix.mark_role('analyst_role', 'anonymized_trusted');
```

## Anonymization configuration (per table)

1\. Label the test data table as personal (requiring anonymization):

```
CALL diffix.mark_personal('test_table', 'id');
```

__That's it!__ The analyst can now connect to the database and issue (only) anonymizing queries against the test dataset. Refer to the [Administration Guide](admin_guide.md) for additional details, or if you wish to modify default anonymization parameters.

# Real Examples

The real examples are databases that are used for the Diffix bounty programs. The databases can be found at https://db001.gda-score.org/.

## The banking database

The banking database is taken from a dataset originally used in a machine learning competition. The data itself is from a Czech bank, and had been pseudonymized to protect privacy. The database is named `raw_banking`. It has seven tables: `accounts`, `cards`, `clients`, `disp`, `loans`, `orders`, and `transactions`. In its original form, different tables had to be joined. For instance, if the analyst wished to obtain client information for each account, then `clients` needs to be joined with `accounts`. 

Diffix Elm does not support SQL JOIN, so the banking tables at db001.gda-score.org are built from joins of `accounts` and `clients` with the other tables (`cards`, `orders`, and so on). In other words, all tables have complete account and client information. The columns from the `accounts` table, for instance, are:

```
      Column      |  Type   
------------------+---------
 account_id       | integer
 acct_district_id | integer
 frequency        | text   
 acct_date        | integer
 uid              | integer
 disp_type        | text   
 birth_number     | text   
 cli_district_id  | integer
 lastname         | text   
 firstname        | text   
 birthdate        | date   
 gender           | text   
 ssn              | text   
 email            | text   
 street           | text   
 zip              | text   
 ```

 The first 6 columns are from the original `accounts` table. Subsequent columns come from the JOIN with `clients`. (The last 8 columns are synthetically generated to mimic personally identifiable information for the bounty program.)

 Note that the `uid` column was originally called `client_id`. It was modified to `uid` for the purpose of the Diffix bounty programs run in prior years.

### Activating pg_diffix

The pg_diffix extension must be activated for every database:

```sql
CREATE EXTENSION pg_diffix;
ALTER DATABASE raw_banking SET session_preload_libraries TO 'pg_diffix';
```

### Selecting the AID columns

The clients (as identified by the `uid` column) are persons, and we wish to protect their anonymity. As such, the `uid` column needs to be tagged as being one of the AIDs. In many, if not most cases, only one column needs to be tagged as the AID.

In the case of `raw_banking`, however, many accounts are joint accounts, meaning that the same account (as identified by the `account_id`) is associated with two clients. If only the `uid` column is an AID, and the suppression threshold is set to the minimum value of 2 (`pg_diffix.low_count_min_threshold`, see the [Administration Guide](admin_guide.md)), then it is possible to output buckets associated with a single account, which in turn can be associated with either of two persons. This can be regarded as breaking anonymity, and so the `account_id` should also be tagged as an AID.

We therefore configure the two AID columns (in this case for the `accounts` table) as follows:

```sql
CALL diffix.mark_personal('accounts', 'uid', 'account_id');
```

We can check that this succeeded with:

```sql
SELECT * FROM diffix.show_labels();
```

which outputs:

```
 objtype |          objname           |        label
---------+----------------------------+----------------------
 table   | public.accounts            | personal
 column  | public.accounts.account_id | aid
 column  | public.accounts.uid        | aid
(3 rows)
```

Similarly, we label AID columns for the remaining tables. (Note that the `client` nd `cards` tables do not contain `account_id`.)

```sql
CALL diffix.mark_personal('clients', 'uid');
CALL diffix.mark_personal('cards', 'uid');
CALL diffix.mark_personal('disp', 'uid', 'account_id');
CALL diffix.mark_personal('loans', 'uid', 'account_id');
CALL diffix.mark_personal('orders', 'uid', 'account_id');
CALL diffix.mark_personal('transactions', 'uid', 'account_id');
```

### Create users

Following the convention of the [demo notebook](banking.ipynb), we create three users with direct, trusted, and untrusted access to the database. The user names are `direct_user`, `trusted_user`, and `untrusted_user` respectively, all with password `demo`:

```sql
CREATE USER direct_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE raw_banking TO direct_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO direct_user;
CALL diffix.mark_role('direct_user', 'direct');
```

```sql
CREATE USER trusted_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE raw_banking TO trusted_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO trusted_user;
CALL diffix.mark_role('trusted_user', 'anonymized_trusted');
```

```sql
CREATE USER untrusted_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE raw_banking TO untrusted_user;
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
 column  | public.accounts.account_id     | aid
 column  | public.accounts.uid            | aid
 column  | public.cards.uid               | aid
 column  | public.clients.uid             | aid
 column  | public.disp.account_id         | aid
 column  | public.disp.uid                | aid
 column  | public.loans.account_id        | aid
 column  | public.loans.uid               | aid
 column  | public.orders.account_id       | aid
 column  | public.orders.uid              | aid
 column  | public.transactions.account_id | aid
 column  | public.transactions.uid        | aid
 role    | direct_user                    | direct
 role    | trusted_user                   | anonymized_trusted
 role    | untrusted_user                 | anonymized_untrusted
(22 rows)
```

## NYC Taxi Database

The NYC Taxi database is named `raw_taxi`. It has a single table `rides`. The table `rides` has 29 columns:

```
    column_name    |          data_type
-------------------+-----------------------------
 mta_tax           | numeric
 tip_amount        | numeric
 tolls_amount      | numeric
 total_amount      | numeric
 pickup_datetime   | timestamp without time zone
 dropoff_datetime  | timestamp without time zone
 birthdate         | date
 passenger_count   | smallint
 trip_time_in_secs | bigint
 trip_distance     | numeric
 pickup_longitude  | numeric
 pickup_latitude   | numeric
 dropoff_longitude | numeric
 dropoff_latitude  | numeric
 rate_code         | smallint
 fare_amount       | numeric
 surcharge         | numeric
 zip               | text
 uid               | character varying
 vendor_id         | character varying
 sf_flag           | character varying
 payment_type      | character varying
 lastname          | text
 firstname         | text
 gender            | text
 ssn               | text
 email             | text
 street            | text
 med               | character varying
(29 rows)
```

### Generalizable types

Of these columns, three of them are `datetime` related. Diffix Elm has no generalization function for these types, and so to generalize them, `substring()` can be used after they are `CAST` to text in the query. (Alternatively, one could build a materialized view that casts these three columns to text. This would simplify query writing, but we are choosing not to do that for pedagogic reasons.)

### Activating pg_diffix

The pg_diffix extension must be activated for every database:

```sql
CREATE EXTENSION pg_diffix;
ALTER DATABASE raw_taxi SET session_preload_libraries TO 'pg_diffix';
```

### Selecting the AID columns

The `med` column is the taxi medallion. It identifies the taxi itself. The `uid` column identifies the driver (originally named `hack`). We would like to provide anonymity for the driver. (No passenger data is in the table: otherwise we'd certainly like to protect the passenger as well.)

We configure the AID column as follows:

```sql
CALL diffix.mark_personal('rides', 'uid');
```

We can check that this succeeded with:

```sql
SELECT * FROM diffix.show_labels();
```

which outputs:

```
 objtype |     objname      |  label
---------+------------------+----------
 table   | public.rides     | personal
 column  | public.rides.uid | aid
(2 rows)
```

### Create users

Following the convention of the [demo notebook](banking.ipynb), we create three users with direct, trusted, and untrusted access to the database. The user names are `direct_user`, `trusted_user`, and `untrusted_user` respectively, all with password `demo`:

Here we create the same three user roles as we did with banking:

```sql
CREATE USER direct_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE raw_taxi TO direct_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO direct_user;
CALL diffix.mark_role('direct_user', 'direct');
```

```sql
CREATE USER trusted_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE raw_taxi TO trusted_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO trusted_user;
CALL diffix.mark_role('trusted_user', 'anonymized_trusted');
```

```sql
CREATE USER untrusted_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE raw_taxi TO untrusted_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO untrusted_user;
CALL diffix.mark_role('untrusted_user', 'anonymized_untrusted');
```

And check:

```
SELECT * FROM diffix.show_labels();

 objtype |     objname      |        label
---------+------------------+----------------------
 table   | public.rides     | personal
 column  | public.rides.uid | aid
 role    | direct_user      | direct
 role    | trusted_user     | anonymized_trusted
 role    | untrusted_user   | anonymized_untrusted
(5 rows)
```

## The Sci-Hub database

Sci-Hub is a service that downloads scientific articles free of charge. The associated database is named `raw_scihub`. It has a single table `downloads`.

Each entry represents one download from an IP address (which has been hashed). We wish to protect the downloading individuals, which are best represented by the hashed IP address. The database has named this column `uid`.

Here we simply list the complete set of commands needed to label the AID column and create the three users.


```sql
CREATE EXTENSION pg_diffix;
ALTER DATABASE raw_scihub SET session_preload_libraries TO 'pg_diffix';

CALL diffix.mark_personal('downloads', 'uid');

CREATE USER direct_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE raw_scihub TO direct_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO direct_user;
CALL diffix.mark_role('direct_user', 'direct');

CREATE USER trusted_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE raw_scihub TO trusted_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO trusted_user;
CALL diffix.mark_role('trusted_user', 'anonymized_trusted');

CREATE USER untrusted_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE raw_scihub TO untrusted_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO untrusted_user;
CALL diffix.mark_role('untrusted_user', 'anonymized_untrusted');
```

And check:

```
SELECT * FROM diffix.show_labels();
```

```
 objtype |       objname        |        label
---------+----------------------+----------------------
 table   | public.downloads     | personal
 column  | public.downloads.uid | aid
 role    | direct_user          | direct
 role    | trusted_user         | anonymized_trusted
 role    | untrusted_user       | anonymized_untrusted
(5 rows)
```

## The Census database

The `raw_census` database has a single table `persons` with 119 columns. Each row refers to one person, and the column `uid` has a unique identifier per row.

Here again we simply list the complete set of commands needed to label the AID column and create the three users.

```sql
CREATE EXTENSION pg_diffix;
ALTER DATABASE raw_census SET session_preload_libraries TO 'pg_diffix';

CALL diffix.mark_personal('persons', 'uid');

CREATE USER direct_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE raw_census TO direct_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO direct_user;
CALL diffix.mark_role('direct_user', 'direct');

CREATE USER trusted_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE raw_census TO trusted_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO trusted_user;
CALL diffix.mark_role('trusted_user', 'anonymized_trusted');

CREATE USER untrusted_user WITH PASSWORD 'demo';
GRANT CONNECT ON DATABASE raw_census TO untrusted_user;
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
 table   | public.persons     | personal
 column  | public.persons.uid | aid
 role    | direct_user        | direct
 role    | trusted_user       | anonymized_trusted
 role    | untrusted_user     | anonymized_untrusted
(5 rows)
```