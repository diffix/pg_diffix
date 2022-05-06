# Important notice

This is a pre-release version of the extension and is not intended for general use yet.
It may be unstable and documentation is limited.
If you have any questions, please contact us at [hello@open-diffix.org](mailto:hello@open-diffix.org).

# Admin tutorial

This document provides an example on how to install and configure `pg_diffix` to expose a simple dataset
for anonymized querying.
It assumes that an existing installation of PostgreSQL 14 on a Linux system is available, a database
named `test_db` was created, and the personal data is in the table `test_table`,
containing a column named `id`, which uniquely identifies protected entities (the anonymization ID).

## Installation

1. Install the packages required for building the extension:

`sudo apt-get install make jq gcc postgresql-server-dev-14`

2. Install PGXN Client tools:

`sudo apt-get install pgxnclient`

3. Install the extension:

`sudo pgxn install pg_diffix`

## Activation

1. Connect to the database as a superuser:

`sudo -u postgres psql test_db`

2. Activate the extension for the current database:

`CREATE EXTENSION pg_diffix;`

3. Automatically load the extension for all users connecting to the database:

`ALTER DATABASE test_db SET session_preload_libraries TO 'pg_diffix';`

## Configuration

1. Label the test data as personal (requiring anonymization):

`CALL diffix.mark_personal('test_table', 'id');`

2. Create an account for the analyst:

`CREATE USER analyst_role WITH PASSWORD 'some_password';`

3. Give the analyst read-only access to the test database:

```
GRANT CONNECT ON DATABASE test_db TO analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst_role;
```

4. Label the analyst as restricted and trusted:

`CALL diffix.mark_role('analyst_role', 'anonymized_trusted');`


__That's it!__ The analyst can now connect to the database and issue (only) anonymizing queries against the test dataset.