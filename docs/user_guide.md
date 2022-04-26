# Important notice

This is a pre-release version of the extension and is not intended for general use yet.
It may be unstable and documentation is limited.
If you have any questions, please contact us at [hello@open-diffix.org](mailto:hello@open-diffix.org).

# User Guide

This document provides detailed information about the configuration, behavior and recommended usage
of __Diffix for PostgreSQL__.

## What is Diffix?

Diffix is a bundled set of mechanisms for anonymizing structured data. It was jointly developed by Aircloak GmbH and
the Max Planck Institute for Software Systems. Diffix exploits mechanisms that have been in use by national statistics
offices for decades: aggregation, generalization, noise, suppression, and swapping. It automatically applies these
mechanisms as needed on a query-by-query basis to minimize noise while ensuring strong anonymity.

## What is Diffix for PostgreSQL?

__Diffix for PostgreSQL__ is an implementation of the Diffix mechanism for PostgreSQL databases.
It is provided as an extension module for PostgreSQL version 13 or higher.

## Configuring the extension

System behaviour can be configured using a combination of custom variables and security labels.

### Labeling database objects

The module acts as a security provider and allows the marking of database objects with various anonymization labels,
which associate additional metadata, needed for anonymization, with existing objects. For more details about security
labels, see the official [documentation page](https://www.postgresql.org/docs/current/sql-security-label.html).

Only superusers can set anonymization labels.
To remove an anonymization label from an object, set it to `NULL`.

Execute `SELECT * FROM diffix.show_labels();` to display the current labels in use by the extension.

Tables can be labeled as `public` or `personal`. Direct access is allowed to public data even for restricted users.
Unlabeled tables can not be queried (unless `treat_unmarked_tables_as_public` is set to `true`).

Anonymization ID (AID) columns for a personal table have to be marked with the anonymization label `aid`.
A personal table can have one or more AID columns.

In order to label a table as `personal` and AID columns, use the
`diffix.mark_personal(namespace, table_name, aid_columns...)` procedure, for example:

```SQL
CALL diffix.mark_personal('public', 'my_table', 'id', 'last_name');
```

In order to label a table as `public`, use the `diffix.mark_public(namespace, table_name)` procedure.

Regular users can be marked with the anoymization labels `direct`, `publish_trusted` or `publish_untrusted`.
The value of the custom variable `pg_diffix.default_access_level` determines the access level for unlabeled regular users.

```SQL
SECURITY LABEL FOR pg_diffix ON ROLE analyst IS 'publish_trusted';
```

### Restricted features and extensions

At access levels other than `direct`, various data and features built into PostgreSQL are restricted. Among others:

1. Issue utility statements like `COPY` and `ALTER TABLE`, beside a few allowlisted ones, are not allowed.
2. Some of the data in `pg_catalog` tables like `pg_user_functions` is not accessible.
3. Selected subset of less frequently used PostgreSQL query features like `EXISTS` or `NULLIF` are disabled.
4. Inheritance involving a personal table is not allowed.
5. Some of the output of `EXPLAIN` for queries involving a personal table is censored.

**NOTE** If any of the currently blocked features is necessary for your use case, open an issue and let us know.

Row level security (RLS) can be enabled and used on personal tables.
It is advised that the active policies are vetted from the point of view of anonymity.

It is also strongly advised to vet any other extensions which are enabled alongside `pg_diffix`,
as well as any user-defined functions and aggregate functions.

### System settings

The module exposes a number of custom variables under the `pg_diffix` prefix.
Superusers can change these variables at runtime for their own session,
while regular users only have read access to them (with few notable exceptions).
To use different values for all future sessions, they have to be set in the configuration file.

Execute `SELECT * FROM diffix.show_settings();` to display the current settings of the extension.
If the result is empty, make sure [`pg_diffix` is loaded](#using-the-extension).

#### Data access settings

`pg_diffix.default_access_level` - Determines the access level for unlabeled users. Default value is `direct`.

`pg_diffix.session_access_level` - Sets the access level for the current session. It can never be higher than the access
level for the current user. Can be changed by all users. Defaults to maximum access level allowed.

`pg_diffix.treat_unmarked_tables_as_public` - Controls whether unmarked tables are readable and treated as public data.
Default value is `false`.

#### Noise settings

`pg_diffix.noise_layer_sd` - Standard deviation for each noise layer added to aggregates. Default value is 1.0.

#### Low count filter settings

`pg_diffix.low_count_min_threshold` - The lower bound for the number of distinct AID values that must be present in a
bucket for it to pass the low count filter. Default value is 2.

`pg_diffix.low_count_mean_gap` - The number of standard deviations between the lower bound and the mean of the
low count filter threshold. Default value is 2.0.

`pg_diffix.low_count_layer_sd` - The standard deviation for each noise layer used when calculating the low count filter
threshold. Default value is 1.0.

`pg_diffix.compute_suppress_bin` - If `True`, some rows in the returned result might belong to the suppress bin,
combining data of all the suppressed (rejected by the low count filter) protected entities. For an aggregating query,
the suppress bin will be returned in the first row, while for a non-aggregating query - in multiple initial rows. In
both cases, these rows have all column values `NULL` (`*` for text-typed columns, customizable via
`pg_diffix.text_label_for_suppress_bin`). Note that the suppress bin may itself be suppressed and not returned at all.

`pg_diffix.text_label_for_suppress_bin` - The value to use for the text-typed grouping labels in the suppress bin row.
Default value is `*`.

#### Aggregation settings

`pg_diffix.outlier_count_min` - Default value is 1. Must not be greater than `outlier_count_max`.

`pg_diffix.outlier_count_max` - Default value is 2. Must not be smaller than `outlier_count_min`.

`pg_diffix.top_count_min` - Default value is 4. Must not be greater than `top_count_max`.

`pg_diffix.top_count_max` - Default value is 6. Must not be smaller than `top_count_min`.

**NOTE** The outlier interval `(outlier_count_min, outlier_count_max)` must not be wider than the `(top_count_min, top_count_max)` interval.
