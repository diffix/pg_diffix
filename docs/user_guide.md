# Important notice

This is a pre-release version of the extension and is not intended for general use yet.
It may be unstable and documentation is limited.
If you have any questions, please contact us at [hello@open-diffix.org](mailto:hello@open-diffix.org).

# Extension configuration

This document provides detailed information about the configuration, behavior and recommended usage of this extension __diffix_anonymization__.

## Background reading

The [Open Diffix website](https://www.open-diffix.org) has general information about the Open Diffix project. The [website FAQ](https://www.open-diffix.org/faq) is a good starting point. [This article](https://www.open-diffix.org/blog/diffix-elm-automates-what-statistics-offices-have-been-doing-for-decades) is a high-level overview of the anonymization mechanisms used by Diffix. [This paper](https://arxiv.org/abs/2201.04351) is a complete specification and privacy analysis of Diffix (version Elm).

## Configuration objects

Extension behavior is controlled by `security labels` and `settings`. Security labels specific to this extension are assigned to tables, columns, and roles (users). Settings are general parameters associated with system operation. Settings can be assigned in the configuration file and are instantiated per session.

Only superusers can assign security labels and settings. To remove a security label from an object, set it to `NULL`.

The command `SELECT * FROM diffix.show_labels();` displays the current security labels assigned to tables and columns by the extension.

The command `SELECT * FROM diffix.access_level();` displays the current security labels assigned to the role associated with the active session.

The command `SELECT * FROM diffix.show_settings();` displays the settings in use by the extension for the active session.

NOTE: If no configuration is done, then by default no anonymization takes place. All users will have full access to table contents.

For more information about PostgreSQL security labels, see the official [documentation page](https://www.postgresql.org/docs/current/sql-security-label.html).

## Security labels

### Security labels for roles (users)

A user may have one of three security labels, `direct`, `anonymized_trusted`, and `anonymized_untrusted`. Users with security label `direct` have full access to the data: no anonymization takes place. The other two labels determine whether a user that is subject to anonymization is treated as _trusted_ or _untrusted_.

* For trusted users, Diffix prevents _accidental_ release of personal data. So long as trusted users do not try to bypass Diffix anonymization, answers to queries are anonymous.
* For untrusted users, Diffix prevents _intentional_ release of personal data. Even for users that are malicious and try to break Diffix anonymization, answer to queries are anonymous.

Trusted users have fewer SQL restrictions than untrusted users, and therefore have better analytic utility.

For example, the command to assign the security label `anonymized_untrusted` to the `public_access` is:

```SQL
SECURITY LABEL FOR pg_diffix ON ROLE public_access IS 'anonymized_untrusted';
```

The value of the setting variable `pg_diffix.default_access_level` determines the default user class for unlabeled regular users (i.e. not superusers). It is set to `direct` by default.

### Security labels for tables

Tables may have one of two security labels, `public` or `personal`.

* Tables labeled as `personal` are anonymized by the extension (for `anonymized_trusted` and `anonymized_untrusted` users, not `direct` users).
* Tables labeled as `public` are not anonymized: all users have full access to these tables.

The procedure `diffix.mark_public(namespace, table_name)` labels a table as `public`.

Note that unlabeled tables can not be queried by `anonymized_trusted` and `anonymized_untrusted` users (unless the setting variable `pg_diffix.treat_unmarked_tables_as_public` is set to `true`).

### Security labels for columns

Each personal table has one or more _protected entities_ associated with it. A protected entity is normally a person or a something associated with a person (a device or an account), but it could also be other things such as a household, a store, or a company.

Each protected entity must have at least one column that contains the identifier of the protected entity. We refer to these columns generally as AID columns (Anonymization ID). See (How to select AID columns)[#how-to-select-aid-columns] for more guidance.

NOTE: if AID columns are not correctly labeled, the extension may fail to anonymize appropriately.

The procedure `diffix.mark_personal(namespace, table_name, aid_columns...)` is used to label a table as personal and to label its AID columns.

For example, 

```SQL
CALL diffix.mark_personal('public', 'employee_info', 'employee_id');
```

labels the table `employee_info` as personal, and labels the `employee_id` column as `employee_id` an AID column.

```SQL
CALL diffix.mark_personal('public', 'transactions', 'sender_acct', 'receiver_acct');
```

labels the table `transactions` as personal, and labels the `sender_acct` and `receiver_acct` columns as AID columns.

## Settings

The extension exposes a number of custom variables under the `pg_diffix` prefix.
Superusers can change these variables at runtime for their own session,
while regular users only have read access to them (with few notable exceptions).
To use different values for all future sessions, they have to be set in the configuration file.

Execute `SELECT * FROM diffix.show_settings();` to display the current settings of the extension.
If the result is empty, make sure [`pg_diffix` is loaded](#using-the-extension).

### Default behavior settings

`pg_diffix.default_access_level` - Determines the default security label for unlabeled users. Default value is `direct`.

`pg_diffix.session_access_level` - The security label that is active for the user's current session. This is initially set to the security label associated with the user, but the user can modify it to a value that has lower privilege than the user's normal security label. Privilege levels from high to low are `direct`, `anonymized_trusted`, `anonymized_untrusted`.

`pg_diffix.treat_unmarked_tables_as_public` - Determines the default security label for tables. If set to `true`, then the default security label is `public`.  The default value is `false`.

### Anonymization strength settings

Diffix has eight constants that determine the "strength" of anonymization. These impact the amount of noise, the number-of-persons threshold below which suppression takes place, and the behavior of flattening (see [this article](https://www.open-diffix.org/blog/diffix-elm-automates-what-statistics-offices-have-been-doing-for-decades) for an overview of these concepts).

Seven of these eight constants are set to values that provide sufficiently strong anonymity in virtually any realistic scenario. These seven constants rarely if ever need to be modified. Doing so only reduces data quality with little meaningful strengthening of anonymity.

The one anonymization constant that sometimes requires adjustment is `pg_diffix.low_count_min_threshold`. This sets the lower bound for the number of distinct AID values that must be present in a aggregate bucket to avoid suppression. Any buckets with fewer than `pg_diffix.low_count_min_threshold` distinct AIDs will be suppressed. See (How to set suppression threshold)[#how-to-set-suppression-threshold] for further guidance.

Default value is 3. Minimum allowed setting is 2.

#### Other seven settings

`pg_diffix.noise_layer_sd` - Standard deviation for each noise layer added to aggregates. Default value is 1.0. Minimum allowed setting is 1.0.

`pg_diffix.low_count_mean_gap` - The number of standard deviations between the lower bound `pg_diffix.low_count_min_threshold` and the mean of the
low count filter threshold. Default value is 2.0. Minimum allowed setting is 2.0.

`pg_diffix.low_count_layer_sd` - The standard deviation for each noise layer used when calculating the low count filter threshold. Default value is 1.0. Minimum allowed setting is 1.0.

`pg_diffix.outlier_count_min` - Default value is 1. Must not be greater than `outlier_count_max`. Minimum allowed setting is 1.

`pg_diffix.outlier_count_max` - Default value is 2. Must not be smaller than `outlier_count_min`. Minimum allowed setting is 2.

`pg_diffix.top_count_min` - Default value is 3. Must not be greater than `top_count_max`. Minimum allowed setting is 2.

`pg_diffix.top_count_max` - Default value is 4. Must not be smaller than `top_count_min`. Minimum allowed setting is 3.

**NOTE** The outlier interval `(outlier_count_min, outlier_count_max)` must not be wider than the `(top_count_min, top_count_max)` interval.

### Anonymization reporting settings

`pg_diffix.compute_suppress_bin` - If `True`, the first row in the query result contains the suppress bin (if any). This
provides the combined anonymized count of all the bins that were suppressed. The suppress bin shows
all column values as `NULL` (`*` for text-typed columns, customizable via `pg_diffix.text_label_for_suppress_bin`). Note
that the suppress bin may itself be suppressed.

`pg_diffix.text_label_for_suppress_bin` - The value to use for the text-typed grouping labels in the suppress bin row.
Default value is `*`.

## Restricted features and extensions

For users other than `direct`, various data and features built into PostgreSQL are restricted. Among others:

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

## How to select AID columns

zzzz

## How to set suppression threshold

zzzz