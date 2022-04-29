# Important notice

This is a pre-release version of the extension and is not intended for general use yet.
It may be unstable and documentation is limited.
If you have any questions, please contact us at [hello@open-diffix.org](mailto:hello@open-diffix.org).

# Extension configuration

This document provides detailed information about the configuration, behavior and recommended usage of this extension.

## Background reading

The [Open Diffix website](https://www.open-diffix.org) has general information about the Open Diffix project. The [website FAQ](https://www.open-diffix.org/faq) is a good starting point. [This article](https://www.open-diffix.org/blog/diffix-elm-automates-what-statistics-offices-have-been-doing-for-decades) is a high-level overview of the anonymization mechanisms used by Diffix (version Elm). [This paper](https://arxiv.org/abs/2201.04351) is a complete specification and privacy analysis of Diffix Elm.

## Configuration objects

Extension behavior is controlled by __security labels__ and __settings__. Security labels specific to this extension are assigned to tables, columns, and roles (users). Settings are general parameters associated with system operation. Settings can be assigned in the configuration file and are instantiated per session.

Only superusers can assign security labels and settings. To remove a security label from an object, set it to `NULL`.

The command `SELECT diffix.show_labels();` displays the current security labels assigned to tables and columns by the extension.

The command `SELECT diffix.access_level();` displays the current security labels assigned to the role associated with the active session.

The command `SELECT diffix.show_settings();` displays the settings in use by the extension for the active session.

NOTE: If no configuration is done, then by default no anonymization takes place. All users will have full access to table contents.

For more information about PostgreSQL security labels, see the official [documentation page](https://www.postgresql.org/docs/current/sql-security-label.html).

## Secret Salt

TODO

## Security labels

### Security labels for roles (users)

A user's security label encodes one of three access levels, `direct`, `anonymized_trusted`, and `anonymized_untrusted`. Users with access level `direct` have full access to the data: no anonymization takes place. The other two labels determine whether Diffix Elm treats the user as _trusted_ or _untrusted_ when anonymizing.

* For trusted users, Diffix Elm prevents _accidental_ release of personal data. So long as trusted users do not try to bypass Diffix Elm anonymization, answers to queries are anonymous.
* For untrusted users, Diffix Elm prevents _intentional_ release of personal data. Even for users that are malicious and try to break Diffix Elm anonymization, answer to queries are anonymous.

Trusted users have fewer SQL restrictions than untrusted users, and therefore have better analytic utility.

For example, the command to assign the access level `anonymized_untrusted` to the role `public_access` is:

```SQL
SECURITY LABEL FOR pg_diffix ON ROLE public_access IS 'anonymized_untrusted';
```

The value of the setting variable `pg_diffix.default_access_level` determines the default user class users. It is set to `direct` by default.

### Security labels for tables

Tables may have one of two security labels, `public` or `personal`.

* Tables labeled as `personal` are anonymized by the extension (for `anonymized_trusted` and `anonymized_untrusted` users, not `direct` users).
* Tables labeled as `public` are not anonymized: all users have full access to these tables.

The procedure `diffix.mark_public(namespace, table_name)` labels a table as `public`.

Note that unlabeled tables can not be queried by `anonymized_trusted` and `anonymized_untrusted` users (unless the setting variable `pg_diffix.treat_unmarked_tables_as_public` is set to `true`).

### Security labels for columns

Each personal table has one or more _protected entities_ associated with it. A protected entity is normally a person or a something associated with a person (a device or an account), but it could also be other things such as a household, a store, or a company.

Each protected entity must have at least one column that contains the identifier of the protected entity. We refer to these columns generally as AID columns (Anonymization ID). See [How to select AID columns](#how-to-select-aid-columns) for more guidance.

__NOTE:__ if AID columns are not correctly labeled, the extension may fail to anonymize appropriately.

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

Execute `SELECT diffix.show_settings();` to display the current settings of the extension.
If the result is empty, make sure [`pg_diffix` is loaded](#using-the-extension).

### Default behavior settings

`pg_diffix.default_access_level` - Determines the default access level for unlabeled users. Default value is `direct`.

`pg_diffix.session_access_level` - The access level that is active for the user's current session. This is initially set to the access level associated with the user, but the user can modify it to a value that has lower privilege than the user's normal access level. Privilege levels from high to low are `direct`, `anonymized_trusted`, `anonymized_untrusted`.

`pg_diffix.treat_unmarked_tables_as_public` - If set to `true`, then tables are `public` by default. Otherwise tables are unlabeled by default. The default value is `false`.

### Anonymization strength settings

Diffix Elm has a number of constants that determine the "strength" of anonymization. These impact the amount of noise, the number-of-persons threshold below which suppression takes place, and the behavior of flattening (see [this article](https://www.open-diffix.org/blog/diffix-elm-automates-what-statistics-offices-have-been-doing-for-decades) for an overview of these concepts).

All but one of these constants are set to values that provide sufficiently strong anonymity in virtually any realistic scenario. These other constants rarely if ever need to be modified. Doing so only reduces data quality with little meaningful strengthening of anonymity.

The one anonymization constant that sometimes requires adjustment is `pg_diffix.low_count_min_threshold`. This sets the lower bound for the number of distinct AID values that must be present in a aggregate bin to avoid suppression. Any bins with fewer than `pg_diffix.low_count_min_threshold` distinct AIDs will be suppressed. See [How to set suppression threshold](#how-to-set-suppression-threshold) for further guidance.

Default value is 3. Minimum allowed setting is 2.

#### Remaining anonymization strength settings

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

**TODO:** I think this kind of information is better put in the notebook tutorial? Or if you want it here it seems incomplete or something. Needs work...

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

Every personal (anonymized) table must have at least one column that identifies the protected entity or entities. We refer to this column as an AID (Anonymizing ID) column. A good AID is one where the value is different for each distinct protected entity, and each protected entity is represented by one value. A good AID should also have very few if any `NULL` values.

Although a protected entity does not need to be a person, for readability the following assumes that this is the case.

The AID is used by Diffix Elm primarily for two purposes. The first, and most important, is to properly suppress bins (output aggregates) that contain too few individuals. The other is to add enough noise to hide the contribution of any given individual.

Examples of AID columns include account numbers, credit card numbers, mobile phone identifiers, email addresses, and login names. Note that these examples are often not perfect AIDs. A given individual might have several accounts. A login name may be shared by several individuals.

### Imperfect AID columns

Given that AIDs may not be perfect, some care must be taken in the selection of AID columns. The main situation to avoid is one where 1) an individual can have several AID values, and 2) there is another column that may effectively identify the individual, especially a column with publicly known information like family name or street address.

For example, imagine the following query in a table where `account_number` is the AID column:

```sql
SELECT last_name, religion, count(*)
FROM table
GROUP BY last_name, religion
```

If an individual has several accounts, and their `last_name` is unique in the table, then they may be the only individual in the bin with that last name because Diffix Elm will interpret the bin as having several distinct individuals (due to there being several distinct `account_number` values for this individual).

In this example, we refer to `last_name` as the isolating column. Other columns could serve the role as the isolating column, for instance `street_address`.

There are several ways to avoid this.

__Remove the isolating column.__ Without the isolating column, the individual cannot be isolated like this. As a general rule, it is always good practice to remove columns that do not have analytic value.

__Label the isolating column as an additional AID column.__ This solves the privacy problem at the expense of poorer analytic utility. For instance, if `last_name` is labeled as an AID column, then all individuals with the same last name would be treated as a single individual by Diffix Elm. This leads to unnecessary suppression and additional noise. As a general rule, one should avoid labeling columns as AID columns if the column values frequently pertain to multiple individuals.

__Label some other appropriate column as an additional AID column.__  It might be that there is another column, in addition to `account_number`, that works pretty well as an AID column. For example, `email_address` or `phone_number`. If the individual with multiple accounts used the same `email_address`, then the bin would be suppressed. Of course, it is possible that the individual used a different email for each account, in which case this fix wouldn't help. As a general rule, labeling multiple AID columns for the same protected entity, where each AID column is quite good but not perfect, leads to slightly stronger anonymity and only slightly poorer analytic utility.

__Generalize the isolating column.__ Some isolating columns may have analytic value, for instance `street_address` denotes location. If `street_address` was generalized to `zip_code`, then some analytic utility is retained while preventing the specific privacy problem. As a general rule, increased generalization, so long as it does not hurt analytic utility, is good practice.

__Increase the suppression threshold.__ If it is known that more than, for instance, five accounts for a given individual is extremely rare, then by setting the suppression threshold to six (`pg_diffix.low_count_min_threshold`), then the problem is almost completely mitigated. This is an effective approach, but increases suppression overall.

### Multiple instances of the same type of protected entity

Tables that convey relationships or interactions between individuals have multiple instances of the same type of protected entity as identified by the same type of identifier. For instance, a table with banking transactions can have a `send_account` and `receive_account`. If the protected entity is account, then both `send_account` and `receive_account` must be labeled as AID columns.

Other examples include `send_email` and `receive_email`, `player1` and `player2` (in a sporting match with two players), and `friend1` and `friend2` (in a social network).

### Multiple different types of protected entities

Some tables may contain different types of protected entities.

One example is where the protected entities are all persons, but with different roles. For instance `doctor_id` and `patient_id`, or `customer_id` and `salesperson_id`.

Another example is when there are groups of people that should be protected. Typical examples are families (which can be indirectly coded as for instance a street address) or couples (i.e. a joint bank account).

In other cases, one of the protected entities may not be a person at all. In particular, a company may wish to protect certain proprietary information. An example here might be a company with many local stores, which doesn't want to reveal information about individual store activity. In this case, the AID columns might be `customer_id` and `store_branch_id`.

Note that analytic utility can be substantially degraded when a protected entity has relatively few distinct instances in the table (a _sparse_ protected entity). In some cases it may make more sense to build a table with the sparse protected entity removed altogether. Finally, note that if a sparse protected entity is removed, then there may be other columns that are strongly correlated with the protected entity that should also be removed. For instance, if there is a `transaction_zip_code` column, and most zip codes have no more than one store, then the `transaction_zip_code` column should be removed along with `store_branch_id`.

## How to set suppression threshold

The purpose of suppression in Diffix Elm is to prevent the release of information pertaining to a single individual. In the GDPR, this is called _singling out_. Narrowly construed, releasing information pertaining to two individuals is not singling out, and so GDPR is not violated. Practically speaking, however, releasing information about two people or even four or five people might be regarded as a privacy violation, especially if the people are closely related (a couple or family).

When selecting a suppression threshold (`pg_diffix.low_count_min_threshold`), there are four main considerations:

1. What is the largest threshold that satisfies analytic goals? There is no reason to have a smaller threshold than that which satisfies analytic goals.
2. What is the largest group of individuals that need to be protected? In other words, what is the largest group whereby the release of information about the group can be interpreted as equivalent to the release of information about an individual in the group?
3. Are there imperfections in the AID that need to be covered by a larger suppression threshold (see [previous section](how-to-select-aid-columns)).
4. What is the public perception of how large an aggregate should be? Public opinion is an important consideration. If for instance the public would be nervous about aggregates of five individuals, even though strictly speaking individual privacy is protected, then setting the threshold to a larger value may make sense.
