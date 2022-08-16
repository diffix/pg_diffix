# Analyst guide

This document describes features and restrictions of `pg_diffix` for users with anonymized access to a database.
The [banking notebook](banking.ipynb) provides a walkthrough with examples and explanations of various
mechanisms that Diffix uses to protect personal data.

## Table of Contents

- [Access levels](#access-levels)
- [Anonymizing queries](#anonymizing-queries)
  - [Explicit grouping](#explicit-grouping)
  - [Implicit grouping](#implicit-grouping)
  - [Input filtering](#input-filtering)
- [Unrestricted queries](#unrestricted-queries)
- [Post processing](#post-processing)
- [Utility statements](#utility-statements)
  - [Other SQL statements](#other-sql-statements)
  - [diffix.show_labels()](#diffixshow_labels)
  - [diffix.access_level()](#diffixaccess_level)
- [Suppress bin](#suppress-bin)
- [Supported functions](#supported-functions)
  - [Aggregates](#aggregates)
    - [diffix.count_histogram(aid, bin_size)](#diffixcount_histogramaid-bin_size)
  - [Numeric generalization functions](#numeric-generalization-functions)
    - [diffix.floor_by(col, K)](#diffixfloor_bycol-k)
    - [diffix.round_by(col, K)](#diffixround_bycol-k)
    - [diffix.ceil_by(col, K)](#diffixceil_bycol-k)
    - [width_bucket(operand, low, high, count)`](#width_bucketoperand-low-high-count)
  - [String generalization functions](#string-generalization-functions)
    - [substring(text_column, start, count)](#substringtext_column-start-count)
  - [Type casts](#type-casts)
  - [Utility functions](#utility-functions)
    - [diffix.is_suppress_bin(*)](#diffixis_suppress_bin)
    - [diffix.unnest_histogram(histogram)](#diffixunnest_histogramhistogram)

# Access levels

Users can have one of the following access levels to a database:

- `direct` - Direct (non-anonymized) access to data. Restrictions listed in this document do not apply in direct mode.
- `anonymized_trusted` - Anonymized access to data. Prevents accidental release of personal data.
- `anonymized_untrusted` - Anonymized access to data. Prevents intentional release of personal data.

Use `SELECT diffix.access_level()` to see the current access level.

# Anonymizing queries

In anonymized access level, queries targeting personal tables are restricted to a limited subset of SQL.
A personal table is a relation that contains data of individuals or other protected entities.
Administrators identify and mark such tables during configuration.

When selecting data from personal tables, the following SQL features are allowed:

## Explicit grouping

Grouping queries have the following form:

```
SELECT col1, col2, ..., count(...)
FROM personal_table
GROUP BY col1, col2, ...
```

Zero or more table columns `col1`, `col2`, ... may be specified.
[Numeric](#numeric-generalization-functions) and [string](#string-generalization-functions) columns may optionally be generalized.

`count(...)` is any of the supported [aggregate](#aggregates) variants.
Any number of aggregates may be specified (including none).

**Example:**

```
SELECT city, year_of_birth, count(*), diffix.count_noise(*)
FROM customers
GROUP BY city, year_of_birth
```

**Example:**

```
SELECT count(*), count(DISTINCT city)
FROM customers
```

## Implicit grouping

The `GROUP BY` clause may be omitted:

```
SELECT col1, col2, ..., colX
FROM personal_table
```

In this case, the results are grouped implicitly by the selected columns.
Bins with insufficient individuals are filtered out, and the remaining
bins are repeated for (anonymized) `count(*)` times.

Semantically this is equivalent to:

```
SELECT col1, col2, ..., colX, generate_series(1, anon_count)
FROM (
  SELECT col1, col2, ..., colX, count(*) AS anon_count
  FROM personal_table
  GROUP BY col1, col2, ..., colX
) x
```

**Example:**

```
SELECT city, year_of_birth
FROM customers
```

## Input filtering

The `WHERE` clause may specify one or more simple equality conditions which all have to match simultaneously for a row to be included in the analysis.

A simple equality condition consists of an equality between a raw or generalized column, on the left side, and a constant value, on the right side.

Combining conditions using `OR` or specifying oher types of conditions is not permitted.

**Example:**

```
SELECT count(*)
FROM customers
WHERE city = 'Berlin' AND gender = 'F'
```

**Example:**

```
SELECT city, count(*)
FROM customers
WHERE substring(date_of_birth, 1, 4) = 2000
GROUP BY city
```

AID columns are not allowed in filtering conditions.
Administrators can additionally restrict the usage of certain columns for data filtering by untrusted users.

# Unrestricted queries

Queries against personal tables are limited to the SQL subset described in the [anonymized queries](#anonymized-queries) section.
However, queries that do not target personal tables are unrestricted, meaning any SQL can be executed.

If `pg_diffix.treat_unmarked_tables_as_public` is set to `false`, then tables not marked as `personal` or `public` are not queryable in any way.
Use [diffix.show_labels()](#diffixshow_labels) to see which tables are marked as `personal` or `public`.

# Post processing

Any SQL may be executed on the output of anonymizing subqueries as their output is no longer considered personal.

**Example:** The following query selects the average number of individuals in each city.

```
SELECT avg(x.num_individuals)
FROM (
  SELECT city, count(*) as num_individuals
  FROM customers
  GROUP BY city
) x
```

Projections of grouping expressions in `SELECT` and the `HAVING` clause of anonymizing
queries are also considered post processing and are accepted.

**Example:** The following query post processes the grouping labels and filters the output bins.

```
SELECT 'City: ' || upper(city), count(*)
FROM customers
GROUP BY city
HAVING count(*) > 10
```

# Utility statements

## Other SQL statements

Most utility statements are disallowed in anonymized access level.
Allowed SQL statements include:

- `EXPLAIN` - explain is partially allowed. Options `COSTS` and `ANALYZE` are rejected.
- `SET` - a subset of parameters can be changed by non-superusers, even in anonymized mode.
  Note that most `pg_diffix` parameters require superuser access.

Any statement that alters the database (`INSERT`, `DELETE`, `UPDATE`, `DROP`, ...) or causes any other persistent state change is forbidden.

`COPY` statements and other indirect methods of reading data from the database are forbidden.

## diffix.show_labels()

Run `SELECT * FROM diffix.show_labels()` to see the complete list of labels on database objects.

## diffix.access_level()

Run `SELECT diffix.access_level()` to see the current [access level](#access-levels) of the session.
Possible values are `direct`, `anonymized_trusted`, and `anonymized_untrusted`.

# Suppress bin

When performing grouping in queries that target personal tables, bins that pertain to too few individuals are suppressed.

Diffix combines the counts of all suppressed rows to a special **suppress bin**.
The suppress bin is the first row of the anonymized query result, unless sorted or [post processed](#post-processing) by other means.
The suppress bin may itself be suppressed.
All grouping columns of the suppress bin are set to `*` for text-typed columns and `NULL` for other types.

The configuration parameter `pg_diffix.compute_suppress_bin` controls whether the suppress bin is computed and emitted.

The configuration parameter `pg_diffix.text_label_for_suppress_bin` specifies the value to use for text-typed grouping labels.
Default value is `*`.

The aggregate `diffix.is_suppress_bin(*)` returns `true` for the suppress bin, and false for any other bin.
This can be used to identify the suppress bin if `NULL` values are ambiguous for a grouping label.

# Supported functions

## Aggregates

The following versions of aggregates are supported:

- `count(*)` - count all rows.
- `count(col)` - counts non-null occurrences of the given column.
- `count(distinct col)` - counts distinct values of the given column.
- `sum(col)` - sums values in the given column.
- `avg(col)` - calculates the average of the given column.
- `diffix.count_histogram(aid, bin_size=1)` - computes a histogram that describes the distribution of rows among entities.
  See [below](#diffixcount_histogramaid-bin_size) for details.

Results of these aggregates are anonymized by applying noise as described in the specification.

Each of the `count(...)`, `sum(...)`, `avg(...)` has an accompanying aggregate,
which returns the approximate magnitude of noise added during anonymization (in terms of its standard deviation).
These are: `diffix.count_noise(...)`, `diffix.sum_noise(...)`, `diffix.avg_noise(...)`, respectively.

### diffix.count_histogram(aid, bin_size)

Returns a 2-dimensional array of shape `bigint[][2]`, where each entry is a pair of `[row_count, num_entities]`.
The `row_count` represents the number of rows contributed by `num_entities` distinct protected entities.

**Example:**

```
SELECT diffix.count_histogram(account)
FROM transactions;

        count_histogram
--------------------------------
 {{NULL,7},{1,15},{2,13},{4,6}}
(1 row)
```

The result of the above query can be interpreted as:
15 accounts have made a single transaction (1 row in result bucket), 13 accounts have made 2 transactions (2 rows),
6 accounts have made 4 transactions, and 7 accounts have made some other number of transactions (identified by the `NULL` count).

The reported `num_entities` is a noisy value, but not the `row_count` itself. Bins with insufficient `num_entities` are merged to
a suppress bin of shape `{NULL, num_entities}` where `num_entities` is also noisy. The suppress bin may itself be suppressed.

The optional `bin_size` parameter allows generalizing the bins' `row_count` to minimize suppression.
It acts identically to the `diffix.floor_by()` function.

The histogram array can be unwrapped to a set of pairs by using [diffix.unnest_histogram()](#diffixunnest_histogramhistogram).

**Restrictions:** The `aid` parameter must be a reference to a column tagged as an AID (identifier of a protected entity).

In untrusted mode, `bin_size` is restricted to a money style number:
1, 2, or 5 preceeded by or followed by zeros ⟨... 0.1, 0.2, 0.5, 1, 2, 5, 10, ...⟩.

## Numeric generalization functions

### diffix.floor_by(col, K)

Rounds column to width `K` and aligns to the lower edge of the interval. Equivalent to `floor(col / K) * K`.

**Restrictions:** In untrusted mode, `K` is restricted to a money style number:
1, 2, or 5 preceeded by or followed by zeros ⟨... 0.1, 0.2, 0.5, 1, 2, 5, 10, ...⟩.

### diffix.round_by(col, K)

Rounds column to width `K` and aligns to the closest edge of the interval. Equivalent to `round(col / K) * K`.

**Restrictions:** Not allowed in untrusted mode.

### diffix.ceil_by(col, K)

Rounds column to width `K` and aligns to the upper edge of the interval. Equivalent to `ceil(col / K) * K`.

**Restrictions:** Not allowed in untrusted mode.

### width_bucket(operand, low, high, count)`

Default [Postgres function](https://www.postgresql.org/docs/14/functions-math.html) that returns the
bucket number of the column value in an equal-width histogram.

**Restrictions:** Not allowed in untrusted mode.

## String generalization functions

### substring(text_column, start, count)

Default [Postgres function](https://www.postgresql.org/docs/14/functions-string.html).

**Restrictions:** In untrusted mode, only `start = 1` is allowed.

## Type casts

When selecting or generalizing columns of personal tables, the following type conversions are allowed:

- All numeric types may be converted to other numeric types.
  Numeric types are: `smallint`, `integer`, `bigint`, `float`, `double`, and `numeric` (`decimal`).
  When converting from a real type to an integer type, implicit rounding occurs (away from zero).
  Such casts count as generalizing expressions and no further generalizations are allowed.
- Date/time types may be converted to text.
  Date/time types are: `date`, `time`, `timestamp`, `timetz`, and `timestamptz`.
  The [DateStyle](https://www.postgresql.org/docs/14/runtime-config-client.html#GUC-DATESTYLE)
  configuration parameter determines the output format of the string.

**Example:**

```
SELECT substring(date_of_birth::text, 1, 4) AS year, count(*)
FROM customers
GROUP BY 1
```

## Utility functions

### diffix.is_suppress_bin(*)

Aggregate that returns `true` only for the suppress bin, `false` otherwise.

### diffix.unnest_histogram(histogram)

Unnests a 2-dimensional array into a result set of 1-dimensional arrays.

**Example:**

```
SELECT diffix.unnest_histogram(diffix.count_histogram(account)) AS bins
FROM transactions;

   bins
----------
 {NULL,7}
 {1,15}
 {2,13}
 {4,6}
(4 rows)
```
