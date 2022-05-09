# Access levels

Users can have one of the following access levels to a database:

- `direct` - Direct (non-anonymized) access to data. Restrictions listed in this document do not apply in direct mode.
- `anonymized_trusted` - Anonymized access to data. Prevents accidental release of personal data.
- `anonymized_untrusted` - Anonymized access to data. Prevents intentional release of personal data.

Use `SELECT diffix.access_level()` to see the current access level.

# Anonymized queries

In anonymized access level, queries targeting personal tables are restricted to a limited subset of SQL.
A personal table is a relation that contains data of individuals or other protected entities.
Administrators identify and mark such tables during configuration.

When selecting data from personal tables, the following queries are allowed.

## Queries with grouping

Grouping queries have the following form:

```
SELECT col1, col2, ..., count(...)
FROM personal_table
GROUP BY col1, col2, ...
```

Where `col1`, `col2`, ... `colX` are table columns.
Numeric and string columns can optionally be generalized.

`count()` is any of the supported [count aggregate](#count) variants.

**Example:**

```
SELECT city, last_name, count(*)
FROM customers
GROUP BY city, last_name
```

## Globally aggregating queries

If no grouping columns are specified, the entire table is aggregated into a single bin.
This bin is never suppressed, even if no rows exist in the table.

```
SELECT count(...), ...
FROM personal_table
```

**Example:**

```
SELECT count(*), count(DISTINCT city)
FROM customers
```

## Queries with implicit grouping

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
SELECT city, last_name
FROM customers
```

# Unrestricted queries

Queries against personal tables are limited to the SQL subset
described in the [anonymized queries](#anonymized-queries) section.
However, queries that do not target personal tables are unrestricted, meaning any SQL can be executed.
This includes queries that select from the output of anonymizing subqueries
as their output is no longer considered personal.

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

If `pg_diffix.treat_unmarked_tables_as_public` is set to `false`. then tables not marked as `personal` or `public` are not queryable in any way.
Use [diffix.show_labels()](#diffixshowlabels) to see which tables are marked.

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
The suppress bin is the first row of the query result. The suppress bin may itself be suppressed.
All grouping columns of the suppress bin are set to `*` for text-typed columns and `NULL` for other types.

The configuration parameter `pg_diffix.compute_suppress_bin` controls whether the suppress bin is computed and emitted.

The configuration parameter `pg_diffix.text_label_for_suppress_bin` specifies the value to use for text-typed grouping labels.
Default value is `*`.

The aggregate `diffix.is_suppress_bin(*)` returns `true` for the suppress bin, and false for any other bin.
This can be used to identify the suppress bin if `NULL` values are ambiguous for a grouping label.

# Supported functions

## Count

The following versions of the count aggregate are supported:

- `count(*)` - count all rows.
- `count(col)` - counts non-null occurrences of the given column.
- `count(distinct col)` - counts distinct values of the given column.
- `count(distinct aid)` - counts number of individuals in the bin.

Results of these aggregates are anonymized by applying noise as described in the specification.

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

## Utility functions

### diffix.is_suppress_bin(*)

Aggregate that returns `true` only for the suppress bin, `false` otherwise.
