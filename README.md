# PG Diffix

`pg_diffix` is a PostgreSQL extension for strong dynamic anonymization. It enables you to query your PostgreSQL
database (nearly) as you’re used to, but makes sure you receive fully anonymous output.
For details, visit the [Open Diffix](https://www.open-diffix.org/) website.

This extension requires a PostgreSQL server version of 13 or higher.

## Installation

PostgreSQL version > 13 is required.

The source is compiled with:

```sh
$ make
# or `make TARGET=release` for release version
```

You should already have the `postgresql-server-dev-x` package installed if you have postgres version `x`.
If not, you must install it in order to compile the source.

The compiled extension is installed with:

```sh
$ make install
```

You probably need to run it with superuser permission as `sudo make install`.

In `psql`, you have to install the extension with `CREATE EXTENSION pg_diffix;`.

## Using the extension

Load the extension with `LOAD 'pg_diffix';`, unless you configured it to preload using [these instructions](#preloading-the-extension).

Once installed, the extension logs information to `/var/log/postgresql/postgresql-13-main.log` or equivalent.

Node dumps can be formatted to readable form by using `pg_node_formatter`.

## Preloading the extension

To enable automatic activation on every session start, you need to configure [library preloading](https://www.postgresql.org/docs/13/runtime-config-client.html#RUNTIME-CONFIG-CLIENT-PRELOAD).

In your `postgresql.conf` file, add `pg_diffix` to either of `session_preload_libraries` or `shared_preload_libraries`.

```
session_preload_libraries = 'pg_diffix'
```

If you have multiple libraries you want to preload, separate them with commas.

## Testing the extension

### `make installcheck`

Once you have a running server with the extension installed, execute `make installcheck` to run the tests. You must ensure you have all the required permissions for this to succeed, for example:

1. In your `pg_hba.conf` your PostgreSQL superuser to have `trust` authentication `METHOD`. `systemctl restart postgresql.service` in case you needed to modify
2. Invoke using `PGUSER=<postgres-superuser> make installcheck`

or if available, just make your usual PostgreSQL user a `SUPERUSER`.

### `PGXN Test Tools`

Or you can use the [PGXN Extension Build and Test Tools](https://github.com/pgxn/docker-pgxn-tools) Docker image:
```sh
$ docker run -it --rm --mount "type=bind,src=$(pwd),dst=/repo" pgxn/pgxn-tools \
    sh -c 'cd /repo && pg-start 13 && pg-build-test'
```

## Docker images

We provide 2 Docker images preconfigured with the extension.

### Base image

The base image is a standard `postgres` image with `pg_diffix` installed and preloaded.
It does not include any additional database or user out of the box.

The example below shows how to build the image and run a minimally configured container.

```sh
# Build the image
$ make image

# Run the container in foreground and expose in port 10432
$ docker run --rm --name pg_diffix -e POSTGRES_PASSWORD=postgres -p 10432:5432 pg_diffix
```

From another shell you can connect to the container via `psql`:

```sh
psql -h localhost -p 10432 -d postgres -U postgres
```

For more advanced usage see the [official image reference](https://hub.docker.com/_/postgres).

### Demo image

The demo image extends the base image with a sample dataset and a user configured with `publish_trusted` access.

Once started, the container creates and populates the `banking` database.
Two users are created, with password `demo`:
  - `banking_publish` with anonymized access to `banking`
  - `banking` with direct (non-anonymized) access to `banking`

**Note:** The required file `docker/demo/01-banking-data.sql` is managed by [Git LFS](https://git-lfs.github.com).

```sh
# Build the image
$ make demo-image

# Run the container in foreground and expose in port 10432
$ docker run --rm --name pg_diffix_demo -e POSTGRES_PASSWORD=postgres -e BANKING_PASSWORD=demo -p 10432:5432 pg_diffix_demo

# Connect to the banking database (from another shell) for anonymized access
$ psql -h localhost -p 10432 -d banking -U banking_publish
```

To keep the container running you can start it in detached mode and with a restart policy:

```sh
$ docker run -d --name pg_diffix_demo --restart unless-stopped -e POSTGRES_PASSWORD=postgres -e BANKING_PASSWORD=demo -p 10432:5432 pg_diffix_demo
```

## Configuring the extension

System behaviour can be configured using a combination of custom variables and security labels.

### Labeling database objects

The module acts as a security provider and allows the marking of database objects with various anonymization labels,
which associate additional metadata, needed for anonymization, with existing objects. For more details about security
labels, see the official [documentation page](https://www.postgresql.org/docs/current/sql-security-label.html).

Only superusers can set anonymization labels.
To remove an anonymization label from an object, set it to `NULL`.

Execute `SELECT * FROM diffix.show_labels();` to display the current labels in use by the extension.

Tables, schemas and databases can be labeled as `public` or `sensitive`. Direct access is allowed to public data
even for restricted users.

If a table targeted by a query is unlabeled, its schema label is checked. If the schema is also unlabeled, the
database label is checked. If no label was found during this process, the table's data is presumed to be public.
This provides the necessary flexibility to implement various data access policies.

```sql
SECURITY LABEL FOR pg_diffix ON TABLE my_table IS 'sensitive';
SECURITY LABEL FOR pg_diffix ON SCHEMA my_schema IS 'sensitive';
SECURITY LABEL FOR pg_diffix ON TABLE my_schema.table IS 'public';
```

Anonymization ID (AID) columns for a sensitive table have to be marked with the anonymization label `aid`. A sensitive
table can have zero, one or more AID columns.

```SQL
SECURITY LABEL FOR pg_diffix ON COLUMN my_table.id IS 'aid';
```

Regular users can be marked with the anoymization labels `direct`, `publish_trusted` or `publish_untrusted`. The value of the custom variable `pg_diffix.default_access_level`
determines the access level for unlabeled regular users.

```SQL
SECURITY LABEL FOR pg_diffix ON ROLE analyst IS 'publish_trusted';
```

### Restricted features and extensions

At access levels other than `direct`, various data and features built into PostgreSQL are restricted, as they might leak
sensitive information. Among others, the following is not allowed:

1. Issue utility statements like `COPY` and `ALTER TABLE`, beside a few allowlisted ones.
2. Access data in the `pg_catalog` schema like `pg_stats`, `pg_user_functions`, or `pg_class`.
3. Access a selected subset of less frequently used PostgreSQL query features like `EXISTS` OR `NULLIF`.
4. Inheritance involving a sensitive table.

Most of these restrictions do not apply to superusers.

**NOTE** If any of the currently blocked features is necessary for your use case, open an issue and let us know.

Row level security (RLS) can be enabled and used on sensitive tables. It is advised that the active policies are vetted from the point of view of anonymity.

It is also strongly advised to vet any other extensions which are enabled alongside `pg_diffix`, as well as any user-defined functions and aggregate functions.

### System settings

The module exposes a bunch of custom variables, under the `pg_diffix` prefix, that can be set in the configuration file
to control the system behaviour for all users. Superusers can change these variables at run-time for their own session,
while regular users only have read access to them (with few notable exceptions).

Execute `SELECT * FROM diffix.show_settings();` to display the current settings of the extension. **NOTE** if the result is empty, make sure [`pg_diffix` is loaded](#using-the-extension).

#### Data access settings

`pg_diffix.default_access_level` - Determines the access level for unlabeled users; default value is `direct`.

`pg_diffix.session_access_level` - Sets the access level for the current session; it can never be higher than the access
level for the current user; can be changed by all users; defaults to maximum access level allowed.

#### Noise settings

`pg_diffix.salt` - Secret value that influences noise generation; needs to be set by the system administrator in
the configuration file; can't be read by regular users.

`pg_diffix.noise_layer_sd` - Standard deviation for each noise layer added to aggregates. Default value is 1.0.

#### Low count filter settings

`pg_diffix.low_count_min_threshold` - The lower bound for the number of distinct AID values that must be present in a
bucket for it to pass the low count filter. Default value is 2.

`pg_diffix.low_count_mean_gap` - The number of standard deviations between the lower bound and the mean of the
low count filter threshold. Default value is 2.0.

`pg_diffix.low_count_layer_sd` - The standard deviation for each noise layer used when calculating the low count filter
threshold. Default value is 1.0.

`pg_diffix.compute_suppress_bin` - If `True`, the first row in the query result might contain the suppress bin, which
provides the combined anonymized count of all the bins that were suppressed (low count filtered). The suppress bin shows
all column values as `NULL` (`*` for text-typed columns, customizable via `pg_diffix.text_label_for_suppress_bin`). Note
that the suppress bin may itself be suppressed.

`pg_diffix.text_label_for_suppress_bin` - The value to use for the text-typed grouping labels in the suppress bin row.
Default value is `*`.

#### Aggregation settings

`pg_diffix.outlier_count_min` - Default value is 1. Must not be greater than `outlier_count_max`.

`pg_diffix.outlier_count_max` - Default value is 2. Must not be smaller than `outlier_count_min`.

`pg_diffix.top_count_min` - Default value is 4. Must not be greater than `top_count_max`.

`pg_diffix.top_count_max` - Default value is 6. Must not be smaller than `top_count_min`.

**NOTE** The outlier interval `(outlier_count_min, outlier_count_max)` must not be wider than the `(top_count_min, top_count_max)` interval.
