# Important notice

This is a pre-release version of the extension and is not intended for general use yet.
It may be unstable and documentation is limited.
If you have any questions, please contact us at [hello@open-diffix.org](mailto:hello@open-diffix.org).

# PG Diffix

`pg_diffix` is a PostgreSQL extension for strong dynamic anonymization. It ensures that answers to simple SQL queries are anonymous. For more information, visit the [Open Diffix](https://www.open-diffix.org/) website.

Check out the [Admin Tutorial](docs/admin_tutorial.md) for an example on how to set up `pg_diffix`.
See the [Admin Guide](docs/user_guide.md) for details on configuring and using the extension.

## Installation

PostgreSQL version 13 or higher is required. You also need `make`, `jq`, and a recent C compiler.
You should already have the `postgresql-server-dev-x` package installed if you have PostgreSQL version `x`.
If not, you must install it in order to compile the source.

The source is compiled with: `make` (or `make TARGET=release` for release version).
The compiled extension is installed with: `make install` (which requires superuser permissions).

The extension is also available on [PGXN](https://pgxn.org/dist/pg_diffix/), and can be installed using
[PGXN Client](https://pgxn.github.io/pgxnclient/).

## Activating the extension

You can set up the extension for the current database by using the command `CREATE EXTENSION pg_diffix;`.

To properly enforce the anonymization restrictions, the extension has to be automatically loaded on
every session start for restricted users. This can be accomplished by configuring
[library preloading](https://www.postgresql.org/docs/current/runtime-config-client.html#RUNTIME-CONFIG-CLIENT-PRELOAD).

For example, to automatically load the `pg_diffix` extension for all users connecting to a database,
you can execute the following command:

`ALTER DATABASE db_name SET session_preload_libraries TO 'pg_diffix';`

Once loaded, the extension logs information to `/var/log/postgresql/postgresql-13-main.log` or equivalent.

Node dumps can be formatted to readable form by using `pg_node_formatter`.

## Deactivating the extension

You can drop the extension from the current database by using the command `DROP EXTENSION pg_diffix;`.

You might also need to remove the extension from the list of preloaded libraries.

For example, to reset the list of preloaded libraries for a database, you can execute the following command:

`ALTER DATABASE db_name SET session_preload_libraries TO DEFAULT;`

## Testing the extension

### `make installcheck`

Once you have a running server with the extension installed, execute `make installcheck` to run the tests.
You must ensure you have all the required permissions for this to succeed, for example:

1. In your `pg_hba.conf` your PostgreSQL superuser to have `trust` authentication `METHOD`.
   If modified, run `systemctl restart postgresql.service` to apply changes.
2. Invoke using `PGUSER=<postgres-superuser> make installcheck`

or if available, just make your usual PostgreSQL user a `SUPERUSER`.

### `PGXN Test Tools`

Or you can use the [PGXN Extension Build and Test Tools](https://github.com/pgxn/docker-pgxn-tools) Docker image:

`docker run -it --rm --mount "type=bind,src=$(pwd),dst=/repo" pgxn/pgxn-tools sh -c 'cd /repo && apt update && apt install -y jq && pg-start 13 && pg-build-test'`.

## Docker images

We provide 2 Docker images preconfigured with the extension.

### Base image

The base image is a standard `postgres` image with `pg_diffix` installed and preloaded.
It does not include any additional database or user out of the box.

The example below shows how to build the image and run a minimally configured container.

Build the image:

`make image`

Run the container in foreground and expose in port 10432:

`docker run --rm --name pg_diffix -e POSTGRES_PASSWORD=postgres -p 10432:5432 pg_diffix`

From another shell you can connect to the container via `psql`:

`psql -h localhost -p 10432 -d postgres -U postgres`

For more advanced usage see the [official image reference](https://hub.docker.com/_/postgres).

### Demo image

The demo image extends the base image with a sample dataset and a user for each access level.

Once started, the container creates and populates the `banking` database.
Three users are created, all of them with password `demo`:
  - `trusted_user` with anonymized access to `banking` in trusted mode
  - `untrusted_user` with anonymized access to `banking` in untrusted mode
  - `direct_user` with direct (non-anonymized) access to `banking`

**NOTE** The required file `docker/demo/01-banking-data.sql` is managed by [Git LFS](https://git-lfs.github.com).

Build the image:

`make demo-image`

Run the container in foreground and expose in port 10432:

`docker run --rm --name pg_diffix_demo -e POSTGRES_PASSWORD=postgres -e BANKING_PASSWORD=demo -p 10432:5432 pg_diffix_demo`

Connect to the banking database (from another shell) for anonymized access:

`psql -h localhost -p 10432 -d banking -U trusted_user`

To keep the container running you can start it in detached mode and with a restart policy:

`docker run -d --name pg_diffix_demo --restart unless-stopped -e POSTGRES_PASSWORD=postgres -e BANKING_PASSWORD=demo -p 10432:5432 pg_diffix_demo`
