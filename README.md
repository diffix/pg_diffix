# PG Diffix

`pg_diffix` is a PostgreSQL extension for strong dynamic anonymization. It enables you to query your PostgreSQL
database (nearly) as you’re used to, but makes sure you receive fully anonymous output.
For details, visit the [Open Diffix](https://www.open-diffix.org/) website.

Check out the [tutorial](docs/tutorial.md) for an example on how to use `pg_diffix`.
For detailed information on configuring and using the extension, check out the [user guide](docs/user_guide.md).

## Installation

PostgreSQL version 13 or higher is required.

The source is compiled with: `make` or `make TARGET=release` for release version.

You should already have the `postgresql-server-dev-x` package installed if you have postgres version `x`.
If not, you must install it in order to compile the source.

The compiled extension is installed with: `make install`.

You probably need to run it with superuser permission as `sudo make install`.

In `psql`, you have to install the extension with `CREATE EXTENSION pg_diffix;`.

## Using the extension

Load the extension with `LOAD 'pg_diffix';`, unless you configured it to preload using [these instructions](#preloading-the-extension).

Once installed, the extension logs information to `/var/log/postgresql/postgresql-13-main.log` or equivalent.

Node dumps can be formatted to readable form by using `pg_node_formatter`.

## Preloading the extension

To enable automatic activation on every session start, you need to configure
[library preloading](https://www.postgresql.org/docs/13/runtime-config-client.html#RUNTIME-CONFIG-CLIENT-PRELOAD).

In your `postgresql.conf` file, add `pg_diffix` to either of `session_preload_libraries` or `shared_preload_libraries`.

`session_preload_libraries = 'pg_diffix'`

If you have multiple libraries you want to preload, separate them with commas.

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
`docker run -it --rm --mount "type=bind,src=$(pwd),dst=/repo" pgxn/pgxn-tools \
  sh -c 'cd /repo && pg-start 13 && pg-build-test'`.

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

The demo image extends the base image with a sample dataset and a user configured with `publish_trusted` access.

Once started, the container creates and populates the `banking` database.
Three users are created, with password `demo`:
  - `banking` with direct (non-anonymized) access to `banking`
  - `banking_trusted` with anonymized access to `banking` in trusted mode
  - `banking_untrusted` with anonymized access to `banking` untrusted mode

**NOTE** The required file `docker/demo/01-banking-data.sql` is managed by [Git LFS](https://git-lfs.github.com).

Build the image:

`make demo-image`

Run the container in foreground and expose in port 10432:

`docker run --rm --name pg_diffix_demo -e POSTGRES_PASSWORD=postgres -e BANKING_PASSWORD=demo -p 10432:5432 pg_diffix_demo`

Connect to the banking database (from another shell) for anonymized access:

`psql -h localhost -p 10432 -d banking -U banking_trusted`

To keep the container running you can start it in detached mode and with a restart policy:

`docker run -d --name pg_diffix_demo --restart unless-stopped -e POSTGRES_PASSWORD=postgres \
  -e BANKING_PASSWORD=demo -p 10432:5432 pg_diffix_demo`
