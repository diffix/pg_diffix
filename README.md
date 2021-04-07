# PG Diffix

## Installation

The source is compiled with:

```sh
$ make
# or `make DEBUG=yes` for debug logs
```

You should already have the `postgresql-server-dev-x` package installed if you have postgres version `x`.
If not, you must install it in order to compile the source.

The compiled extension is installed with:

```sh
$ make install
```

You probably need to run it with superuser permission as `sudo make install`.

In `psql`, you have to install the extension with `create extension pg_diffix;`.

## Using the extension

Once installed, the extension logs information to `/var/log/postgresql/postgresql-13-main.log` or equivalent.

Node dumps can be formatted to readable form by using `pg_node_formatter`.

## Preloading the extension

To enable automatic activation you need to configure [shared library preloading](https://www.postgresql.org/docs/13/runtime-config-client.html#RUNTIME-CONFIG-CLIENT-PRELOAD).

In your `postgresql.conf` file, add `pg_diffix` to `session_preload_libraries`.

```
session_preload_libraries = 'pg_diffix'
```

If you have multiple libraries you want to preload, separate them with commas.

## Testing the extension

Once you have a running server with the extension installed, execute `make installcheck` to run the tests.

## Running Docker image

We provide a Docker image preconfigured with the extension.

The example below shows how to build the image and run a minimally configured container.

```sh
# Build the image
$ docker build -t pg_diffix .

# Run the container in foreground and expose in port 10432
$ docker run --rm --name pg_diffix -e POSTGRES_PASSWORD=postgres -p 10432:5432 pg_diffix
```

From another shell you can connect to the container via `psql`:

```sh
psql -h localhost -p 10432 -d postgres -U postgres
```

For more advanced usage see the [official image reference](https://hub.docker.com/_/postgres).
