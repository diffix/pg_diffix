# PG Diffix

## Installation

The source is compiled with:

```sh
$ make
```

You should already have the `postgresql-server-dev-x` package installed if you have postgres version `x`.
If not, you must install it in order to compile the source.

The compiled extension is installed with:

```sh
$ make install
```

You probably need to run it with superuser permission as `sudo make install`.

In `psql`, you have to install the extension with `create extension pg_diffix;`.

To enable automatic activation you need to configure [shared library preloading](https://www.postgresql.org/docs/13/runtime-config-client.html#RUNTIME-CONFIG-CLIENT-PRELOAD).

## Using the extension

Once installed, the extension logs information to `/var/log/postgresql/postgresql-13-main.log` or equivalent.

Node dumps can be formatted to readable form by using `pg_node_formatter`.