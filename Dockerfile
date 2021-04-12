FROM postgres:13 AS builder

RUN set -ex \
  && apt-get update \
  && apt-get install -y build-essential postgresql-server-dev-13 \
  && mkdir -p /usr/src/pg_diffix

WORKDIR /usr/src/pg_diffix
COPY . .
RUN make && make install

FROM postgres:13 as pg_diffix

# Hint that session_preload_libraries should have 'pg_diffix'.
# This will get copied to the default config on initialization.
#
# Users can (and likely will) provide custom configuration files,
# in which case they need to be aware that this parameter is required
# for the extension to function properly.
RUN sed -i \
  "/session_preload_libraries/c\session_preload_libraries = 'pg_diffix'" \
  /usr/share/postgresql/postgresql.conf.sample

# Runs CREATE EXTENSION in POSTGRES_DB.
COPY docker/0* /docker-entrypoint-initdb.d/

# Copy artifacts from builder.
# If LLVM is enabled, there will be other files that need to be copied.
COPY --from=builder /usr/lib/postgresql/13/lib/pg_diffix.so /usr/lib/postgresql/13/lib/pg_diffix.so
COPY --from=builder /usr/share/postgresql/13/extension/pg_diffix* /usr/share/postgresql/13/extension/

FROM pg_diffix as pg_diffix_demo

# Copy dataset SQL files.
COPY docker/1* /docker-entrypoint-initdb.d/
COPY docker/demo /docker-entrypoint-initdb.d/demo
