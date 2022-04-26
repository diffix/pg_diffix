#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE DATABASE banking;
EOSQL

function sql {
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "banking" "$@"
}

sql -c "CREATE EXTENSION pg_diffix"

sql -f "/docker-entrypoint-initdb.d/demo/00-banking-schema.sql" \
    -f "/docker-entrypoint-initdb.d/demo/01-banking-data.sql" \
    -f "/docker-entrypoint-initdb.d/demo/02-banking-constraints.sql"

banking_password=${BANKING_PASSWORD:-demo}

sql -c "CREATE USER direct_user WITH PASSWORD '$banking_password';"
sql -c "CREATE USER trusted_user WITH PASSWORD '$banking_password';"
sql -c "CREATE USER untrusted_user WITH PASSWORD '$banking_password';"

sql <<-EOSQL
  GRANT CONNECT ON DATABASE banking TO direct_user;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO direct_user;
  SECURITY LABEL FOR pg_diffix ON ROLE direct_user IS 'direct';

  GRANT CONNECT ON DATABASE banking TO trusted_user;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO trusted_user;
  SECURITY LABEL FOR pg_diffix ON ROLE trusted_user IS 'anonymized_trusted';

  GRANT CONNECT ON DATABASE banking TO untrusted_user;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO untrusted_user;
  SECURITY LABEL FOR pg_diffix ON ROLE untrusted_user IS 'anonymized_untrusted';
EOSQL
