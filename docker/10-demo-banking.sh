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

sql -c "CREATE USER banking WITH PASSWORD '$banking_password';"

sql -c "CREATE USER banking_publish WITH PASSWORD '$banking_password';"

sql <<-EOSQL
  GRANT CONNECT ON DATABASE banking TO banking;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO banking;

  GRANT CONNECT ON DATABASE banking TO banking_publish;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO banking_publish;
  SECURITY LABEL FOR pg_diffix ON ROLE banking_publish IS 'anonymized_trusted';
EOSQL
