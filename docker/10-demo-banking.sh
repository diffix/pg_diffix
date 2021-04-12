#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	CREATE DATABASE banking;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "banking" <<-EOSQL
	CREATE EXTENSION pg_diffix;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "banking" \
	-f "/docker-entrypoint-initdb.d/demo/00-banking-schema.sql" \
	-f "/docker-entrypoint-initdb.d/demo/01-banking-data.sql" \
	-f "/docker-entrypoint-initdb.d/demo/02-banking-constraints.sql"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "banking" <<-EOSQL
	CREATE USER publish WITH PASSWORD 'password';
	SECURITY LABEL FOR pg_diffix ON ROLE publish IS 'publish';
	GRANT CONNECT ON DATABASE banking TO publish;
	GRANT SELECT ON ALL TABLES IN SCHEMA public TO publish;
EOSQL
