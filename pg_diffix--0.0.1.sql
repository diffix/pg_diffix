-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_diffix" to load this file. \quit

CREATE SCHEMA diffix;

REVOKE ALL ON SCHEMA diffix FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA diffix FROM PUBLIC;

GRANT USAGE ON SCHEMA diffix TO PUBLIC;

/* ----------------------------------------------------------------
 * Utilities
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix.access_level()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path = '';

CREATE OR REPLACE FUNCTION diffix.show_settings()
RETURNS table(name text, setting text, short_desc text)
LANGUAGE SQL
AS $$
  SELECT
    name, setting, short_desc
  FROM pg_settings
  WHERE name LIKE 'pg_diffix.%';
$$
SECURITY INVOKER SET search_path = '';

CREATE OR REPLACE FUNCTION diffix.show_labels()
RETURNS table(object text, label text)
LANGUAGE SQL
AS $$
  SELECT
    pg_describe_object(classoid, objoid, objsubid), label
  FROM pg_seclabel
  WHERE provider = 'pg_diffix';
$$
SECURITY INVOKER SET search_path = '';

CREATE OR REPLACE PROCEDURE diffix.make_personal(table_name text, salt text, variadic aid_columns text[])
AS $$
  DECLARE
    table_oid integer := (SELECT oid FROM pg_class WHERE relname = table_name);
    aid_column text;
  BEGIN
    DELETE FROM pg_seclabel WHERE provider = 'pg_diffix' AND objoid=table_oid AND label='aid';

    EXECUTE 'SECURITY LABEL FOR pg_diffix ON TABLE '
            || quote_ident(table_name)
            || ' IS '
            || quote_literal(concat('personal:', salt));

    FOREACH aid_column IN ARRAY aid_columns LOOP
      EXECUTE 'SECURITY LABEL FOR pg_diffix ON COLUMN '
              || quote_ident(table_name)
              || '.'
              || quote_ident(aid_column)
              || ' IS ''aid''';
    END LOOP;
  END;
$$ LANGUAGE plpgsql
SECURITY INVOKER SET search_path = 'public';

CREATE OR REPLACE PROCEDURE diffix.make_public(table_name text)
AS $$
  DECLARE
    table_oid integer := (SELECT oid FROM pg_class WHERE relname = table_name);
  BEGIN
    DELETE FROM pg_seclabel WHERE provider = 'pg_diffix' AND objoid=table_oid AND label='aid';

    EXECUTE 'SECURITY LABEL FOR pg_diffix ON TABLE '
            || quote_ident(table_name)
            || ' IS ''public''';
  END;
$$ LANGUAGE plpgsql
SECURITY INVOKER SET search_path = 'public';

/* ----------------------------------------------------------------
 * Common aggregation interface
 * ----------------------------------------------------------------
 */

/*
 * AnonAggState is a pointer in disguise. We want Postgres to pass it by value to avoid unintended data copying.
 *
 * The AnonAggState data is supposed to be finalized by the parent BucketScan.
 * As a raw value, it cannot be used in expressions as it does not support any operators/functions.
 * Projections of original aggregates are delayed until after finalization in the BucketScan node.
 *
 * However, an AnonAggState retrieved by a direct call to anonymizing aggregators may be inspected in the query output.
 * Serialization is handled by `anon_agg_state_output`, which forwards it to the aggregate's explain implementation.
 * The parse function `anon_agg_state_input` is a stub which will always throw an error.
 *
 * If anonymizing aggregators are invoked directly by SQL in a non-anonymizing query, then the AnonAggState
 * will be allocated in the aggregation context of the current Agg node. Passing an AnonAggState up
 * (for example from a subquery) outside of the intended scope may result in memory corruption.
 *
 * See `aggregation/common.h` for more info.
 */
CREATE TYPE AnonAggState;

CREATE FUNCTION diffix.anon_agg_state_input(cstring)
RETURNS AnonAggState
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT STABLE
SECURITY INVOKER SET search_path = '';

CREATE FUNCTION diffix.anon_agg_state_output(AnonAggState)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT STABLE
SECURITY INVOKER SET search_path = '';

CREATE TYPE AnonAggState (
  INPUT = diffix.anon_agg_state_input,
  OUTPUT = diffix.anon_agg_state_output,
  LIKE = internal
);

CREATE FUNCTION diffix.anon_agg_state_transfn(AnonAggState, variadic aids "any")
RETURNS AnonAggState
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path = '';

CREATE FUNCTION diffix.anon_agg_state_transfn(AnonAggState, value "any", variadic aids "any")
RETURNS AnonAggState
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path = '';

CREATE FUNCTION diffix.anon_agg_state_finalfn(AnonAggState, variadic aids "any")
RETURNS AnonAggState
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path = '';

CREATE FUNCTION diffix.anon_agg_state_finalfn(AnonAggState, value "any", variadic aids "any")
RETURNS AnonAggState
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path = '';

/* ----------------------------------------------------------------
 * Anonymizing aggregators
 * ----------------------------------------------------------------
 */

/*
 * Aggregates sharing the same inputs and transition functions can get merged
 * into a single transition calculation. We mark finalfunc_modify=read_write
 * to force a unique state for each anonymizing aggregator.
 */

CREATE AGGREGATE diffix.low_count(variadic aids "any") (
  sfunc = diffix.anon_agg_state_transfn,
  stype = AnonAggState,
  finalfunc = diffix.anon_agg_state_finalfn,
  finalfunc_extra = true,
  finalfunc_modify = read_write
);

CREATE AGGREGATE diffix.anon_count_distinct(value "any", variadic aids "any") (
  sfunc = diffix.anon_agg_state_transfn,
  stype = AnonAggState,
  finalfunc = diffix.anon_agg_state_finalfn,
  finalfunc_extra = true,
  finalfunc_modify = read_write
);

CREATE AGGREGATE diffix.anon_count_star(variadic aids "any") (
  sfunc = diffix.anon_agg_state_transfn,
  stype = AnonAggState,
  finalfunc = diffix.anon_agg_state_finalfn,
  finalfunc_extra = true,
  finalfunc_modify = read_write
);

CREATE AGGREGATE diffix.anon_count_value(value "any", variadic aids "any") (
  sfunc = diffix.anon_agg_state_transfn,
  stype = AnonAggState,
  finalfunc = diffix.anon_agg_state_finalfn,
  finalfunc_extra = true,
  finalfunc_modify = read_write
);

/* ----------------------------------------------------------------
 * Bucket-specific aggregates
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix.placeholder_func(anyelement)
RETURNS anyelement
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT
SECURITY INVOKER SET search_path = '';

CREATE AGGREGATE diffix.is_suppress_bin(*) (
  sfunc = diffix.placeholder_func,
  stype = boolean,
  initcond = false
);

/* ----------------------------------------------------------------
 * Scalar functions
 * ----------------------------------------------------------------
 */

CREATE OR REPLACE FUNCTION diffix.round_by(value numeric, amount numeric)
RETURNS numeric AS $$
  BEGIN
	IF amount <= 0 THEN
	  RETURN NULL;
	ELSE
		RETURN round(value / amount) * amount;
	END IF;
  END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT
SECURITY INVOKER SET search_path = '';

CREATE OR REPLACE FUNCTION diffix.round_by(value double precision, amount double precision)
RETURNS double precision AS $$
  BEGIN
	IF amount <= 0 THEN
	  RETURN NULL;
	ELSE
		RETURN round(value / amount) * amount;
	END IF;
  END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT
SECURITY INVOKER SET search_path = '';

CREATE OR REPLACE FUNCTION diffix.ceil_by(value numeric, amount numeric)
RETURNS numeric AS $$
  BEGIN
	IF amount <= 0 THEN
	  RETURN NULL;
	ELSE
		RETURN ceil(value / amount) * amount;
	END IF;
  END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT
SECURITY INVOKER SET search_path = '';

CREATE OR REPLACE FUNCTION diffix.ceil_by(value double precision, amount double precision)
RETURNS double precision AS $$
  BEGIN
	IF amount <= 0 THEN
	  RETURN NULL;
	ELSE
		RETURN ceil(value / amount) * amount;
	END IF;
  END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT
SECURITY INVOKER SET search_path = '';

CREATE OR REPLACE FUNCTION diffix.floor_by(value numeric, amount numeric)
RETURNS numeric AS $$
  BEGIN
	IF amount <= 0 THEN
	  RETURN NULL;
	ELSE
		RETURN floor(value / amount) * amount;
	END IF;
  END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT
SECURITY INVOKER SET search_path = '';

CREATE OR REPLACE FUNCTION diffix.floor_by(value double precision, amount double precision)
RETURNS double precision AS $$
  BEGIN
	IF amount <= 0 THEN
	  RETURN NULL;
	ELSE
		RETURN floor(value / amount) * amount;
	END IF;
  END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT
SECURITY INVOKER SET search_path = '';
