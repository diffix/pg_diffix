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
SECURITY INVOKER SET search_path='';

CREATE OR REPLACE FUNCTION diffix.show_settings()
RETURNS table(name text, setting text, short_desc text)
LANGUAGE SQL
AS $$
  SELECT
    name, setting, short_desc
  FROM pg_settings
  WHERE name LIKE 'pg_diffix.%';
$$
SECURITY INVOKER SET search_path='';

CREATE OR REPLACE FUNCTION diffix.show_labels()
RETURNS table(object text, label text)
LANGUAGE SQL
AS $$
  SELECT
    pg_describe_object(classoid, objoid, objsubid), label
  FROM pg_seclabel
  WHERE provider = 'pg_diffix';
$$
SECURITY INVOKER SET search_path='';

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
 * However, an AnonAggState retrieved by a direct call to anonymizing aggregates may be inspected in the query output.
 * Serialization is handled by `anon_agg_state_output`, which forwards it to the aggregate's explain implementation.
 * The parse function `anon_agg_state_input` is a stub which will always throw an error.
 *
 * If anonymizing aggregates are invoked directly by SQL in a non-anonymizing query, then the AnonAggState
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
SECURITY INVOKER SET search_path='';

CREATE FUNCTION diffix.anon_agg_state_output(AnonAggState)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT STABLE
SECURITY INVOKER SET search_path='';

CREATE TYPE AnonAggState (
  INPUT = diffix.anon_agg_state_input,
  OUTPUT = diffix.anon_agg_state_output,
  LIKE = internal
);

CREATE FUNCTION diffix.anon_agg_state_transfn(AnonAggState, variadic aids "any")
RETURNS AnonAggState
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path='';

CREATE FUNCTION diffix.anon_agg_state_transfn(AnonAggState, value "any", variadic aids "any")
RETURNS AnonAggState
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path='';

CREATE FUNCTION diffix.anon_agg_state_finalfn(AnonAggState, variadic aids "any")
RETURNS AnonAggState
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path='';

CREATE FUNCTION diffix.anon_agg_state_finalfn(AnonAggState, value "any", variadic aids "any")
RETURNS AnonAggState
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path='';

/* ----------------------------------------------------------------
 * lcf(aids...)
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix.lcf_transfn(internal, variadic aids "any")
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path='';

CREATE FUNCTION diffix.lcf_finalfn(internal, variadic aids "any")
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path='';

CREATE AGGREGATE diffix.lcf(variadic aids "any") (
  sfunc = diffix.lcf_transfn,
  stype = internal,
  finalfunc = diffix.lcf_finalfn,
  finalfunc_extra
);

/* ----------------------------------------------------------------
 * anon_count_distinct(any, aids...)
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix.anon_count_distinct_transfn(internal, value "any", variadic aids "any")
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path='';

CREATE FUNCTION diffix.anon_count_distinct_finalfn(internal, value "any", variadic aids "any")
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path='';

CREATE AGGREGATE diffix.anon_count_distinct(value "any", variadic aids "any") (
  sfunc = diffix.anon_count_distinct_transfn,
  stype = internal,
  finalfunc = diffix.anon_count_distinct_finalfn,
  finalfunc_extra
);

/* ----------------------------------------------------------------
 * anon_count_star(aids...)
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix.anon_count_star_transfn(internal, variadic aids "any")
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path='';

CREATE FUNCTION diffix.anon_count_star_finalfn(internal, variadic aids "any")
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path='';

CREATE AGGREGATE diffix.anon_count_star(variadic aids "any") (
  sfunc = diffix.anon_count_star_transfn,
  stype = internal,
  finalfunc = diffix.anon_count_star_finalfn,
  finalfunc_extra
);

/* ----------------------------------------------------------------
 * anon_count_value(any, aids...)
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix.anon_count_value_transfn(internal, value "any", variadic aids "any")
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path='';

CREATE FUNCTION diffix.anon_count_value_finalfn(internal, value "any", variadic aids "any")
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path='';

CREATE AGGREGATE diffix.anon_count_value(value "any", variadic aids "any") (
  sfunc = diffix.anon_count_value_transfn,
  stype = internal,
  finalfunc = diffix.anon_count_value_finalfn,
  finalfunc_extra
);

/* ----------------------------------------------------------------
 * Bucket-specific aggregates
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix.placeholder_func(anyelement)
RETURNS anyelement
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT
SECURITY INVOKER SET search_path='';

CREATE AGGREGATE diffix.is_suppress_bin(*) (
  sfunc = diffix.placeholder_func,
  stype = boolean,
  initcond = FALSE
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
SECURITY INVOKER SET search_path='';

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
SECURITY INVOKER SET search_path='';

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
SECURITY INVOKER SET search_path='';

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
SECURITY INVOKER SET search_path='';

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
SECURITY INVOKER SET search_path='';

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
SECURITY INVOKER SET search_path='';
