-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_diffix" to load this file. \quit

REVOKE ALL ON SCHEMA @extschema@ FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA @extschema@ FROM PUBLIC;

GRANT USAGE ON SCHEMA @extschema@ TO PUBLIC;

DO $$ BEGIN
  -- Generate a random salt for the current database.
  EXECUTE 'ALTER DATABASE ' || current_database() || ' SET pg_diffix.salt TO ''' || gen_random_uuid() || '''';
END $$ LANGUAGE plpgsql;

/* ----------------------------------------------------------------
 * Utilities
 * ----------------------------------------------------------------
 */

CREATE FUNCTION access_level()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path = '';

CREATE FUNCTION show_settings()
RETURNS table(name text, setting text, short_desc text)
LANGUAGE SQL
AS $$
  SELECT
    name, setting, short_desc
  FROM pg_settings
  WHERE name LIKE 'pg_diffix.%';
$$
SECURITY INVOKER SET search_path = '';

CREATE FUNCTION show_labels()
RETURNS table(objtype text, objname text, label text)
LANGUAGE SQL
AS $$
  SELECT objtype, objname, label
  FROM pg_seclabels
  WHERE provider = 'pg_diffix'
  ORDER BY
    CASE WHEN objtype = 'table' THEN 1 WHEN objtype = 'column' THEN 2 WHEN objtype = 'role' THEN 3 END,
    objname;
$$
SECURITY INVOKER SET search_path = '';

CREATE PROCEDURE mark_personal(table_name text, variadic aid_columns text[])
AS $$
  DECLARE
    aid_column text;
  BEGIN
    DELETE FROM pg_catalog.pg_seclabel WHERE provider = 'pg_diffix' AND objoid = table_name::regclass::oid AND label = 'aid';

    EXECUTE 'SECURITY LABEL FOR pg_diffix ON TABLE ' || table_name || ' IS ''personal''';

    FOREACH aid_column IN ARRAY aid_columns LOOP
      EXECUTE 'SECURITY LABEL FOR pg_diffix ON COLUMN ' || table_name || '.' || aid_column || ' IS ''aid''';
    END LOOP;
  END;
$$ LANGUAGE plpgsql;

CREATE PROCEDURE mark_public(table_name text)
AS $$
  BEGIN
    DELETE FROM pg_catalog.pg_seclabel WHERE provider = 'pg_diffix' AND objoid = table_name::regclass::oid AND label = 'aid';

    EXECUTE 'SECURITY LABEL FOR pg_diffix ON TABLE ' || table_name || ' IS ''public''';
  END;
$$ LANGUAGE plpgsql;

CREATE PROCEDURE unmark_table(table_name text)
AS $$
  BEGIN
    DELETE FROM pg_catalog.pg_seclabel WHERE provider = 'pg_diffix' AND objoid = table_name::regclass::oid;
  END;
$$ LANGUAGE plpgsql;

CREATE TYPE AccessLevel AS ENUM ('direct', 'anonymized_trusted', 'anonymized_untrusted');

CREATE PROCEDURE mark_role(role_name text, access_level AccessLevel)
AS $$
  BEGIN
    EXECUTE 'SECURITY LABEL FOR pg_diffix ON ROLE ' || quote_ident(role_name) || ' IS ''' || access_level || '''';
  END;
$$ LANGUAGE plpgsql;

CREATE PROCEDURE unmark_role(role_name text)
AS $$
  BEGIN
    EXECUTE 'SECURITY LABEL FOR pg_diffix ON ROLE ' || quote_ident(role_name) || ' IS NULL';
  END;
$$ LANGUAGE plpgsql;

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

CREATE FUNCTION anon_agg_state_input(cstring)
RETURNS AnonAggState
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT STABLE
SECURITY INVOKER SET search_path = '';

CREATE FUNCTION anon_agg_state_output(AnonAggState)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT STABLE
SECURITY INVOKER SET search_path = '';

CREATE TYPE AnonAggState (
  INPUT = anon_agg_state_input,
  OUTPUT = anon_agg_state_output,
  LIKE = internal
);

CREATE FUNCTION anon_agg_state_transfn(AnonAggState, variadic aids "any")
RETURNS AnonAggState
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path = '';

CREATE FUNCTION anon_agg_state_transfn(AnonAggState, value "any", variadic aids "any")
RETURNS AnonAggState
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path = '';

CREATE FUNCTION anon_agg_state_finalfn(AnonAggState, variadic aids "any")
RETURNS AnonAggState
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path = '';

CREATE FUNCTION anon_agg_state_finalfn(AnonAggState, value "any", variadic aids "any")
RETURNS AnonAggState
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path = '';


/* ----------------------------------------------------------------
 * Non-anonymizing aggregators
 * ----------------------------------------------------------------
 */

/*
 * Present here only to provide the proper signatures for the aggregators available
 * to the non-direct access users.
 */

CREATE FUNCTION dummy_transfn(AnonAggState)
RETURNS AnonAggState
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path = '';

CREATE FUNCTION dummy_transfn(AnonAggState, value "any")
RETURNS AnonAggState
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path = '';

CREATE FUNCTION dummy_finalfn(AnonAggState)
RETURNS AnonAggState
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path = '';

CREATE FUNCTION dummy_finalfn(AnonAggState, value "any")
RETURNS AnonAggState
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE
SECURITY INVOKER SET search_path = '';

CREATE AGGREGATE count_noise(*) (
  sfunc = dummy_transfn,
  stype = AnonAggState,
  finalfunc = dummy_finalfn,
  finalfunc_extra = true,
  finalfunc_modify = read_write
);

CREATE AGGREGATE count_noise(value "any") (
  sfunc = dummy_transfn,
  stype = AnonAggState,
  finalfunc = dummy_finalfn,
  finalfunc_extra = true,
  finalfunc_modify = read_write
);

/* ----------------------------------------------------------------
 * Anonymizing aggregators
 * ----------------------------------------------------------------
 */

/*
 * Aggregates sharing the same inputs and transition functions can get merged
 * into a single transition calculation. We mark finalfunc_modify=read_write
 * to force a unique state for each anonymizing aggregator.
 */

CREATE AGGREGATE low_count(variadic aids "any") (
  sfunc = anon_agg_state_transfn,
  stype = AnonAggState,
  finalfunc = anon_agg_state_finalfn,
  finalfunc_extra = true,
  finalfunc_modify = read_write
);

CREATE AGGREGATE anon_count_distinct(value "any", variadic aids "any") (
  sfunc = anon_agg_state_transfn,
  stype = AnonAggState,
  finalfunc = anon_agg_state_finalfn,
  finalfunc_extra = true,
  finalfunc_modify = read_write
);

CREATE AGGREGATE anon_count_star(variadic aids "any") (
  sfunc = anon_agg_state_transfn,
  stype = AnonAggState,
  finalfunc = anon_agg_state_finalfn,
  finalfunc_extra = true,
  finalfunc_modify = read_write
);

CREATE AGGREGATE anon_count_value(value "any", variadic aids "any") (
  sfunc = anon_agg_state_transfn,
  stype = AnonAggState,
  finalfunc = anon_agg_state_finalfn,
  finalfunc_extra = true,
  finalfunc_modify = read_write
);

CREATE AGGREGATE anon_count_distinct_noise(value "any", variadic aids "any") (
  sfunc = anon_agg_state_transfn,
  stype = AnonAggState,
  finalfunc = anon_agg_state_finalfn,
  finalfunc_extra = true,
  finalfunc_modify = read_write
);

CREATE AGGREGATE anon_count_star_noise(variadic aids "any") (
  sfunc = anon_agg_state_transfn,
  stype = AnonAggState,
  finalfunc = anon_agg_state_finalfn,
  finalfunc_extra = true,
  finalfunc_modify = read_write
);

CREATE AGGREGATE anon_count_value_noise(value "any", variadic aids "any") (
  sfunc = anon_agg_state_transfn,
  stype = AnonAggState,
  finalfunc = anon_agg_state_finalfn,
  finalfunc_extra = true,
  finalfunc_modify = read_write
);

/* ----------------------------------------------------------------
 * Bucket-specific aggregates
 * ----------------------------------------------------------------
 */

CREATE FUNCTION placeholder_func(anyelement)
RETURNS anyelement
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT
SECURITY INVOKER SET search_path = '';

CREATE AGGREGATE is_suppress_bin(*) (
  sfunc = placeholder_func,
  stype = boolean,
  initcond = false
);

CREATE FUNCTION internal_qual_wrapper(boolean)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE
SECURITY INVOKER SET search_path = '';

/* ----------------------------------------------------------------
 * Scalar functions
 * ----------------------------------------------------------------
 */

CREATE FUNCTION round_by(value numeric, amount numeric)
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

CREATE FUNCTION round_by(value double precision, amount double precision)
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

CREATE FUNCTION ceil_by(value numeric, amount numeric)
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

CREATE FUNCTION ceil_by(value double precision, amount double precision)
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

CREATE FUNCTION floor_by(value numeric, amount numeric)
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

CREATE FUNCTION floor_by(value double precision, amount double precision)
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
