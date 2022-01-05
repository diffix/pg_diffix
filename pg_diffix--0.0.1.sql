-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_diffix" to load this file. \quit

CREATE SCHEMA diffix;
GRANT USAGE ON SCHEMA diffix TO PUBLIC;

/* ----------------------------------------------------------------
 * Utilities
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix.access_level()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION diffix.show_settings()
RETURNS table(name text, setting text, short_desc text)
LANGUAGE SQL
AS $$
  SELECT
    name, setting, short_desc
  FROM pg_settings
  WHERE name LIKE 'pg_diffix.%';
$$;

CREATE OR REPLACE FUNCTION diffix.show_labels()
RETURNS table(object text, label text)
LANGUAGE SQL
AS $$
  SELECT
    pg_describe_object(classoid, objoid, objsubid), label
  FROM pg_seclabel
  WHERE provider = 'pg_diffix';
$$;

/* ----------------------------------------------------------------
 * lcf(aids...)
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix.lcf_transfn(internal, variadic aids "any")
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix.lcf_finalfn(internal, variadic aids "any")
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix.lcf_explain_finalfn(internal, variadic aids "any")
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE AGGREGATE diffix.lcf(variadic aids "any") (
  sfunc = diffix.lcf_transfn,
  stype = internal,
  finalfunc = diffix.lcf_finalfn,
  finalfunc_extra
);

CREATE AGGREGATE diffix.explain_lcf(variadic aids "any") (
  sfunc = diffix.lcf_transfn,
  stype = internal,
  finalfunc = diffix.lcf_explain_finalfn,
  finalfunc_extra
);

/* ----------------------------------------------------------------
 * anon_count_distinct(any, aids...)
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix.anon_count_distinct_transfn(internal, value "any", variadic aids "any")
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix.anon_count_distinct_finalfn(internal, value "any", variadic aids "any")
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix.anon_count_distinct_explain_finalfn(internal, value "any", variadic aids "any")
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE AGGREGATE diffix.anon_count_distinct(value "any", variadic aids "any") (
  sfunc = diffix.anon_count_distinct_transfn,
  stype = internal,
  finalfunc = diffix.anon_count_distinct_finalfn,
  finalfunc_extra
);

CREATE AGGREGATE diffix.explain_anon_count_distinct(value "any", variadic aids "any") (
  sfunc = diffix.anon_count_distinct_transfn,
  stype = internal,
  finalfunc = diffix.anon_count_distinct_explain_finalfn,
  finalfunc_extra
);

/* ----------------------------------------------------------------
 * anon_count(aids...)
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix.anon_count_transfn(internal, variadic aids "any")
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix.anon_count_finalfn(internal, variadic aids "any")
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix.anon_count_explain_finalfn(internal, variadic aids "any")
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE AGGREGATE diffix.anon_count(variadic aids "any") (
  sfunc = diffix.anon_count_transfn,
  stype = internal,
  finalfunc = diffix.anon_count_finalfn,
  finalfunc_extra
);

CREATE AGGREGATE diffix.explain_anon_count(variadic aids "any") (
  sfunc = diffix.anon_count_transfn,
  stype = internal,
  finalfunc = diffix.anon_count_explain_finalfn,
  finalfunc_extra
);

/* ----------------------------------------------------------------
 * anon_count_any(any, aids...)
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix.anon_count_any_transfn(internal, value "any", variadic aids "any")
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix.anon_count_any_finalfn(internal, value "any", variadic aids "any")
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix.anon_count_any_explain_finalfn(internal, value "any", variadic aids "any")
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE AGGREGATE diffix.anon_count_any(value "any", variadic aids "any") (
  sfunc = diffix.anon_count_any_transfn,
  stype = internal,
  finalfunc = diffix.anon_count_any_finalfn,
  finalfunc_extra
);

CREATE AGGREGATE diffix.explain_anon_count_any(value "any", variadic aids "any") (
  sfunc = diffix.anon_count_any_transfn,
  stype = internal,
  finalfunc = diffix.anon_count_any_explain_finalfn,
  finalfunc_extra
);
