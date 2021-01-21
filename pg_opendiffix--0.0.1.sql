-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_opendiffix" to load this file. \quit

CREATE FUNCTION diffix_int4_count_transfn(internal, int4)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix_int4_count_any_transfn(internal, int4, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix_int4_count_finalfn(internal)
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

-- count(*)
CREATE AGGREGATE diffix_count(int4) (
  sfunc = diffix_int4_count_transfn,
  stype = internal,
  finalfunc = diffix_int4_count_finalfn
);

-- count(column)
CREATE AGGREGATE diffix_count(int4, anyelement) (
  sfunc = diffix_int4_count_any_transfn,
  stype = internal,
  finalfunc = diffix_int4_count_finalfn
);
