-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_opendiffix" to load this file. \quit

/* ----------------------------------------------------------------
 * AID: int4
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix_int4_count_star_transfn(internal, int4)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix_int4_count_transfn(internal, int4, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix_int4_count_finalfn(internal)
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE AGGREGATE diffix_count(int4) (
  sfunc = diffix_int4_count_star_transfn,
  stype = internal,
  finalfunc = diffix_int4_count_finalfn
);

CREATE AGGREGATE diffix_count(int4, anyelement) (
  sfunc = diffix_int4_count_transfn,
  stype = internal,
  finalfunc = diffix_int4_count_finalfn
);


/* ----------------------------------------------------------------
 * AID: text
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix_text_count_star_transfn(internal, text)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix_text_count_transfn(internal, text, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix_text_count_finalfn(internal)
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE AGGREGATE diffix_count(text) (
  sfunc = diffix_text_count_star_transfn,
  stype = internal,
  finalfunc = diffix_text_count_finalfn
);

CREATE AGGREGATE diffix_count(text, anyelement) (
  sfunc = diffix_text_count_transfn,
  stype = internal,
  finalfunc = diffix_text_count_finalfn
);
