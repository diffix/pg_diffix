-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_diffix" to load this file. \quit

/* ----------------------------------------------------------------
 * AID: int4
 * ----------------------------------------------------------------
 */

-- count:int4

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

CREATE FUNCTION diffix_int4_count_explain_finalfn(internal)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE AGGREGATE diffix_count(int4) (
  sfunc = diffix_int4_count_star_transfn,
  stype = internal,
  finalfunc = diffix_int4_count_finalfn
);

CREATE AGGREGATE explain_diffix_count(int4) (
  sfunc = diffix_int4_count_star_transfn,
  stype = internal,
  finalfunc = diffix_int4_count_explain_finalfn
);

CREATE AGGREGATE diffix_count(int4, anyelement) (
  sfunc = diffix_int4_count_transfn,
  stype = internal,
  finalfunc = diffix_int4_count_finalfn
);

CREATE AGGREGATE explain_diffix_count(int4, anyelement) (
  sfunc = diffix_int4_count_transfn,
  stype = internal,
  finalfunc = diffix_int4_count_explain_finalfn
);

-- /count:int4

-- lcf:int4

CREATE FUNCTION diffix_int4_lcf_transfn(internal, int4)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix_int4_lcf_finalfn(internal)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix_int4_lcf_explain_finalfn(internal)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE AGGREGATE diffix_lcf(int4) (
  sfunc = diffix_int4_lcf_transfn,
  stype = internal,
  finalfunc = diffix_int4_lcf_finalfn
);

CREATE AGGREGATE explain_diffix_lcf(int4) (
  sfunc = diffix_int4_lcf_transfn,
  stype = internal,
  finalfunc = diffix_int4_lcf_explain_finalfn
);

-- /lcf:int4

/* ----------------------------------------------------------------
 * AID: text
 * ----------------------------------------------------------------
 */

-- count:text

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

CREATE FUNCTION diffix_text_count_explain_finalfn(internal)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE AGGREGATE diffix_count(text) (
  sfunc = diffix_text_count_star_transfn,
  stype = internal,
  finalfunc = diffix_text_count_finalfn
);

CREATE AGGREGATE explain_diffix_count(text) (
  sfunc = diffix_text_count_star_transfn,
  stype = internal,
  finalfunc = diffix_text_count_explain_finalfn
);

CREATE AGGREGATE diffix_count(text, anyelement) (
  sfunc = diffix_text_count_transfn,
  stype = internal,
  finalfunc = diffix_text_count_finalfn
);

CREATE AGGREGATE explain_diffix_count(text, anyelement) (
  sfunc = diffix_text_count_transfn,
  stype = internal,
  finalfunc = diffix_text_count_explain_finalfn
);

-- /count:text

-- lcf:text

CREATE FUNCTION diffix_text_lcf_transfn(internal, text)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix_text_lcf_finalfn(internal)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix_text_lcf_explain_finalfn(internal)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE AGGREGATE diffix_lcf(text) (
  sfunc = diffix_text_lcf_transfn,
  stype = internal,
  finalfunc = diffix_text_lcf_finalfn
);

CREATE AGGREGATE explain_diffix_lcf(text) (
  sfunc = diffix_text_lcf_transfn,
  stype = internal,
  finalfunc = diffix_text_lcf_explain_finalfn
);

-- /lcf:text
