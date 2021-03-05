-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_diffix" to load this file. \quit

/* ----------------------------------------------------------------
 * diffix_lcf(aid)
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix_lcf_transfn(internal, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix_lcf_finalfn(internal, anyelement)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix_lcf_explain_finalfn(internal, anyelement)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE AGGREGATE diffix_lcf(anyelement) (
  sfunc = diffix_lcf_transfn,
  stype = internal,
  finalfunc = diffix_lcf_finalfn,
  finalfunc_extra
);

CREATE AGGREGATE explain_diffix_lcf(anyelement) (
  sfunc = diffix_lcf_transfn,
  stype = internal,
  finalfunc = diffix_lcf_explain_finalfn,
  finalfunc_extra
);

/* ----------------------------------------------------------------
 * diffix_count_distinct(aid)
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix_count_distinct_transfn(internal, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix_count_distinct_finalfn(internal, anyelement)
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix_count_distinct_explain_finalfn(internal, anyelement)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE AGGREGATE diffix_count_distinct(anyelement) (
  sfunc = diffix_count_distinct_transfn,
  stype = internal,
  finalfunc = diffix_count_distinct_finalfn,
  finalfunc_extra
);

CREATE AGGREGATE explain_diffix_count_distinct(anyelement) (
  sfunc = diffix_count_distinct_transfn,
  stype = internal,
  finalfunc = diffix_count_distinct_explain_finalfn,
  finalfunc_extra
);

/* ----------------------------------------------------------------
 * diffix_count(aid)
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix_count_transfn(internal, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix_count_finalfn(internal, anyelement)
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix_count_explain_finalfn(internal, anyelement)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE AGGREGATE diffix_count(anyelement) (
  sfunc = diffix_count_transfn,
  stype = internal,
  finalfunc = diffix_count_finalfn,
  finalfunc_extra
);

CREATE AGGREGATE explain_diffix_count(anyelement) (
  sfunc = diffix_count_transfn,
  stype = internal,
  finalfunc = diffix_count_explain_finalfn,
  finalfunc_extra
);

/* ----------------------------------------------------------------
 * diffix_count(aid, any)
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix_count_any_transfn(internal, anyelement, "any")
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix_count_any_finalfn(internal, anyelement, "any")
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix_count_any_explain_finalfn(internal, anyelement, "any")
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE AGGREGATE diffix_count(anyelement, "any") (
  sfunc = diffix_count_any_transfn,
  stype = internal,
  finalfunc = diffix_count_any_finalfn,
  finalfunc_extra
);

CREATE AGGREGATE explain_diffix_count(anyelement, "any") (
  sfunc = diffix_count_any_transfn,
  stype = internal,
  finalfunc = diffix_count_any_explain_finalfn,
  finalfunc_extra
);
