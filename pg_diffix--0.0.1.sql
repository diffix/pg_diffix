-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_diffix" to load this file. \quit

CREATE SCHEMA diffix;

/* ----------------------------------------------------------------
 * lcf(aid)
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix.lcf_transfn(internal, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix.lcf_finalfn(internal, anyelement)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix.lcf_explain_finalfn(internal, anyelement)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE AGGREGATE diffix.lcf(anyelement) (
  sfunc = diffix.lcf_transfn,
  stype = internal,
  finalfunc = diffix.lcf_finalfn,
  finalfunc_extra
);

CREATE AGGREGATE diffix.explain_lcf(anyelement) (
  sfunc = diffix.lcf_transfn,
  stype = internal,
  finalfunc = diffix.lcf_explain_finalfn,
  finalfunc_extra
);

/* ----------------------------------------------------------------
 * anon_count_distinct(aid)
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix.anon_count_distinct_transfn(internal, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix.anon_count_distinct_finalfn(internal, anyelement)
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix.anon_count_distinct_explain_finalfn(internal, anyelement)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE AGGREGATE diffix.anon_count_distinct(anyelement) (
  sfunc = diffix.anon_count_distinct_transfn,
  stype = internal,
  finalfunc = diffix.anon_count_distinct_finalfn,
  finalfunc_extra
);

CREATE AGGREGATE diffix.explain_anon_count_distinct(anyelement) (
  sfunc = diffix.anon_count_distinct_transfn,
  stype = internal,
  finalfunc = diffix.anon_count_distinct_explain_finalfn,
  finalfunc_extra
);

/* ----------------------------------------------------------------
 * anon_count(aid)
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix.anon_count_transfn(internal, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix.anon_count_finalfn(internal, anyelement)
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix.anon_count_explain_finalfn(internal, anyelement)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE AGGREGATE diffix.anon_count(anyelement) (
  sfunc = diffix.anon_count_transfn,
  stype = internal,
  finalfunc = diffix.anon_count_finalfn,
  finalfunc_extra
);

CREATE AGGREGATE diffix.explain_anon_count(anyelement) (
  sfunc = diffix.anon_count_transfn,
  stype = internal,
  finalfunc = diffix.anon_count_explain_finalfn,
  finalfunc_extra
);

/* ----------------------------------------------------------------
 * anon_count(aid, any)
 * ----------------------------------------------------------------
 */

CREATE FUNCTION diffix.anon_count_any_transfn(internal, anyelement, "any")
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix.anon_count_any_finalfn(internal, anyelement, "any")
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE FUNCTION diffix.anon_count_any_explain_finalfn(internal, anyelement, "any")
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE;

CREATE AGGREGATE diffix.anon_count(anyelement, "any") (
  sfunc = diffix.anon_count_any_transfn,
  stype = internal,
  finalfunc = diffix.anon_count_any_finalfn,
  finalfunc_extra
);

CREATE AGGREGATE diffix.explain_anon_count(anyelement, "any") (
  sfunc = diffix.anon_count_any_transfn,
  stype = internal,
  finalfunc = diffix.anon_count_any_explain_finalfn,
  finalfunc_extra
);
