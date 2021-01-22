/*
 * Accepts AID type, generates standard diffix_count UDFs.
 * Does not clean up input macros.
 *
 * Input macros:
 *
 * AGG_AID_LABEL
 * AGG_AID_TYPE
 * AGG_AID_FMT
 * AGG_AID_EQUAL(a, b)
 * AGG_AID_HASH(aid)
 * AGG_AID_GETARG(a)
 * AGG_INIT_AID_HASH(state, aid, aid_hash) - optional
 * AGG_INIT_ENTRY(state, entry)            - optional
 *
 * Output:
 * diffix_<AID_LABEL>_count_<function>
 */

#ifndef PG_OPENDIFFIX_AGG_COUNT_A_H
#define PG_OPENDIFFIX_AGG_COUNT_A_H

#include "postgres.h"
#include "fmgr.h"

#include "utils/builtins.h"
#include "lib/stringinfo.h"

#define ARGS fcinfo

#endif /* PG_OPENDIFFIX_AGG_COUNT_A_H */

#define AGG_CONCAT_HELPER(a, b) CppConcat(a, b)
#define AGG_MAKE_NAME(name) \
  AGG_CONCAT_HELPER(AGG_CONCAT_HELPER(diffix_, AGG_AID_LABEL), AGG_CONCAT_HELPER(_, name))

/* Contribution state */
#define CS_PREFIX AGG_MAKE_NAME(count)
#define CS_AID_TYPE AGG_AID_TYPE
#define CS_AID_EQUAL(a, b) AGG_AID_EQUAL(a, b)
#define CS_AID_HASH(aid) AGG_AID_HASH(aid)
#define CS_TRACK_CONTRIBUTION
#define CS_CONTRIBUTION_TYPE uint64
#define CS_CONTRIBUTION_GREATER(a, b) (a > b)
#define CS_CONTRIBUTION_EQUAL(a, b) (a == b)
#define CS_CONTRIBUTION_COMBINE(a, b) (a + b)
#define CS_OVERALL_CONTRIBUTION_CALCULATE
#define CS_OVERALL_CONTRIBUTION_INITIAL 0
#define CS_SCOPE static inline
#define CS_DECLARE
#define CS_DEFINE
#ifdef AGG_INIT_AID_HASH
#define CS_INIT_AID_HASH(state, aid, aid_hash) AGG_INIT_AID_HASH(state, aid, aid_hash)
#endif
#ifdef AGG_INIT_ENTRY
#define CS_INIT_ENTRY(state, entry) AGG_INIT_ENTRY(state, entry)
#endif
#include "pg_opendiffix/template/contribution_state.h"

#define AGG_STAR_TRANSFN AGG_MAKE_NAME(count_star_transfn)
#define AGG_TRANSFN AGG_MAKE_NAME(count_transfn)
#define AGG_FINALFN AGG_MAKE_NAME(count_finalfn)
#define AGG_EXPLAIN_FINALFN AGG_MAKE_NAME(count_explain_finalfn)

#define AGG_CONTRIBUTION_STATE AGG_MAKE_NAME(count_ContributionState)
#define AGG_GET_STATE AGG_MAKE_NAME(count_state_getarg0)
#define AGG_UPDATE_AID AGG_MAKE_NAME(count_state_update_aid)
#define AGG_UPDATE_CONTRIBUTION AGG_MAKE_NAME(count_state_update_contribution)

PG_FUNCTION_INFO_V1(AGG_STAR_TRANSFN);
PG_FUNCTION_INFO_V1(AGG_TRANSFN);
PG_FUNCTION_INFO_V1(AGG_FINALFN);
PG_FUNCTION_INFO_V1(AGG_EXPLAIN_FINALFN);

Datum AGG_STAR_TRANSFN(PG_FUNCTION_ARGS)
{
  AGG_CONTRIBUTION_STATE *state = AGG_GET_STATE(ARGS);

  if (!PG_ARGISNULL(1))
  {
    AGG_UPDATE_CONTRIBUTION(state, AGG_AID_GETARG(state, 1), 1);
  }

  PG_RETURN_POINTER(state);
}

Datum AGG_TRANSFN(PG_FUNCTION_ARGS)
{
  AGG_CONTRIBUTION_STATE *state = AGG_GET_STATE(ARGS);

  if (!PG_ARGISNULL(1))
  {
    if (PG_ARGISNULL(2))
    {
      AGG_UPDATE_AID(state, AGG_AID_GETARG(state, 1));
    }
    else
    {
      AGG_UPDATE_CONTRIBUTION(state, AGG_AID_GETARG(state, 1), 1);
    }
  }

  PG_RETURN_POINTER(state);
}

Datum AGG_FINALFN(PG_FUNCTION_ARGS)
{
  AGG_CONTRIBUTION_STATE *state = AGG_GET_STATE(ARGS);
  PG_RETURN_INT64(state->distinct_aids);
}

Datum AGG_EXPLAIN_FINALFN(PG_FUNCTION_ARGS)
{
  AGG_CONTRIBUTION_STATE *state = AGG_GET_STATE(ARGS);
  StringInfoData string;
  int top_length = Min(state->top_contributors_length, state->distinct_aids);

  initStringInfo(&string);

  appendStringInfo(&string, "uniq_aid=%lu, seed=%u",
                   state->distinct_aids, state->aid_seed);

  appendStringInfo(&string, "\ntop=[");

  for (int i = 0; i < top_length; i++)
  {
    appendStringInfo(&string, AGG_AID_FMT "\u2794%lu",
                     state->top_contributors[i].aid,
                     state->top_contributors[i].contribution);
    if (i < top_length - 1)
    {
      appendStringInfo(&string, ", ");
    }
  }

  appendStringInfo(&string, "]");

  appendStringInfo(&string, "\ntrue=%li, anon=%li, final=%li",
                   state->overall_contribution,
                   state->overall_contribution,
                   state->overall_contribution);

  PG_RETURN_TEXT_P(cstring_to_text(string.data));
}

/* Clean up only locally used macros. */
#undef AGG_STAR_TRANSFN
#undef AGG_TRANSFN
#undef AGG_FINALFN
#undef AGG_EXPLAIN_FINALFN

#undef AGG_CONTRIBUTION_STATE
#undef AGG_GET_STATE
#undef AGG_UPDATE_AID
#undef AGG_UPDATE_CONTRIBUTION
