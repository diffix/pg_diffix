#include "postgres.h"
#include "fmgr.h"

#include "common/hashfn.h"

#include "pg_opendiffix/utils.h"

#define ARGS fcinfo

#define OUTLIER_COUNT 3
#define TOP_COUNT 3

#define TOP_CONTRIBUTORS (OUTLIER_COUNT + TOP_COUNT)

/* ----------------------------------------------------------------
 * AID: int4
 * ----------------------------------------------------------------
 */

PG_FUNCTION_INFO_V1(diffix_int4_count_transfn);
PG_FUNCTION_INFO_V1(diffix_int4_count_any_transfn);
PG_FUNCTION_INFO_V1(diffix_int4_count_finalfn);

/* Sum, Count */
#define CS_PREFIX int4_Sum_int8
#define CS_AID_TYPE int32
#define CS_AID_EQUAL(a, b) (a == b)
#define CS_AID_HASH(aid) murmurhash32(aid)
#define CS_CONTRIBUTION_TYPE int64
#define CS_CONTRIBUTION_GREATER(a, b) (a > b)
#define CS_CONTRIBUTION_EQUAL(a, b) (a == b)
#define CS_CONTRIBUTION_COMBINE(a, b) (a + b)
#define CS_OVERALL_CONTRIBUTION_CALCULATE
#define CS_OVERALL_CONTRIBUTION_INITIAL 0
#define CS_SCOPE static inline
#define CS_DECLARE
#define CS_DEFINE
#include "pg_opendiffix/template/contribution_state.h"

Datum diffix_int4_count_transfn(PG_FUNCTION_ARGS)
{
  int4_Sum_int8_ContributionState *state =
      int4_Sum_int8_state_getarg0(ARGS, TOP_CONTRIBUTORS);

  if (!PG_ARGISNULL(1))
  {
    int4_Sum_int8_state_update(state, PG_GETARG_INT32(1), 1);
  }

  PG_RETURN_POINTER(state);
}

Datum diffix_int4_count_any_transfn(PG_FUNCTION_ARGS)
{
  int4_Sum_int8_ContributionState *state =
      int4_Sum_int8_state_getarg0(ARGS, TOP_CONTRIBUTORS);

  if (!PG_ARGISNULL(1) && !PG_ARGISNULL(2))
  {
    int4_Sum_int8_state_update(state, PG_GETARG_INT32(1), 1);
  }

  PG_RETURN_POINTER(state);
}

Datum diffix_int4_count_finalfn(PG_FUNCTION_ARGS)
{
  int4_Sum_int8_ContributionState *state =
      int4_Sum_int8_state_getarg0(ARGS, TOP_CONTRIBUTORS);

  PG_RETURN_INT64(state->overall_contribution);
}
