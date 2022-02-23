#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"

#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/aggregation/count_common.h"
#include "pg_diffix/query/anonymization.h"

/*-------------------------------------------------------------------------
 * Aggregation callbacks
 *-------------------------------------------------------------------------
 */

static const int AIDS_OFFSET = 1;

static AnonAggState *agg_create_state(MemoryContext memory_context, PG_FUNCTION_ARGS)
{
  return count_agg_create_state(memory_context, fcinfo, AIDS_OFFSET);
}

static void agg_transition(AnonAggState *base_state, PG_FUNCTION_ARGS)
{
  CountState *state = (CountState *)base_state;

  if (all_aids_null(fcinfo, AIDS_OFFSET, list_length(state->contribution_trackers)))
    return;

  ListCell *cell = NULL;
  foreach (cell, state->contribution_trackers)
  {
    int aid_index = foreach_current_index(cell) + AIDS_OFFSET;
    ContributionTrackerState *contribution_tracker = (ContributionTrackerState *)lfirst(cell);
    if (!PG_ARGISNULL(aid_index))
    {
      aid_t aid = contribution_tracker->aid_descriptor.make_aid(PG_GETARG_DATUM(aid_index));
      contribution_tracker_update_contribution(contribution_tracker, aid, one_contribution);
    }
    else
    {
      contribution_tracker->unaccounted_for++;
    }
  }
}

const AnonAggFuncs g_count_funcs = {
    count_agg_final_type,
    agg_create_state,
    agg_transition,
    count_agg_finalize,
    count_agg_merge,
    count_agg_explain,
};

/*-------------------------------------------------------------------------
 * UDFs
 *-------------------------------------------------------------------------
 */

PG_FUNCTION_INFO_V1(anon_count_transfn);
PG_FUNCTION_INFO_V1(anon_count_finalfn);
PG_FUNCTION_INFO_V1(anon_count_explain_finalfn);

Datum anon_count_transfn(PG_FUNCTION_ARGS)
{
  AnonAggState *state = count_agg_get_state(fcinfo, AIDS_OFFSET);
  agg_transition(state, fcinfo);
  PG_RETURN_POINTER(state);
}

Datum anon_count_finalfn(PG_FUNCTION_ARGS)
{
  bool is_null = false;
  Datum result = count_agg_finalize(count_agg_get_state(fcinfo, AIDS_OFFSET), NULL, NULL, &is_null);
  Assert(!is_null);
  PG_RETURN_DATUM(result);
}

Datum anon_count_explain_finalfn(PG_FUNCTION_ARGS)
{
  PG_RETURN_TEXT_P(cstring_to_text(count_agg_explain(count_agg_get_state(fcinfo, AIDS_OFFSET))));
}
