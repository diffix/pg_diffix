#include "postgres.h"

#include "fmgr.h"

#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/aggregation/count_common.h"
#include "pg_diffix/query/anonymization.h"

/* TODO: Implement aggregator methods. */
const AnonAggFuncs g_count_any_funcs = {0};

static const int VALUE_INDEX = 1;
static const int AIDS_OFFSET = 2;

PG_FUNCTION_INFO_V1(anon_count_any_transfn);
PG_FUNCTION_INFO_V1(anon_count_any_finalfn);
PG_FUNCTION_INFO_V1(anon_count_any_explain_finalfn);

Datum anon_count_any_transfn(PG_FUNCTION_ARGS)
{
  List *trackers = get_count_contribution_trackers(fcinfo, AIDS_OFFSET);

  if (all_aids_null(fcinfo, AIDS_OFFSET, list_length(trackers)))
    PG_RETURN_POINTER(trackers);

  ListCell *cell;
  foreach (cell, trackers)
  {
    int aid_index = foreach_current_index(cell) + AIDS_OFFSET;
    ContributionTrackerState *tracker = (ContributionTrackerState *)lfirst(cell);
    if (!PG_ARGISNULL(aid_index))
    {
      aid_t aid = tracker->aid_descriptor.make_aid(PG_GETARG_DATUM(aid_index));
      if (PG_ARGISNULL(VALUE_INDEX))
        /* No contribution since argument is NULL, only keep track of the AID value. */
        contribution_tracker_update_aid(tracker, aid);
      else
        contribution_tracker_update_contribution(tracker, aid, one_contribution);
    }
    else if (!PG_ARGISNULL(VALUE_INDEX))
    {
      tracker->unaccounted_for++;
    }
  }

  PG_RETURN_POINTER(trackers);
}

Datum anon_count_any_finalfn(PG_FUNCTION_ARGS)
{
  List *trackers = get_count_contribution_trackers(fcinfo, AIDS_OFFSET);
  return count_calculate_final(fcinfo, trackers);
}

Datum anon_count_any_explain_finalfn(PG_FUNCTION_ARGS)
{
  List *trackers = get_count_contribution_trackers(fcinfo, AIDS_OFFSET);
  seed_t bucket_seed = compute_bucket_seed();
  return explain_count_trackers(bucket_seed, trackers);
}
