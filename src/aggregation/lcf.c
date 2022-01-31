#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include <inttypes.h>

#include "pg_diffix/aggregation/aid_tracker.h"
#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/aggregation/noise.h"

/* TODO: Implement aggregator methods. */
const AnonAggFuncs g_lcf_funcs = {0};

typedef struct LcfResult
{
  seed_t aid_seed;
  int threshold;
  bool passes_lcf;
} LcfResult;

static LcfResult lcf_calculate_final(const AidTrackerState *tracker);

PG_FUNCTION_INFO_V1(lcf_transfn);
PG_FUNCTION_INFO_V1(lcf_finalfn);
PG_FUNCTION_INFO_V1(lcf_explain_finalfn);

static const int LCF_AIDS_OFFSET = 1;

Datum lcf_transfn(PG_FUNCTION_ARGS)
{
  List *trackers = get_aggregate_aid_trackers(fcinfo, LCF_AIDS_OFFSET);

  Assert(PG_NARGS() == list_length(trackers) + LCF_AIDS_OFFSET);

  ListCell *cell;
  foreach (cell, trackers)
  {
    int aid_index = foreach_current_index(cell) + LCF_AIDS_OFFSET;
    if (!PG_ARGISNULL(aid_index))
    {
      AidTrackerState *tracker = (AidTrackerState *)lfirst(cell);
      aid_t aid = tracker->aid_descriptor.make_aid(PG_GETARG_DATUM(aid_index));
      aid_tracker_update(tracker, aid);
    }
  }

  PG_RETURN_POINTER(trackers);
}

Datum lcf_finalfn(PG_FUNCTION_ARGS)
{
  bool passes_lcf = true;
  List *trackers = get_aggregate_aid_trackers(fcinfo, LCF_AIDS_OFFSET);

  ListCell *cell;
  foreach (cell, trackers)
  {
    AidTrackerState *tracker = (AidTrackerState *)lfirst(cell);
    LcfResult result = lcf_calculate_final(tracker);
    passes_lcf = passes_lcf && result.passes_lcf;
  }

  PG_RETURN_BOOL(passes_lcf);
}

static void append_tracker_info(StringInfo string, const AidTrackerState *tracker)
{
  LcfResult result = lcf_calculate_final(tracker);

  appendStringInfo(string, "uniq=%" PRIu32, tracker->aid_set->members);

  appendStringInfo(string, ", thresh=%i, pass=%s",
                   result.threshold,
                   result.passes_lcf ? "true" : "false");

  appendStringInfo(string, ", aid_seed=%016" PRIx64, result.aid_seed);
}

Datum lcf_explain_finalfn(PG_FUNCTION_ARGS)
{
  StringInfoData string;
  initStringInfo(&string);

  List *trackers = get_aggregate_aid_trackers(fcinfo, LCF_AIDS_OFFSET);

  ListCell *cell;
  foreach (cell, trackers)
  {
    if (foreach_current_index(cell) > 0)
      appendStringInfo(&string, " \n");

    AidTrackerState *tracker = (AidTrackerState *)lfirst(cell);
    append_tracker_info(&string, tracker);
  }

  PG_RETURN_TEXT_P(cstring_to_text(string.data));
}

static LcfResult lcf_calculate_final(const AidTrackerState *tracker)
{
  LcfResult result = {0};

  result.aid_seed = tracker->aid_seed;
  result.threshold = generate_lcf_threshold(tracker->aid_seed);
  result.passes_lcf = tracker->aid_set->members >= result.threshold;

  return result;
}
