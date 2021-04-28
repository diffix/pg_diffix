#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include <inttypes.h>

#include "pg_diffix/config.h"
#include "pg_diffix/aggregation/aid_tracker.h"
#include "pg_diffix/aggregation/random.h"

typedef struct LcfResult
{
  uint64 random_seed;
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

  ListCell *lc;
  foreach (lc, trackers)
  {
    int aid_index = foreach_current_index(lc) + LCF_AIDS_OFFSET;
    if (!PG_ARGISNULL(aid_index))
    {
      AidTrackerState *tracker = (AidTrackerState *)lfirst(lc);
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

  ListCell *lc;
  foreach (lc, trackers)
  {
    AidTrackerState *tracker = (AidTrackerState *)lfirst(lc);
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

  /* Print only effective part of the seed. */
  uint16 *random_seed = (uint16 *)(&result.random_seed);
  appendStringInfo(string,
                   ", seed=%04" PRIx16 "%04" PRIx16 "%04" PRIx16,
                   random_seed[0], random_seed[1], random_seed[2]);
}

Datum lcf_explain_finalfn(PG_FUNCTION_ARGS)
{
  StringInfoData string;
  initStringInfo(&string);

  List *trackers = get_aggregate_aid_trackers(fcinfo, LCF_AIDS_OFFSET);

  ListCell *lc;
  foreach (lc, trackers)
  {
    if (foreach_current_index(lc) > 0)
      appendStringInfo(&string, " \n");

    AidTrackerState *tracker = (AidTrackerState *)lfirst(lc);
    append_tracker_info(&string, tracker);
  }

  PG_RETURN_TEXT_P(cstring_to_text(string.data));
}

static LcfResult lcf_calculate_final(const AidTrackerState *tracker)
{
  LcfResult result = {0};
  uint64 seed = make_seed(tracker->aid_seed);

  result.random_seed = seed;

  /* Pick an integer in interval [min, min+2]. */
  const int LCF_RANGE = 2;
  result.threshold = next_uniform_int(
      &seed,
      g_config.minimum_allowed_aid_values,
      g_config.minimum_allowed_aid_values + LCF_RANGE + 1); /* +1 because max is exclusive. */

  result.passes_lcf = tracker->aid_set->members >= result.threshold;

  return result;
}
