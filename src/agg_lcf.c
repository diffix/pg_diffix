#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include <math.h>
#include <inttypes.h>

#include "pg_diffix/aid_tracker.h"
#include "pg_diffix/config.h"
#include "pg_diffix/random.h"

typedef struct LcfResult
{
  uint64 random_seed;
  int threshold;
  bool passes_lcf;
} LcfResult;

static LcfResult lcf_calculate_final(AidTrackerState *state);

PG_FUNCTION_INFO_V1(diffix_lcf_transfn);
PG_FUNCTION_INFO_V1(diffix_lcf_finalfn);
PG_FUNCTION_INFO_V1(diffix_lcf_explain_finalfn);

Datum diffix_lcf_transfn(PG_FUNCTION_ARGS)
{
  AidTrackerState *state = get_aggregate_aid_tracker(fcinfo);

  if (!PG_ARGISNULL(1))
  {
    aid_tracker_update(state, PG_GETARG_DATUM(1));
  }

  PG_RETURN_POINTER(state);
}

Datum diffix_lcf_finalfn(PG_FUNCTION_ARGS)
{
  AidTrackerState *state = get_aggregate_aid_tracker(fcinfo);
  LcfResult result = lcf_calculate_final(state);
  PG_RETURN_BOOL(result.passes_lcf);
}

Datum diffix_lcf_explain_finalfn(PG_FUNCTION_ARGS)
{
  AidTrackerState *state = get_aggregate_aid_tracker(fcinfo);
  LcfResult result = lcf_calculate_final(state);

  StringInfoData string;
  initStringInfo(&string);

  appendStringInfo(&string, "uniq=%" PRIu32, state->aid_set->members);

  /* Print only effective part of the seed. */
  uint16 *random_seed = (uint16 *)(&result.random_seed);
  appendStringInfo(&string,
                   ", seed=%04" PRIx16 "%04" PRIx16 "%04" PRIx16,
                   random_seed[0], random_seed[1], random_seed[2]);

  appendStringInfo(&string, "\nthresh=%i, pass=%s",
                   result.threshold,
                   result.passes_lcf ? "true" : "false");

  PG_RETURN_TEXT_P(cstring_to_text(string.data));
}

static LcfResult lcf_calculate_final(AidTrackerState *state)
{
  LcfResult result;
  uint64 seed = make_seed(state->aid_seed);

  result.random_seed = seed;

  /* Pick an integer in interval [min, min+2]. */
  const int LCF_RANGE = 2;
  result.threshold = next_uniform_int(
      &seed,
      Config.minimum_allowed_aids,
      Config.minimum_allowed_aids + LCF_RANGE + 1); /* +1 because max is exclusive. */

  result.passes_lcf = state->aid_set->members >= result.threshold;

  return result;
}
