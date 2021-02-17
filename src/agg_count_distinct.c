#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include <math.h>
#include <inttypes.h>

#include "pg_diffix/aid_tracker.h"
#include "pg_diffix/config.h"
#include "pg_diffix/random.h"

typedef struct CountDistinctResult
{
  uint64 random_seed;
  int64 noisy_count;
} CountDistinctResult;

static CountDistinctResult count_distinct_calculate_final(AidTrackerState *state);

PG_FUNCTION_INFO_V1(diffix_count_distinct_transfn);
PG_FUNCTION_INFO_V1(diffix_count_distinct_finalfn);
PG_FUNCTION_INFO_V1(diffix_count_distinct_explain_finalfn);

Datum diffix_count_distinct_transfn(PG_FUNCTION_ARGS)
{
  AidTrackerState *state = get_aggregate_aid_tracker(fcinfo);

  if (!PG_ARGISNULL(1))
  {
    aid_tracker_update(state, PG_GETARG_DATUM(1));
  }

  PG_RETURN_POINTER(state);
}

Datum diffix_count_distinct_finalfn(PG_FUNCTION_ARGS)
{
  AidTrackerState *state = get_aggregate_aid_tracker(fcinfo);
  CountDistinctResult result = count_distinct_calculate_final(state);
  PG_RETURN_INT64(result.noisy_count);
}

Datum diffix_count_distinct_explain_finalfn(PG_FUNCTION_ARGS)
{
  AidTrackerState *state = get_aggregate_aid_tracker(fcinfo);
  CountDistinctResult result = count_distinct_calculate_final(state);

  /*
   * Split seed into 4 shorts, because only the first 3 will be effective in the rng.
   */
  uint16 *random_seed = (uint16 *)(&result.random_seed);

  StringInfoData string;
  initStringInfo(&string);

  appendStringInfo(&string, "uniq=%" PRIu32, state->aid_set->members);

  /* Print only effective part of the seed. */
  appendStringInfo(&string,
                   ", seed=%04" PRIx16 "%04" PRIx16 "%04" PRIx16,
                   random_seed[0], random_seed[1], random_seed[2]);

  appendStringInfo(&string, "\nnoisy_count=%" PRIi64, result.noisy_count);

  PG_RETURN_TEXT_P(cstring_to_text(string.data));
}

static CountDistinctResult count_distinct_calculate_final(AidTrackerState *state)
{
  CountDistinctResult result;
  uint64 seed = make_seed(state->aid_seed);
  result.random_seed = seed;
  result.noisy_count = apply_noise(state->aid_set->members, &seed);
  return result;
}
