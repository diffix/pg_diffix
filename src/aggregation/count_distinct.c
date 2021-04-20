#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include <math.h>
#include <inttypes.h>

#include "pg_diffix/config.h"
#include "pg_diffix/aggregation/aid_tracker.h"
#include "pg_diffix/aggregation/random.h"

typedef struct CountDistinctResult
{
  uint64 random_seed;
  int64 noisy_count;
} CountDistinctResult;

static CountDistinctResult count_distinct_calculate_final(AidTrackerState *state);

PG_FUNCTION_INFO_V1(anon_count_distinct_transfn);
PG_FUNCTION_INFO_V1(anon_count_distinct_finalfn);
PG_FUNCTION_INFO_V1(anon_count_distinct_explain_finalfn);

Datum anon_count_distinct_transfn(PG_FUNCTION_ARGS)
{
  AidTrackerState *state = get_aggregate_aid_tracker(fcinfo);

  if (!PG_ARGISNULL(1))
  {
    aid_t aid = state->aid_descriptor.make_aid(PG_GETARG_DATUM(1));
    aid_tracker_update(state, aid);
  }

  PG_RETURN_POINTER(state);
}

Datum anon_count_distinct_finalfn(PG_FUNCTION_ARGS)
{
  AidTrackerState *state = get_aggregate_aid_tracker(fcinfo);
  CountDistinctResult result = count_distinct_calculate_final(state);
  PG_RETURN_INT64(result.noisy_count);
}

Datum anon_count_distinct_explain_finalfn(PG_FUNCTION_ARGS)
{
  AidTrackerState *state = get_aggregate_aid_tracker(fcinfo);
  CountDistinctResult result = count_distinct_calculate_final(state);

  StringInfoData string;
  initStringInfo(&string);

  appendStringInfo(&string, "uniq=%" PRIu32, state->aid_set->members);

  /* Print only effective part of the seed. */
  uint16 *random_seed = (uint16 *)(&result.random_seed);
  appendStringInfo(&string,
                   ", seed=%04" PRIx16 "%04" PRIx16 "%04" PRIx16,
                   random_seed[0], random_seed[1], random_seed[2]);

  appendStringInfo(&string, "\nnoisy_count=%" PRIi64, result.noisy_count);

  PG_RETURN_TEXT_P(cstring_to_text(string.data));
}

static CountDistinctResult count_distinct_calculate_final(AidTrackerState *state)
{
  CountDistinctResult result = {0};
  uint64 seed = make_seed(state->aid_seed);

  result.random_seed = seed;
  result.noisy_count = state->aid_set->members + (int64)round(generate_noise(&seed, g_config.noise_sigma));
  result.noisy_count = Max(result.noisy_count, 0);

  return result;
}
