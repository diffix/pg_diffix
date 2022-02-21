#include "postgres.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include <inttypes.h>

#include "pg_diffix/aggregation/aid_tracker.h"
#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/aggregation/noise.h"
#include "pg_diffix/query/anonymization.h"

typedef struct AidResult
{
  seed_t aid_seed;
  int threshold;
  bool low_count;
} AidResult;

static AidResult calculate_aid_result(seed_t bucket_seed, const AidTrackerState *tracker)
{
  AidResult result = {.aid_seed = tracker->aid_seed};

  seed_t seeds[] = {bucket_seed, tracker->aid_seed};
  result.threshold = generate_lcf_threshold(seeds, ARRAY_LENGTH(seeds));
  result.low_count = tracker->aid_set->members < result.threshold;

  return result;
}

/*-------------------------------------------------------------------------
 * Aggregation callbacks
 *-------------------------------------------------------------------------
 */

static const int AIDS_OFFSET = 1;

static void agg_final_type(Oid *type, int32 *typmod, Oid *collid)
{
  *type = BOOLOID;
  *typmod = -1;
  *collid = 0;
}

typedef struct LowCountState
{
  AnonAggState base;
  List *aid_trackers;
} LowCountState;

static AnonAggState *agg_create_state(MemoryContext memory_context, PG_FUNCTION_ARGS)
{
  MemoryContext old_context = MemoryContextSwitchTo(memory_context);

  LowCountState *state = (LowCountState *)palloc0(sizeof(LowCountState));
  state->aid_trackers = create_aid_trackers(fcinfo, AIDS_OFFSET);

  MemoryContextSwitchTo(old_context);
  return &state->base;
}

static void agg_transition(AnonAggState *base_state, PG_FUNCTION_ARGS)
{
  LowCountState *state = (LowCountState *)base_state;

  Assert(PG_NARGS() == list_length(state->aid_trackers) + AIDS_OFFSET);

  ListCell *cell = NULL;
  foreach (cell, state->aid_trackers)
  {
    int aid_index = foreach_current_index(cell) + AIDS_OFFSET;
    if (!PG_ARGISNULL(aid_index))
    {
      AidTrackerState *aid_tracker = (AidTrackerState *)lfirst(cell);
      aid_t aid = aid_tracker->aid_descriptor.make_aid(PG_GETARG_DATUM(aid_index));
      aid_tracker_update(aid_tracker, aid);
    }
  }
}

static Datum agg_finalize(AnonAggState *base_state, Bucket *bucket, BucketDescriptor *bucket_desc, bool *is_null)
{
  LowCountState *state = (LowCountState *)base_state;

  bool low_count = false;
  seed_t bucket_seed = compute_bucket_seed();

  ListCell *cell;
  foreach (cell, state->aid_trackers)
  {
    AidTrackerState *aid_tracker = (AidTrackerState *)lfirst(cell);
    AidResult result = calculate_aid_result(bucket_seed, aid_tracker);
    low_count = low_count || result.low_count;
  }

  *is_null = false;
  return DatumGetBool(low_count);
}

static void agg_merge(AnonAggState *dst_base_state, const AnonAggState *src_base_state)
{
  LowCountState *dst_state = (LowCountState *)dst_base_state;
  const LowCountState *src_state = (const LowCountState *)src_base_state;

  Assert(list_length(dst_state->aid_trackers) == list_length(src_state->aid_trackers));

  ListCell *dst_cell = NULL;
  const ListCell *src_cell = NULL;
  forboth(dst_cell, dst_state->aid_trackers, src_cell, src_state->aid_trackers)
  {
    AidTrackerState *dst_aid_tracker = (AidTrackerState *)lfirst(dst_cell);
    const AidTrackerState *src_aid_tracker = (const AidTrackerState *)lfirst(src_cell);

    AidTracker_iterator iterator;
    AidTracker_start_iterate(src_aid_tracker->aid_set, &iterator);
    AidTrackerHashEntry *entry = NULL;
    while ((entry = AidTracker_iterate(src_aid_tracker->aid_set, &iterator)) != NULL)
    {
      aid_tracker_update(dst_aid_tracker, entry->aid);
    }
  }
}

static void append_tracker_info(StringInfo string, seed_t bucket_seed, const AidTrackerState *tracker)
{
  AidResult result = calculate_aid_result(bucket_seed, tracker);

  appendStringInfo(string, "uniq=%" PRIu32, tracker->aid_set->members);

  appendStringInfo(string, ", thresh=%i, LC=%s",
                   result.threshold,
                   result.low_count ? "true" : "false");

  appendStringInfo(string, ", seeds: bkt=%016" PRIx64 ", aid=%016" PRIx64,
                   bucket_seed, result.aid_seed);
}

static const char *agg_explain(const AnonAggState *base_state)
{
  LowCountState *state = (LowCountState *)base_state;

  StringInfoData string;
  initStringInfo(&string);

  seed_t bucket_seed = compute_bucket_seed();

  ListCell *cell;
  foreach (cell, state->aid_trackers)
  {
    if (foreach_current_index(cell) > 0)
      appendStringInfo(&string, " \n");

    AidTrackerState *aid_tracker = (AidTrackerState *)lfirst(cell);
    append_tracker_info(&string, bucket_seed, aid_tracker);
  }

  return string.data;
}

const AnonAggFuncs g_low_count_funcs = {
    agg_final_type,
    agg_create_state,
    agg_transition,
    agg_finalize,
    agg_merge,
    agg_explain,
};

/*-------------------------------------------------------------------------
 * UDFs
 *-------------------------------------------------------------------------
 */

PG_FUNCTION_INFO_V1(lcf_transfn);
PG_FUNCTION_INFO_V1(lcf_finalfn);
PG_FUNCTION_INFO_V1(lcf_explain_finalfn);

static const int STATE_INDEX = 0;

static AnonAggState *agg_get_state(PG_FUNCTION_ARGS)
{
  if (!PG_ARGISNULL(STATE_INDEX))
    return (AnonAggState *)PG_GETARG_POINTER(STATE_INDEX);

  /* We want all memory allocations to be done per aggregation node. */
  MemoryContext memory_context;
  if (AggCheckCallContext(fcinfo, &memory_context) != AGG_CONTEXT_AGGREGATE)
    FAILWITH("Aggregate called in non-aggregate context");

  return agg_create_state(memory_context, fcinfo);
}

Datum lcf_transfn(PG_FUNCTION_ARGS)
{
  AnonAggState *state = agg_get_state(fcinfo);
  agg_transition(state, fcinfo);
  PG_RETURN_POINTER(state);
}

Datum lcf_finalfn(PG_FUNCTION_ARGS)
{
  bool is_null = false;
  bool low_count = DatumGetBool(agg_finalize(agg_get_state(fcinfo), NULL, NULL, &is_null));
  Assert(!is_null);
  PG_RETURN_BOOL(!low_count);
}

Datum lcf_explain_finalfn(PG_FUNCTION_ARGS)
{
  PG_RETURN_TEXT_P(cstring_to_text(agg_explain(agg_get_state(fcinfo))));
}
