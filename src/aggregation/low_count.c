#include "postgres.h"

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

static AidResult calculate_aid_result(seed_t bucket_seed, const char *salt, const AidTrackerState *tracker)
{
  AidResult result = {.aid_seed = tracker->aid_seed};

  seed_t seeds[] = {bucket_seed, tracker->aid_seed};
  result.threshold = generate_lcf_threshold(seeds, ARRAY_LENGTH(seeds), salt);
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
  int trackers_count;
  AidTrackerState *trackers[];
} LowCountState;

static AnonAggState *agg_create_state(MemoryContext memory_context, ArgsDescriptor *args_desc)
{
  MemoryContext old_context = MemoryContextSwitchTo(memory_context);

  int trackers_count = args_desc->num_args - AIDS_OFFSET;
  LowCountState *state = palloc0(sizeof(LowCountState) + trackers_count * sizeof(AidTrackerState *));
  state->trackers_count = trackers_count;
  for (int i = 0; i < trackers_count; i++)
  {
    Oid aid_type = args_desc->args[i + AIDS_OFFSET].type_oid;
    state->trackers[i] = aid_tracker_new(get_aid_mapper(aid_type));
  }

  MemoryContextSwitchTo(old_context);
  return &state->base;
}

static void agg_transition(AnonAggState *base_state, int num_args, NullableDatum *args)
{
  LowCountState *state = (LowCountState *)base_state;

  for (int i = 0; i < state->trackers_count; i++)
  {
    int aid_index = i + AIDS_OFFSET;
    if (!args[aid_index].isnull)
    {
      aid_t aid = state->trackers[i]->aid_mapper(args[aid_index].value);
      aid_tracker_update(state->trackers[i], aid);
    }
  }
}

static Datum agg_finalize(AnonAggState *base_state, Bucket *bucket, BucketDescriptor *bucket_desc, bool *is_null)
{
  LowCountState *state = (LowCountState *)base_state;

  bool low_count = false;
  seed_t bucket_seed = compute_bucket_seed(bucket, bucket_desc);

  for (int i = 0; i < state->trackers_count; i++)
  {
    AidResult result = calculate_aid_result(bucket_seed, bucket_desc->anon_context->salt, state->trackers[i]);
    low_count = low_count || result.low_count;
  }

  *is_null = false;
  return DatumGetBool(low_count);
}

static void agg_merge(AnonAggState *dst_base_state, const AnonAggState *src_base_state)
{
  LowCountState *dst_state = (LowCountState *)dst_base_state;
  const LowCountState *src_state = (const LowCountState *)src_base_state;

  Assert(dst_state->trackers_count == src_state->trackers_count);

  for (int i = 0; i < src_state->trackers_count; i++)
  {
    AidTrackerState *dst_tracker = dst_state->trackers[i];
    const AidTrackerState *src_tracker = src_state->trackers[i];

    AidTracker_iterator iterator;
    AidTracker_start_iterate(src_tracker->aid_set, &iterator);
    AidTrackerHashEntry *entry = NULL;
    while ((entry = AidTracker_iterate(src_tracker->aid_set, &iterator)) != NULL)
    {
      aid_tracker_update(dst_tracker, entry->aid);
    }
  }
}

static const char *agg_explain(const AnonAggState *base_state)
{
  return "diffix.lcf";
}

const AnonAggFuncs g_low_count_funcs = {
    .final_type = agg_final_type,
    .create_state = agg_create_state,
    .transition = agg_transition,
    .finalize = agg_finalize,
    .merge = agg_merge,
    .explain = agg_explain,
};
