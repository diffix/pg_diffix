#include "postgres.h"

#include "catalog/pg_type.h"

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
  List *aid_trackers;
} LowCountState;

static AnonAggState *agg_create_state(MemoryContext memory_context, ArgsDescriptor *args_desc)
{
  MemoryContext old_context = MemoryContextSwitchTo(memory_context);

  LowCountState *state = palloc0(sizeof(LowCountState));
  state->aid_trackers = create_aid_trackers(args_desc, AIDS_OFFSET);

  MemoryContextSwitchTo(old_context);
  return &state->base;
}

static void agg_transition(AnonAggState *base_state, int num_args, NullableDatum *args)
{
  LowCountState *state = (LowCountState *)base_state;

  Assert(num_args == list_length(state->aid_trackers) + AIDS_OFFSET);

  ListCell *cell = NULL;
  foreach (cell, state->aid_trackers)
  {
    int aid_index = foreach_current_index(cell) + AIDS_OFFSET;
    if (!args[aid_index].isnull)
    {
      AidTrackerState *aid_tracker = (AidTrackerState *)lfirst(cell);
      aid_t aid = aid_tracker->aid_descriptor.make_aid(args[aid_index].value);
      aid_tracker_update(aid_tracker, aid);
    }
  }
}

static Datum agg_finalize(AnonAggState *base_state, Bucket *bucket, BucketDescriptor *bucket_desc, bool *is_null)
{
  LowCountState *state = (LowCountState *)base_state;

  bool low_count = false;
  seed_t bucket_seed = compute_bucket_seed(bucket, bucket_desc);

  ListCell *cell;
  foreach (cell, state->aid_trackers)
  {
    AidTrackerState *aid_tracker = (AidTrackerState *)lfirst(cell);
    AidResult result = calculate_aid_result(bucket_seed, bucket_desc->anon_context->salt, aid_tracker);
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
