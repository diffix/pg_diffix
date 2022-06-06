#include "postgres.h"

#include <math.h>

#include "catalog/pg_type.h"

#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/aggregation/count.h"
#include "pg_diffix/aggregation/summable.h"
#include "pg_diffix/config.h"
#include "pg_diffix/query/anonymization.h"

static const contribution_t zero_contribution = {.integer = 0};
static const contribution_t one_contribution = {.integer = 1};

int64 finalize_count_result(const SummableResultAccumulator *accumulator)
{
  return (int64)round(accumulator->sum_for_flattening + accumulator->noise_with_max_sd);
}

/*-------------------------------------------------------------------------
 * Aggregation callbacks
 *-------------------------------------------------------------------------
 */

typedef struct CountState
{
  AnonAggState base;
  int trackers_count;
  ContributionTrackerState *trackers[FLEXIBLE_ARRAY_MEMBER];
} CountState;

static void count_final_type(const ArgsDescriptor *args_desc, Oid *type, int32 *typmod, Oid *collid)
{
  *type = INT8OID;
  *typmod = -1;
  *collid = 0;
}

static AnonAggState *count_create_state(MemoryContext memory_context, ArgsDescriptor *args_desc, int aids_offset)
{
  MemoryContext old_context = MemoryContextSwitchTo(memory_context);

  int trackers_count = args_desc->num_args - aids_offset;
  CountState *state = palloc0(sizeof(CountState) + trackers_count * sizeof(ContributionTrackerState *));
  state->trackers_count = trackers_count;
  for (int i = 0; i < trackers_count; i++)
  {
    Oid aid_type = args_desc->args[i + aids_offset].type_oid;
    state->trackers[i] = contribution_tracker_new(get_aid_mapper(aid_type), &integer_descriptor);
  }

  MemoryContextSwitchTo(old_context);
  return &state->base;
}

static SummableResultAccumulator count_calculate_final(AnonAggState *base_state, Bucket *bucket, BucketDescriptor *bucket_desc, bool *is_null)
{
  CountState *state = (CountState *)base_state;
  SummableResultAccumulator result_accumulator = {0};
  seed_t bucket_seed = compute_bucket_seed(bucket, bucket_desc);

  for (int i = 0; i < state->trackers_count; i++)
  {
    SummableResult result = calculate_result(bucket_seed, state->trackers[i]);

    accumulate_result(&result_accumulator, &result);
    if (result_accumulator.not_enough_aid_values)
      break;
  }
  return result_accumulator;
}

static Datum count_finalize(AnonAggState *base_state, Bucket *bucket, BucketDescriptor *bucket_desc, bool *is_null)
{
  SummableResultAccumulator result_accumulator = count_calculate_final(base_state, bucket, bucket_desc, is_null);
  bool is_global = bucket_desc->num_labels == 0;
  int64 min_count = is_global ? 0 : g_config.low_count_min_threshold;
  if (result_accumulator.not_enough_aid_values)
    return Int64GetDatum(min_count);
  else
    return Int64GetDatum(Max(finalize_count_result(&result_accumulator), min_count));
}

static void count_merge(AnonAggState *dst_base_state, const AnonAggState *src_base_state)
{
  CountState *dst_state = (CountState *)dst_base_state;
  const CountState *src_state = (const CountState *)src_base_state;
  merge_trackers(dst_state->trackers_count, src_state->trackers_count, dst_state->trackers, src_state->trackers);
}

static const int COUNT_VALUE_INDEX = 1;
static const int COUNT_VALUE_AIDS_OFFSET = 2;

static AnonAggState *count_value_create_state(MemoryContext memory_context, ArgsDescriptor *args_desc)
{
  return count_create_state(memory_context, args_desc, COUNT_VALUE_AIDS_OFFSET);
}

static void count_value_transition(AnonAggState *base_state, int num_args, NullableDatum *args)
{
  CountState *state = (CountState *)base_state;

  if (all_aids_null(args, COUNT_VALUE_AIDS_OFFSET, state->trackers_count))
    return;

  for (int i = 0; i < state->trackers_count; i++)
  {
    int aid_index = i + COUNT_VALUE_AIDS_OFFSET;
    if (!args[aid_index].isnull)
    {
      aid_t aid = state->trackers[i]->aid_mapper(args[aid_index].value);
      if (args[COUNT_VALUE_INDEX].isnull)
        /* No contribution since argument is NULL, only keep track of the AID value. */
        contribution_tracker_update_contribution(state->trackers[i], aid, zero_contribution);
      else
        contribution_tracker_update_contribution(state->trackers[i], aid, one_contribution);
    }
    else if (!args[COUNT_VALUE_INDEX].isnull)
    {
      ContributionCombineFunc combine = state->trackers[i]->contribution_descriptor.contribution_combine;
      state->trackers[i]->unaccounted_for = combine(state->trackers[i]->unaccounted_for, one_contribution);
    }
  }
}

static const char *count_value_explain(const AnonAggState *base_state)
{
  return "diffix.anon_count_value";
}

const AnonAggFuncs g_count_value_funcs = {
    .final_type = count_final_type,
    .create_state = count_value_create_state,
    .transition = count_value_transition,
    .finalize = count_finalize,
    .merge = count_merge,
    .explain = count_value_explain,
};

static const int COUNT_STAR_AIDS_OFFSET = 1;

static AnonAggState *count_star_create_state(MemoryContext memory_context, ArgsDescriptor *args_desc)
{
  return count_create_state(memory_context, args_desc, COUNT_STAR_AIDS_OFFSET);
}

static void count_star_transition(AnonAggState *base_state, int num_args, NullableDatum *args)
{
  CountState *state = (CountState *)base_state;

  if (all_aids_null(args, COUNT_STAR_AIDS_OFFSET, state->trackers_count))
    return;

  for (int i = 0; i < state->trackers_count; i++)
  {
    int aid_index = i + COUNT_STAR_AIDS_OFFSET;
    if (!args[aid_index].isnull)
    {
      aid_t aid = state->trackers[i]->aid_mapper(args[aid_index].value);
      contribution_tracker_update_contribution(state->trackers[i], aid, one_contribution);
    }
    else
    {
      ContributionCombineFunc combine = state->trackers[i]->contribution_descriptor.contribution_combine;
      state->trackers[i]->unaccounted_for = combine(state->trackers[i]->unaccounted_for, one_contribution);
    }
  }
}

static const char *count_star_explain(const AnonAggState *base_state)
{
  return "diffix.anon_count_star";
}

const AnonAggFuncs g_count_star_funcs = {
    .final_type = count_final_type,
    .create_state = count_star_create_state,
    .transition = count_star_transition,
    .finalize = count_finalize,
    .merge = count_merge,
    .explain = count_star_explain,
};

static void count_noise_final_type(const ArgsDescriptor *args_desc, Oid *type, int32 *typmod, Oid *collid)
{
  *type = FLOAT8OID;
  *typmod = -1;
  *collid = 0;
}

static Datum count_noise_finalize(AnonAggState *base_state, Bucket *bucket, BucketDescriptor *bucket_desc, bool *is_null)
{
  SummableResultAccumulator result_accumulator = count_calculate_final(base_state, bucket, bucket_desc, is_null);
  if (result_accumulator.not_enough_aid_values)
    return Float8GetDatum(0.0);
  else
    return Float8GetDatum(finalize_noise_result(&result_accumulator));
}

static const char *count_value_noise_explain(const AnonAggState *base_state)
{
  return "diffix.anon_count_value_noise";
}

static const char *count_noise_explain(const AnonAggState *base_state)
{
  return "diffix.anon_count_star_noise";
}

const AnonAggFuncs g_count_value_noise_funcs = {
    .final_type = count_noise_final_type,
    .create_state = count_value_create_state,
    .transition = count_value_transition,
    .finalize = count_noise_finalize,
    .merge = count_merge,
    .explain = count_value_noise_explain,
};

const AnonAggFuncs g_count_star_noise_funcs = {
    .final_type = count_noise_final_type,
    .create_state = count_star_create_state,
    .transition = count_star_transition,
    .finalize = count_noise_finalize,
    .merge = count_merge,
    .explain = count_noise_explain,
};
