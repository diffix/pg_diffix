#include "postgres.h"

#include <math.h>

#include "catalog/pg_type.h"

#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/aggregation/summable.h"
#include "pg_diffix/config.h"
#include "pg_diffix/query/anonymization.h"

static double finalize_sum_result(const SummableResultAccumulator *accumulator)
{
  return accumulator->sum_for_flattening + accumulator->noise_with_max_sd;
}

/*-------------------------------------------------------------------------
 * Aggregation callbacks
 *-------------------------------------------------------------------------
 */

typedef struct SumState
{
  AnonAggState base;
  int trackers_count;
  Oid summand_type;
  ContributionTrackerState *trackers[FLEXIBLE_ARRAY_MEMBER];
} SumState;

static void sum_final_type(Oid primary_arg_type, Oid *type, int32 *typmod, Oid *collid)
{
  switch (primary_arg_type)
  {
  case INT2OID:
  case INT4OID:
    *type = INT8OID;
    break;
  case INT8OID:
    // FIXME should  be numeric
    *type = INT8OID;
    break;
  case NUMERICOID:
    FAILWITH("sum(numeric) not supported");
    break;
  case FLOAT4OID:
    *type = FLOAT4OID;
    break;
  case FLOAT8OID:
    *type = FLOAT8OID;
    break;
  default:
    Assert(false);
    *type = FLOAT8OID;
  }
  *typmod = -1;
  *collid = 0;
}

static const int SUM_VALUE_INDEX = 1;
static const int SUM_AIDS_OFFSET = 2;

static AnonAggState *sum_create_state(MemoryContext memory_context, ArgsDescriptor *args_desc)
{
  MemoryContext old_context = MemoryContextSwitchTo(memory_context);

  int trackers_count = args_desc->num_args - SUM_AIDS_OFFSET;
  SumState *state = palloc0(sizeof(SumState) + trackers_count * sizeof(ContributionTrackerState *));
  state->trackers_count = trackers_count;
  state->summand_type = args_desc->args[SUM_VALUE_INDEX].type_oid;
  ContributionDescriptor typed_sum_descriptor = {0};
  switch (state->summand_type)
  {

  case INT2OID:
  case INT4OID:
  case INT8OID:
    typed_sum_descriptor = integer_descriptor;
    break;
  case NUMERICOID:
    FAILWITH("sum(numeric) not supported");
    break;
  case FLOAT4OID:
  case FLOAT8OID:
    typed_sum_descriptor = real_descriptor;
    break;
  default:
    Assert(false);
    typed_sum_descriptor = real_descriptor;
  }
  for (int i = 0; i < trackers_count; i++)
  {
    Oid aid_type = args_desc->args[i + SUM_AIDS_OFFSET].type_oid;
    state->trackers[i] = contribution_tracker_new(get_aid_mapper(aid_type), &typed_sum_descriptor);
  }

  MemoryContextSwitchTo(old_context);
  return &state->base;
}

static SummableResultAccumulator sum_calculate_final(AnonAggState *base_state, Bucket *bucket, BucketDescriptor *bucket_desc, bool *is_null)
{
  SumState *state = (SumState *)base_state;
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

static Datum sum_finalize(AnonAggState *base_state, Bucket *bucket, BucketDescriptor *bucket_desc, bool *is_null)
{
  SumState *state = (SumState *)base_state;
  SummableResultAccumulator result_accumulator = sum_calculate_final(base_state, bucket, bucket_desc, is_null);

  if (result_accumulator.not_enough_aid_values)
  {
    *is_null = true;
    switch (state->summand_type)
    {
    case INT2OID:
    case INT4OID:
      return Int64GetDatum(0);
    case INT8OID:
      // FIXME should  be numeric
      return Int64GetDatum(0);
    case FLOAT4OID:
      return Float4GetDatum(0);
    case FLOAT8OID:
      return Float8GetDatum(0);
    default:
      Assert(false);
      return Float8GetDatum(0);
    }
  }
  else
  {
    switch (state->summand_type)
    {
    case INT2OID:
    case INT4OID:
      return Int64GetDatum((int64)round(finalize_sum_result(&result_accumulator)));
    case INT8OID:
      // FIXME should  be numeric
      return Int64GetDatum((int64)round(finalize_sum_result(&result_accumulator)));
    case FLOAT4OID:
      return Float4GetDatum((float4)finalize_sum_result(&result_accumulator));
    case FLOAT8OID:
      return Float8GetDatum(finalize_sum_result(&result_accumulator));
    default:
      Assert(false);
      return Float8GetDatum(finalize_sum_result(&result_accumulator));
    }
  }
}

static void sum_merge(AnonAggState *dst_base_state, const AnonAggState *src_base_state)
{
  SumState *dst_state = (SumState *)dst_base_state;
  const SumState *src_state = (const SumState *)src_base_state;

  Assert(dst_state->summand_type == src_state->summand_type);
  merge_trackers(dst_state->trackers_count, src_state->trackers_count, dst_state->trackers, src_state->trackers);
}

static contribution_t summand_to_contribution(Datum arg, Oid summand_type)
{
  switch (summand_type)
  {
  case INT2OID:
    return (contribution_t){.integer = DatumGetInt16(arg)};
  case INT4OID:
    return (contribution_t){.integer = DatumGetInt32(arg)};
  case INT8OID:
    return (contribution_t){.integer = DatumGetInt64(arg)};
  case NUMERICOID:
    // FIXME support it
    FAILWITH("sum(numeric) not supported yet");
    return (contribution_t){.real = 0.0};
  case FLOAT4OID:
    return (contribution_t){.real = DatumGetFloat4(arg)};
  case FLOAT8OID:
    return (contribution_t){.real = DatumGetFloat8(arg)};
  default:
    Assert(false);
    return (contribution_t){.real = 0.0};
  }
}

static void sum_transition(AnonAggState *base_state, int num_args, NullableDatum *args)
{
  SumState *state = (SumState *)base_state;

  if (all_aids_null(args, SUM_AIDS_OFFSET, state->trackers_count))
    return;

  if (!args[SUM_VALUE_INDEX].isnull)
  /* We're completely ignoring `NULL`, contrary to `count(col)` where it contributes 0. */
  {
    contribution_t value_contribution = summand_to_contribution(args[SUM_VALUE_INDEX].value, state->summand_type);
    for (int i = 0; i < state->trackers_count; i++)
    {
      int aid_index = i + SUM_AIDS_OFFSET;

      if (!args[aid_index].isnull)
      {
        aid_t aid = state->trackers[i]->aid_mapper(args[aid_index].value);
        contribution_tracker_update_contribution(state->trackers[i], aid, value_contribution);
      }
      else
      {
        ContributionCombineFunc combine = state->trackers[i]->contribution_descriptor.contribution_combine;
        state->trackers[i]->unaccounted_for = combine(state->trackers[i]->unaccounted_for, value_contribution);
      }
    }
  }
}

static const char *sum_explain(const AnonAggState *base_state)
{
  return "diffix.anon_sum";
}

const AnonAggFuncs g_sum_funcs = {
    .final_type = sum_final_type,
    .create_state = sum_create_state,
    .transition = sum_transition,
    .finalize = sum_finalize,
    .merge = sum_merge,
    .explain = sum_explain,
};

static void sum_noise_final_type(Oid primary_arg_type, Oid *type, int32 *typmod, Oid *collid)
{
  *type = FLOAT8OID;
  *typmod = -1;
  *collid = 0;
}

static Datum sum_noise_finalize(AnonAggState *base_state, Bucket *bucket, BucketDescriptor *bucket_desc, bool *is_null)
{
  SummableResultAccumulator result_accumulator = sum_calculate_final(base_state, bucket, bucket_desc, is_null);
  if (result_accumulator.not_enough_aid_values)
    return Float8GetDatum(0.0);
  else
    return Float8GetDatum(finalize_noise_result(&result_accumulator));
}

static const char *sum_noise_explain(const AnonAggState *base_state)
{
  return "diffix.anon_sum_noise";
}

const AnonAggFuncs g_sum_noise_funcs = {
    .final_type = sum_noise_final_type,
    .create_state = sum_create_state,
    .transition = sum_transition,
    .finalize = sum_noise_finalize,
    .merge = sum_merge,
    .explain = sum_noise_explain,
};
