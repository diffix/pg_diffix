#include "postgres.h"

#include <math.h>

#include "catalog/pg_type.h"
#include "utils/fmgrprotos.h"

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

typedef ContributionTrackerState *SumLeg;

typedef struct SumState
{
  AnonAggState base;
  int trackers_count;
  Oid summand_type;
  SumLeg *positive;
  SumLeg *negative;
} SumState;

static void sum_final_type(const ArgsDescriptor *args_desc, Oid *type, int32 *typmod, Oid *collid)
{
  Oid primary_arg_type = args_desc->args[1].type_oid;
  switch (primary_arg_type)
  {
  case INT2OID:
  case INT4OID:
    *type = INT8OID;
    break;
  case INT8OID:
  case NUMERICOID:
    *type = NUMERICOID;
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
  SumState *state = palloc0(sizeof(SumState));
  state->trackers_count = trackers_count;
  state->summand_type = args_desc->args[SUM_VALUE_INDEX].type_oid;
  state->positive = palloc0(trackers_count * sizeof(SumLeg));
  state->negative = palloc0(trackers_count * sizeof(SumLeg));
  ContributionDescriptor typed_sum_descriptor = {0};
  switch (state->summand_type)
  {
  case INT2OID:
  case INT4OID:
  case INT8OID:
    typed_sum_descriptor = integer_descriptor;
    break;
  case NUMERICOID:
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
    state->positive[i] = contribution_tracker_new(get_aid_mapper(aid_type), &typed_sum_descriptor);
    state->negative[i] = contribution_tracker_new(get_aid_mapper(aid_type), &typed_sum_descriptor);
  }

  MemoryContextSwitchTo(old_context);
  return &state->base;
}

typedef struct SumResult
{
  bool not_enough_aid_values;
  SummableResultAccumulator positive;
  SummableResultAccumulator negative;
} SumResult;

static SumResult sum_calculate_final(AnonAggState *base_state, Bucket *bucket, BucketDescriptor *bucket_desc)
{
  SumState *state = (SumState *)base_state;
  SummableResultAccumulator positive_result_accumulator = {0};
  SummableResultAccumulator negative_result_accumulator = {0};
  seed_t bucket_seed = compute_bucket_seed(bucket, bucket_desc);

  for (int i = 0; i < state->trackers_count; i++)
  {
    SummableResult positive_result = calculate_result(bucket_seed, state->positive[i]);
    SummableResult negative_result = calculate_result(bucket_seed, state->negative[i]);

    if (positive_result.not_enough_aid_values && negative_result.not_enough_aid_values)
    {
      return (SumResult){.not_enough_aid_values = true};
    }
    else
    {
      /* Unless both legs had `not_enough_aid_values` for given AID instance, we proceed. */
      accumulate_result(&positive_result_accumulator, &positive_result);
      accumulate_result(&negative_result_accumulator, &negative_result);
    }
  }
  return (SumResult){.positive = positive_result_accumulator, .negative = negative_result_accumulator};
}

static Datum sum_finalize(AnonAggState *base_state, Bucket *bucket, BucketDescriptor *bucket_desc, bool *is_null)
{
  SumState *state = (SumState *)base_state;
  SumResult result = sum_calculate_final(base_state, bucket, bucket_desc);

  /* We deliberately ignore the `not_enough_aid_values` fields in the `result.positive` and `negative`. */
  if (result.not_enough_aid_values)
  {
    *is_null = true;
    switch (state->summand_type)
    {
    case INT2OID:
    case INT4OID:
      return Int64GetDatum(0);
    case INT8OID:
    case NUMERICOID:
      return DirectFunctionCall1(float8_numeric, 0);
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
    double combined_result = finalize_sum_result(&result.positive) - finalize_sum_result(&result.negative);
    switch (state->summand_type)
    {
    case INT2OID:
    case INT4OID:
      return Int64GetDatum((int64)round(combined_result));
    case INT8OID:
    case NUMERICOID:
      return DirectFunctionCall1(float8_numeric, Float8GetDatum(combined_result));
    case FLOAT4OID:
      return Float4GetDatum((float4)combined_result);
    case FLOAT8OID:
      return Float8GetDatum(combined_result);
    default:
      Assert(false);
      return Float8GetDatum(combined_result);
    }
  }
}

static void sum_merge(AnonAggState *dst_base_state, const AnonAggState *src_base_state)
{
  SumState *dst_state = (SumState *)dst_base_state;
  const SumState *src_state = (const SumState *)src_base_state;

  Assert(dst_state->summand_type == src_state->summand_type);
  merge_trackers(dst_state->trackers_count, src_state->trackers_count, dst_state->positive, src_state->positive);
  merge_trackers(dst_state->trackers_count, src_state->trackers_count, dst_state->negative, src_state->negative);
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
    return (contribution_t){.real = DatumGetFloat8(DirectFunctionCall1(numeric_float8, arg))};
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
      ContributionDescriptor descriptor = state->positive[i]->contribution_descriptor;
      contribution_t abs_contribution = descriptor.contribution_abs(value_contribution);
      ContributionCombineFunc combine = descriptor.contribution_combine;
      ContributionGreaterFunc gt = descriptor.contribution_greater;
      ContributionEqualFunc eq = descriptor.contribution_equal;

      if (!args[aid_index].isnull)
      {
        aid_t aid = state->positive[i]->aid_mapper(args[aid_index].value);
        if (gt(value_contribution, descriptor.contribution_initial) || eq(value_contribution, descriptor.contribution_initial))
          contribution_tracker_update_contribution(state->positive[i], aid, abs_contribution);
        if (gt(descriptor.contribution_initial, value_contribution) || eq(value_contribution, descriptor.contribution_initial))
          contribution_tracker_update_contribution(state->negative[i], aid, abs_contribution);
      }
      else
      {
        if (gt(value_contribution, descriptor.contribution_initial))
          state->positive[i]->unaccounted_for = combine(state->positive[i]->unaccounted_for, abs_contribution);
        if (gt(descriptor.contribution_initial, value_contribution))
          state->negative[i]->unaccounted_for = combine(state->negative[i]->unaccounted_for, abs_contribution);
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

static void sum_noise_final_type(const ArgsDescriptor *args_desc, Oid *type, int32 *typmod, Oid *collid)
{
  *type = FLOAT8OID;
  *typmod = -1;
  *collid = 0;
}

static Datum sum_noise_finalize(AnonAggState *base_state, Bucket *bucket, BucketDescriptor *bucket_desc, bool *is_null)
{
  SumResult result = sum_calculate_final(base_state, bucket, bucket_desc);

  /* We deliberately ignore the `not_enough_aid_values` fields in the `result.positive` and `negative`. */
  if (result.not_enough_aid_values)
  {
    *is_null = true;
    return Float8GetDatum(0.0);
  }
  else
  {
    return Float8GetDatum(sqrt(pow(finalize_noise_result(&result.positive), 2) + pow(finalize_noise_result(&result.negative), 2)));
  }
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
