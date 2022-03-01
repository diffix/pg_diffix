#include "postgres.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include <inttypes.h>
#include <math.h>

#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/aggregation/count.h"
#include "pg_diffix/aggregation/noise.h"
#include "pg_diffix/config.h"
#include "pg_diffix/query/anonymization.h"

static bool contribution_greater(contribution_t x, contribution_t y)
{
  return x.integer > y.integer;
}

static bool contribution_equal(contribution_t x, contribution_t y)
{
  return x.integer == y.integer;
}

static contribution_t contribution_combine(contribution_t x, contribution_t y)
{
  return (contribution_t){.integer = x.integer + y.integer};
}

const ContributionDescriptor count_descriptor = {
    .contribution_greater = contribution_greater,
    .contribution_equal = contribution_equal,
    .contribution_combine = contribution_combine,
    .contribution_initial = {.integer = 0},
};

static seed_t contributors_seed(const Contributor *contributors, int count)
{
  seed_t seed = 0;
  for (int i = 0; i < count; i++)
    seed ^= contributors[i].aid;
  return seed;
}

static void determine_outlier_top_counts(
    uint64 total_count,
    const Contributors *top_contributors,
    CountResult *result)
{
  /* Compact flattening intervals */
  int total_adjustment = g_config.outlier_count_max + g_config.top_count_max - total_count;
  int compact_outlier_count_max = g_config.outlier_count_max;
  int compact_top_count_max = g_config.top_count_max;

  if (total_adjustment > 0)
  {
    int outlier_range = g_config.outlier_count_max - g_config.outlier_count_min;

    if (outlier_range > g_config.top_count_max - g_config.top_count_min)
    {
      FAILWITH("Invalid config: (outlier_count_min, outlier_count_max) wider than (top_count_min, top_count_max)");
    }

    if (outlier_range < total_adjustment / 2)
    {
      compact_outlier_count_max -= outlier_range;
      compact_top_count_max -= total_adjustment - outlier_range;
    }
    else
    {
      compact_outlier_count_max -= total_adjustment / 2;
      compact_top_count_max -= total_adjustment - total_adjustment / 2;
    }
  }

  /* Determine noisy outlier/top counts. */
  seed_t flattening_seed = contributors_seed(
      top_contributors->members, compact_outlier_count_max + compact_top_count_max);

  result->noisy_outlier_count = generate_uniform_noise(
      flattening_seed, "outlier", g_config.outlier_count_min, compact_outlier_count_max);
  result->noisy_top_count = generate_uniform_noise(
      flattening_seed, "top", g_config.top_count_min, compact_top_count_max);
}

CountResult aggregate_count_contributions(
    seed_t bucket_seed, seed_t aid_seed,
    uint64 true_count, uint64 distinct_contributors, uint64 unacounted_for,
    const Contributors *top_contributors)
{
  CountResult result = {0};

  result.aid_seed = aid_seed;
  result.true_count = true_count;

  if (distinct_contributors < g_config.outlier_count_min + g_config.top_count_min)
  {
    result.not_enough_aidvs = true;
    return result;
  }

  determine_outlier_top_counts(distinct_contributors, top_contributors, &result);

  uint32 top_end_index = result.noisy_outlier_count + result.noisy_top_count;

  /* Remove outliers from overall count. */
  for (uint32 i = 0; i < result.noisy_outlier_count; i++)
    result.flattening += (double)top_contributors->members[i].contribution.integer;

  /* Compute average of top values. */
  uint64 top_contribution = 0;
  for (uint32 i = result.noisy_outlier_count; i < top_end_index; i++)
    top_contribution += top_contributors->members[i].contribution.integer;
  double top_average = top_contribution / (double)result.noisy_top_count;

  /* Compensate for dropped outliers. */
  result.flattening -= top_average * result.noisy_outlier_count;

  /* Compensate for the unaccounted for NULL-value AIDs. */
  double flattened_unaccounted_for = Max((double)unacounted_for - result.flattening, 0.0);

  result.flattened_count = result.true_count - result.flattening + flattened_unaccounted_for;

  double average = result.flattened_count / (double)distinct_contributors;
  double noise_scale = Max(average, 0.5 * top_average);
  result.noise_sd = g_config.noise_layer_sd * noise_scale;
  seed_t noise_layers[] = {bucket_seed, aid_seed};
  result.noise = generate_layered_noise(noise_layers, ARRAY_LENGTH(noise_layers), "noise", result.noise_sd);

  return result;
}

static CountResult count_calculate_result(seed_t bucket_seed, const ContributionTrackerState *tracker)
{
  return aggregate_count_contributions(
      bucket_seed,
      tracker->aid_seed,
      tracker->overall_contribution.integer,
      tracker->distinct_contributors,
      tracker->unaccounted_for,
      &tracker->top_contributors);
}

void accumulate_count_result(CountResultAccumulator *accumulator, const CountResult *result)
{
  Assert(result->not_enough_aidvs == false);
  Assert(result->flattening >= 0);

  if (result->flattening > accumulator->max_flattening)
  {
    /* Get the flattened count for the AID with the maximum flattening... */
    accumulator->max_flattening = result->flattening;
    accumulator->count_for_flattening = result->flattened_count;
  }
  else if (result->flattening == accumulator->max_flattening)
  {
    /* ...and resolve draws using the largest flattened count. */
    accumulator->count_for_flattening = Max(accumulator->count_for_flattening, result->flattened_count);
  }

  if (result->noise_sd > accumulator->max_noise_sd)
  {
    accumulator->max_noise_sd = result->noise_sd;
    accumulator->noise_with_max_sd = result->noise;
  }
}

int64 finalize_count_result(const CountResultAccumulator *accumulator)
{
  int64 rounded_noisy_count = (int64)round(accumulator->count_for_flattening + accumulator->noise_with_max_sd);
  return Max(rounded_noisy_count, 0);
}

/*-------------------------------------------------------------------------
 * Aggregation callbacks
 *-------------------------------------------------------------------------
 */

static bool all_aids_null(PG_FUNCTION_ARGS, int aids_offset, int aids_count)
{
  for (int aid_index = aids_offset; aid_index < aids_offset + aids_count; aid_index++)
  {
    if (!PG_ARGISNULL(aid_index))
      return false;
  }
  return true;
}

static void append_tracker_info(StringInfo string, seed_t bucket_seed,
                                const ContributionTrackerState *tracker)
{
  CountResult result = count_calculate_result(bucket_seed, tracker);

  appendStringInfo(string, "uniq=%" PRIu32, tracker->contribution_table->members);

  /* Top contributors */
  uint32 top_length = tracker->top_contributors.length;
  appendStringInfo(string, ", top=[");

  for (uint32 i = 0; i < top_length; i++)
  {
    const Contributor *contributor = &tracker->top_contributors.members[i];
    appendStringInfo(string, "%" CONTRIBUTION_INT_FMT "x%" PRIu64,
                     contributor->contribution.integer, contributor->aid);

    if (i == result.noisy_outlier_count - 1)
      appendStringInfo(string, " | ");
    else if (i < top_length - 1)
      appendStringInfo(string, ", ");
  }

  appendStringInfo(string, "]");

  appendStringInfo(string, ", true=%" PRIi64, result.true_count);

  if (result.not_enough_aidvs)
    appendStringInfo(string, ", insufficient AIDs");
  else
    appendStringInfo(string, ", flat=%.3f, noise=%.3f, SD=%.3f",
                     result.flattened_count, result.noise, result.noise_sd);

  appendStringInfo(string, ", seeds: bkt=%016" PRIx64 ", aid=%016" PRIx64,
                   bucket_seed, result.aid_seed);
}

typedef struct CountState
{
  AnonAggState base;
  List *contribution_trackers;
  bool is_global;
} CountState;

static void count_final_type(Oid *type, int32 *typmod, Oid *collid)
{
  *type = INT8OID;
  *typmod = -1;
  *collid = 0;
}

static AnonAggState *count_create_state(MemoryContext memory_context, PG_FUNCTION_ARGS, int aids_offset)
{
  MemoryContext old_context = MemoryContextSwitchTo(memory_context);

  CountState *state = (CountState *)palloc0(sizeof(CountState));
  state->is_global = is_global_aggregation(fcinfo);
  state->contribution_trackers = create_contribution_trackers(fcinfo, aids_offset, &count_descriptor);
  Assert(PG_NARGS() == list_length(state->contribution_trackers) + aids_offset);

  MemoryContextSwitchTo(old_context);
  return &state->base;
}

static Datum count_finalize(AnonAggState *base_state, Bucket *bucket, BucketDescriptor *bucket_desc, bool *is_null)
{
  CountState *state = (CountState *)base_state;
  CountResultAccumulator result_accumulator = {0};
  int64 min_count = state->is_global ? 0 : g_config.low_count_min_threshold;
  seed_t bucket_seed = compute_bucket_seed();

  ListCell *cell = NULL;
  foreach (cell, state->contribution_trackers)
  {
    ContributionTrackerState *contribution_tracker = (ContributionTrackerState *)lfirst(cell);
    CountResult result = count_calculate_result(bucket_seed, contribution_tracker);

    if (result.not_enough_aidvs)
      return Int64GetDatum(min_count);

    accumulate_count_result(&result_accumulator, &result);
  }

  int64 finalized_count_result = finalize_count_result(&result_accumulator);
  return Int64GetDatum(Max(finalized_count_result, min_count));
}

static void count_merge(AnonAggState *dst_base_state, const AnonAggState *src_base_state)
{
  CountState *dst_state = (CountState *)dst_base_state;
  const CountState *src_state = (const CountState *)src_base_state;

  Assert(list_length(dst_state->contribution_trackers) == list_length(src_state->contribution_trackers));

  ListCell *dst_cell = NULL;
  const ListCell *src_cell = NULL;
  forboth(dst_cell, dst_state->contribution_trackers, src_cell, src_state->contribution_trackers)
  {
    ContributionTrackerState *dst_contribution_tracker = (ContributionTrackerState *)lfirst(dst_cell);
    const ContributionTrackerState *src_contribution_tracker = (const ContributionTrackerState *)lfirst(src_cell);

    ContributionTracker_iterator iterator;
    ContributionTracker_start_iterate(src_contribution_tracker->contribution_table, &iterator);
    ContributionTrackerHashEntry *entry = NULL;
    while ((entry = ContributionTracker_iterate(src_contribution_tracker->contribution_table, &iterator)) != NULL)
    {
      if (entry->has_contribution)
        contribution_tracker_update_contribution(
            dst_contribution_tracker, entry->contributor.aid, entry->contributor.contribution);
      else
        contribution_tracker_update_aid(dst_contribution_tracker, entry->contributor.aid);
    }

    dst_contribution_tracker->unaccounted_for += src_contribution_tracker->unaccounted_for;
  }
}

static const char *count_explain(const AnonAggState *base_state)
{
  CountState *state = (CountState *)base_state;

  seed_t bucket_seed = compute_bucket_seed();

  StringInfoData string;
  initStringInfo(&string);

  ListCell *cell = NULL;
  foreach (cell, state->contribution_trackers)
  {
    if (foreach_current_index(cell) > 0)
      appendStringInfo(&string, " \n");

    ContributionTrackerState *contribution_tracker = (ContributionTrackerState *)lfirst(cell);
    append_tracker_info(&string, bucket_seed, contribution_tracker);
  }

  return string.data;
}

static const int COUNT_VALUE_INDEX = 1;
static const int COUNT_VALUE_AIDS_OFFSET = 2;

static AnonAggState *count_value_create_state(MemoryContext memory_context, PG_FUNCTION_ARGS)
{
  return count_create_state(memory_context, fcinfo, COUNT_VALUE_AIDS_OFFSET);
}

static void count_value_transition(AnonAggState *base_state, PG_FUNCTION_ARGS)
{
  CountState *state = (CountState *)base_state;

  if (all_aids_null(fcinfo, COUNT_VALUE_AIDS_OFFSET, list_length(state->contribution_trackers)))
    return;

  ListCell *cell = NULL;
  foreach (cell, state->contribution_trackers)
  {
    int aid_index = foreach_current_index(cell) + COUNT_VALUE_AIDS_OFFSET;
    ContributionTrackerState *contribution_tracker = (ContributionTrackerState *)lfirst(cell);
    if (!PG_ARGISNULL(aid_index))
    {
      aid_t aid = contribution_tracker->aid_descriptor.make_aid(PG_GETARG_DATUM(aid_index));
      if (PG_ARGISNULL(COUNT_VALUE_INDEX))
        /* No contribution since argument is NULL, only keep track of the AID value. */
        contribution_tracker_update_aid(contribution_tracker, aid);
      else
        contribution_tracker_update_contribution(contribution_tracker, aid, one_contribution);
    }
    else if (!PG_ARGISNULL(COUNT_VALUE_INDEX))
    {
      contribution_tracker->unaccounted_for++;
    }
  }
}

const AnonAggFuncs g_count_value_funcs = {
    count_final_type,
    count_value_create_state,
    count_value_transition,
    count_finalize,
    count_merge,
    NULL /* TODO */,
    count_explain,
};

static const int COUNT_STAR_AIDS_OFFSET = 1;

static AnonAggState *count_star_create_state(MemoryContext memory_context, PG_FUNCTION_ARGS)
{
  return count_create_state(memory_context, fcinfo, COUNT_STAR_AIDS_OFFSET);
}

static void count_star_transition(AnonAggState *base_state, PG_FUNCTION_ARGS)
{
  CountState *state = (CountState *)base_state;

  if (all_aids_null(fcinfo, COUNT_STAR_AIDS_OFFSET, list_length(state->contribution_trackers)))
    return;

  ListCell *cell = NULL;
  foreach (cell, state->contribution_trackers)
  {
    int aid_index = foreach_current_index(cell) + COUNT_STAR_AIDS_OFFSET;
    ContributionTrackerState *contribution_tracker = (ContributionTrackerState *)lfirst(cell);
    if (!PG_ARGISNULL(aid_index))
    {
      aid_t aid = contribution_tracker->aid_descriptor.make_aid(PG_GETARG_DATUM(aid_index));
      contribution_tracker_update_contribution(contribution_tracker, aid, one_contribution);
    }
    else
    {
      contribution_tracker->unaccounted_for++;
    }
  }
}

const AnonAggFuncs g_count_star_funcs = {
    count_final_type,
    count_star_create_state,
    count_star_transition,
    count_finalize,
    count_merge,
    NULL /* TODO */,
    count_explain,
};

/*-------------------------------------------------------------------------
 * UDFs
 *-------------------------------------------------------------------------
 */

static const int STATE_INDEX = 0;

static AnonAggState *count_get_state(PG_FUNCTION_ARGS, int aids_offset)
{
  if (!PG_ARGISNULL(STATE_INDEX))
    return (AnonAggState *)PG_GETARG_POINTER(STATE_INDEX);

  /* We want all memory allocations to be done per aggregation node. */
  MemoryContext memory_context;
  if (AggCheckCallContext(fcinfo, &memory_context) != AGG_CONTEXT_AGGREGATE)
    FAILWITH("Aggregate called in non-aggregate context");

  return count_create_state(memory_context, fcinfo, aids_offset);
}

PG_FUNCTION_INFO_V1(anon_count_value_transfn);
PG_FUNCTION_INFO_V1(anon_count_value_finalfn);
PG_FUNCTION_INFO_V1(anon_count_value_explain_finalfn);

Datum anon_count_value_transfn(PG_FUNCTION_ARGS)
{
  AnonAggState *state = count_get_state(fcinfo, COUNT_VALUE_AIDS_OFFSET);
  count_value_transition(state, fcinfo);
  PG_RETURN_POINTER(state);
}

Datum anon_count_value_finalfn(PG_FUNCTION_ARGS)
{
  bool is_null = false;
  Datum result = count_finalize(count_get_state(fcinfo, COUNT_VALUE_AIDS_OFFSET), NULL, NULL, &is_null);
  Assert(!is_null);
  PG_RETURN_DATUM(result);
}

Datum anon_count_value_explain_finalfn(PG_FUNCTION_ARGS)
{
  PG_RETURN_TEXT_P(cstring_to_text(count_explain(count_get_state(fcinfo, COUNT_VALUE_AIDS_OFFSET))));
}

PG_FUNCTION_INFO_V1(anon_count_star_transfn);
PG_FUNCTION_INFO_V1(anon_count_star_finalfn);
PG_FUNCTION_INFO_V1(anon_count_star_explain_finalfn);

Datum anon_count_star_transfn(PG_FUNCTION_ARGS)
{
  AnonAggState *state = count_get_state(fcinfo, COUNT_STAR_AIDS_OFFSET);
  count_star_transition(state, fcinfo);
  PG_RETURN_POINTER(state);
}

Datum anon_count_star_finalfn(PG_FUNCTION_ARGS)
{
  bool is_null = false;
  Datum result = count_finalize(count_get_state(fcinfo, COUNT_STAR_AIDS_OFFSET), NULL, NULL, &is_null);
  Assert(!is_null);
  PG_RETURN_DATUM(result);
}

Datum anon_count_star_explain_finalfn(PG_FUNCTION_ARGS)
{
  PG_RETURN_TEXT_P(cstring_to_text(count_explain(count_get_state(fcinfo, COUNT_STAR_AIDS_OFFSET))));
}
