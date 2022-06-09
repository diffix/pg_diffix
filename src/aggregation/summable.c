#include "postgres.h"

#include <math.h>

#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/aggregation/noise.h"
#include "pg_diffix/aggregation/summable.h"
#include "pg_diffix/config.h"

static bool integer_contribution_greater(contribution_t x, contribution_t y)
{
  return x.integer > y.integer;
}

static bool integer_contribution_equal(contribution_t x, contribution_t y)
{
  return x.integer == y.integer;
}

static contribution_t integer_contribution_combine(contribution_t x, contribution_t y)
{
  return (contribution_t){.integer = x.integer + y.integer};
}

static double integer_contribution_to_double(contribution_t x)
{
  return (double)x.integer;
}

const ContributionDescriptor integer_descriptor = {
    .contribution_greater = integer_contribution_greater,
    .contribution_equal = integer_contribution_equal,
    .contribution_combine = integer_contribution_combine,
    .contribution_to_double = integer_contribution_to_double,
    .contribution_initial = {.integer = 0},
};
static bool real_contribution_greater(contribution_t x, contribution_t y)
{
  return x.real > y.real;
}

static bool real_contribution_equal(contribution_t x, contribution_t y)
{
  return x.real == y.real;
}

static contribution_t real_contribution_combine(contribution_t x, contribution_t y)
{
  return (contribution_t){.real = x.real + y.real};
}

static double real_contribution_to_double(contribution_t x)
{
  return (double)x.real;
}

const ContributionDescriptor real_descriptor = {
    .contribution_greater = real_contribution_greater,
    .contribution_equal = real_contribution_equal,
    .contribution_combine = real_contribution_combine,
    .contribution_to_double = real_contribution_to_double,
    .contribution_initial = {.real = 0.0},
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
    SummableResult *result)
{
  /* Compact flattening intervals */
  int total_adjustment = g_config.outlier_count_max + g_config.top_count_max - total_count;
  int compact_outlier_count_max = g_config.outlier_count_max;
  int compact_top_count_max = g_config.top_count_max;

  if (total_adjustment > 0)
  {
    /* 
     * At this point we know `0 < total_adjustment <= outlier_range + top_range` (*) because:
     * `total_adjustment = outlier_count_max + top_count_max - total_count
     *                  <= outlier_count_max + top_count_max - outlier_count_min - top_count_min`.
     */
    int outlier_range = g_config.outlier_count_max - g_config.outlier_count_min;
    int top_range = g_config.top_count_max - g_config.top_count_min;
    /* `top_adjustment` will be half of `total_adjustment` rounded up, so it takes priority as it should. */
    int outlier_adjustment = total_adjustment / 2;
    int top_adjustment = total_adjustment - outlier_adjustment;

    /* Adjust, depending on how the adjustments "fit" in the ranges. */
    if (outlier_range >= outlier_adjustment && top_range >= top_adjustment)
    {
      /* Both ranges are compacted at same rate. */
      compact_outlier_count_max -= outlier_adjustment;
      compact_top_count_max -= top_adjustment;
    }
    else if (outlier_range < outlier_adjustment && top_range >= top_adjustment)
    {
      /* `outlier_count` is compacted as much as possible by `outlier_range`, `top_count` takes the surplus adjustment. */
      compact_outlier_count_max = g_config.outlier_count_min;
      compact_top_count_max -= total_adjustment - outlier_range;
    }
    else if (outlier_range >= outlier_adjustment && top_range < top_adjustment)
    {
      /* Vice versa. */
      compact_outlier_count_max -= total_adjustment - top_range;
      compact_top_count_max = g_config.top_count_min;
    }
    else
    {
      /*
       * Not possible. Otherwise `outlier_range + top_range < outlier_adjustment + top_adjustment = total_adjustment`
       * but we knew the opposite was true in (*) above.
       */
      FAILWITH("Internal error - impossible interval compacting");
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

SummableResult aggregate_contributions(
    seed_t bucket_seed, seed_t aid_seed,
    contribution_t true_sum, uint64 distinct_contributors, contribution_t unaccounted_for,
    ContributionToDoubleFunc contribution_to_double, const Contributors *top_contributors)
{
  SummableResult result = {0};

  result.aid_seed = aid_seed;

  if (distinct_contributors < g_config.outlier_count_min + g_config.top_count_min)
  {
    result.not_enough_aid_values = true;
    return result;
  }

  determine_outlier_top_counts(distinct_contributors, top_contributors, &result);

  uint32 top_end_index = result.noisy_outlier_count + result.noisy_top_count;

  /* Remove outliers from overall count. */
  for (uint32 i = 0; i < result.noisy_outlier_count; i++)
    result.flattening += contribution_to_double(top_contributors->members[i].contribution);

  /* Compute average of top values. */
  double top_contribution = 0.0;
  for (uint32 i = result.noisy_outlier_count; i < top_end_index; i++)
    top_contribution += contribution_to_double(top_contributors->members[i].contribution);
  double top_average = top_contribution / result.noisy_top_count;

  /* Compensate for dropped outliers. */
  result.flattening -= top_average * result.noisy_outlier_count;

  /* Compensate for the unaccounted for NULL-value AIDs. */
  double flattened_unaccounted_for = Max(contribution_to_double(unaccounted_for) - result.flattening, 0.0);

  result.flattened_sum = contribution_to_double(true_sum) - result.flattening;

  double average = result.flattened_sum / (double)distinct_contributors;
  double noise_scale = Max(average, 0.5 * top_average);
  result.noise_sd = g_config.noise_layer_sd * noise_scale;
  seed_t noise_layers[] = {bucket_seed, aid_seed};
  result.noise = generate_layered_noise(noise_layers, ARRAY_LENGTH(noise_layers), "noise", result.noise_sd);

  result.flattened_sum += flattened_unaccounted_for;

  return result;
}

SummableResult calculate_result(seed_t bucket_seed, const ContributionTrackerState *tracker)
{
  return aggregate_contributions(
      bucket_seed,
      tracker->aid_seed,
      tracker->overall_contribution,
      tracker->distinct_contributors,
      tracker->unaccounted_for,
      tracker->contribution_descriptor.contribution_to_double,
      &tracker->top_contributors);
}

void accumulate_result(SummableResultAccumulator *accumulator, const SummableResult *result)
{
  if (result->not_enough_aid_values)
  {
    accumulator->not_enough_aid_values = true;
    return;
  }

  Assert(result->flattening >= 0);

  if (result->flattening > accumulator->max_flattening)
  {
    /* Get the flattened aggregation for the AID with the maximum flattening... */
    accumulator->max_flattening = result->flattening;
    accumulator->sum_for_flattening = result->flattened_sum;
  }
  else if (result->flattening == accumulator->max_flattening)
  {
    /* ...and resolve draws using the largest flattened aggregation. */
    accumulator->sum_for_flattening = Max(accumulator->sum_for_flattening, result->flattened_sum);
  }

  if (result->noise_sd > accumulator->max_noise_sd)
  {
    accumulator->max_noise_sd = result->noise_sd;
    accumulator->noise_with_max_sd = result->noise;
  }
  else if (result->noise_sd == accumulator->max_noise_sd &&
           fabs(result->noise) > fabs(accumulator->noise_with_max_sd))
  {
    /* For determinism, resolve draws using maximum absolute noise value. */
    accumulator->noise_with_max_sd = result->noise;
  }
}

/*-------------------------------------------------------------------------
 * Aggregation callbacks
 *-------------------------------------------------------------------------
 */

void merge_trackers(
    int dst_trackers_count,
    int src_trackers_count,
    ContributionTrackerState *dst_trackers[],
    ContributionTrackerState *const src_trackers[])
{

  Assert(dst_trackers_count == src_trackers_count);

  for (int i = 0; i < src_trackers_count; i++)
  {
    ContributionTrackerState *dst_tracker = dst_trackers[i];
    const ContributionTrackerState *src_tracker = src_trackers[i];

    Assert(dst_tracker->contribution_descriptor.contribution_combine == src_tracker->contribution_descriptor.contribution_combine);

    ContributionTracker_iterator iterator;
    ContributionTracker_start_iterate(src_tracker->contribution_table, &iterator);
    ContributionTrackerHashEntry *entry = NULL;
    while ((entry = ContributionTracker_iterate(src_tracker->contribution_table, &iterator)) != NULL)
    {
      contribution_tracker_update_contribution(dst_tracker, entry->contributor.aid, entry->contributor.contribution);
    }

    ContributionCombineFunc combine = dst_tracker->contribution_descriptor.contribution_combine;
    dst_tracker->unaccounted_for = combine(dst_tracker->unaccounted_for, src_tracker->unaccounted_for);
  }
}

double finalize_noise_result(const SummableResultAccumulator *accumulator)
{
  return round_reported_noise_sd(accumulator->max_noise_sd);
}
