#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include <math.h>
#include <inttypes.h>

#include "pg_diffix/config.h"
#include "pg_diffix/utils.h"
#include "pg_diffix/aggregation/count.h"
#include "pg_diffix/aggregation/random.h"

static CountResult count_calculate_aid_result(const ContributionTrackerState *tracker);
static Datum count_calculate_final(PG_FUNCTION_ARGS, List *trackers);
static bool all_aids_null(PG_FUNCTION_ARGS, int aids_offset, int ntrackers);
static void determine_outlier_top_counts(uint64 total_count, uint64 *seed, CountResult *result);

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

static const int COUNT_AIDS_OFFSET = 1;
static const int COUNT_ANY_AIDS_OFFSET = 2;
static const int VALUE_INDEX = 1;

PG_FUNCTION_INFO_V1(anon_count_transfn);
PG_FUNCTION_INFO_V1(anon_count_finalfn);
PG_FUNCTION_INFO_V1(anon_count_explain_finalfn);

PG_FUNCTION_INFO_V1(anon_count_any_transfn);
PG_FUNCTION_INFO_V1(anon_count_any_finalfn);
PG_FUNCTION_INFO_V1(anon_count_any_explain_finalfn);

Datum anon_count_transfn(PG_FUNCTION_ARGS)
{
  List *trackers = get_aggregate_contribution_trackers(fcinfo, COUNT_AIDS_OFFSET, &count_descriptor);

  Assert(PG_NARGS() == list_length(trackers) + COUNT_AIDS_OFFSET);

  if (all_aids_null(fcinfo, COUNT_AIDS_OFFSET, list_length(trackers)))
    PG_RETURN_POINTER(trackers);

  ListCell *cell;
  foreach (cell, trackers)
  {
    int aid_index = foreach_current_index(cell) + COUNT_AIDS_OFFSET;
    ContributionTrackerState *tracker = (ContributionTrackerState *)lfirst(cell);
    if (!PG_ARGISNULL(aid_index))
    {
      aid_t aid = tracker->aid_descriptor.make_aid(PG_GETARG_DATUM(aid_index));
      contribution_tracker_update_contribution(tracker, aid, one_contribution);
    }
    else
    {
      tracker->unaccounted_for++;
    }
  }

  PG_RETURN_POINTER(trackers);
}

Datum anon_count_any_transfn(PG_FUNCTION_ARGS)
{
  List *trackers = get_aggregate_contribution_trackers(fcinfo, COUNT_ANY_AIDS_OFFSET, &count_descriptor);

  Assert(PG_NARGS() == list_length(trackers) + COUNT_ANY_AIDS_OFFSET);

  if (all_aids_null(fcinfo, COUNT_ANY_AIDS_OFFSET, list_length(trackers)))
    PG_RETURN_POINTER(trackers);

  ListCell *cell;
  foreach (cell, trackers)
  {
    int aid_index = foreach_current_index(cell) + COUNT_ANY_AIDS_OFFSET;
    ContributionTrackerState *tracker = (ContributionTrackerState *)lfirst(cell);
    if (!PG_ARGISNULL(aid_index))
    {
      aid_t aid = tracker->aid_descriptor.make_aid(PG_GETARG_DATUM(aid_index));
      if (PG_ARGISNULL(VALUE_INDEX))
        /* count argument is NULL, so no contribution, only keep track of the AID value */
        contribution_tracker_update_aid(tracker, aid);
      else
        contribution_tracker_update_contribution(tracker, aid, one_contribution);
    }
    else
    {
      tracker->unaccounted_for++;
    }
  }

  PG_RETURN_POINTER(trackers);
}

Datum anon_count_finalfn(PG_FUNCTION_ARGS)
{
  List *trackers = get_aggregate_contribution_trackers(fcinfo, COUNT_AIDS_OFFSET, &count_descriptor);
  return count_calculate_final(fcinfo, trackers);
}

Datum anon_count_any_finalfn(PG_FUNCTION_ARGS)
{
  List *trackers = get_aggregate_contribution_trackers(fcinfo, COUNT_ANY_AIDS_OFFSET, &count_descriptor);
  return count_calculate_final(fcinfo, trackers);
}

static void append_tracker_info(StringInfo string, const ContributionTrackerState *tracker)
{
  CountResult result = count_calculate_aid_result(tracker);

  appendStringInfo(string, "uniq=%" PRIu32, tracker->contribution_table->members);

  /* Top contributors */
  uint32 top_length = tracker->top_contributors.length;
  appendStringInfo(string, ", top=[");

  for (uint32 i = 0; i < top_length; i++)
  {
    const Contributor *contributor = &tracker->top_contributors.members[i];
    appendStringInfo(string, "%" CONTRIBUTION_INT_FMT "x%" AID_FMT,
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
                     result.flattened_count, result.noise, result.noise_sigma);

  /* Print only effective part of the seed. */
  const uint16 *random_seed = (const uint16 *)(&result.random_seed);
  appendStringInfo(string,
                   ", seed=%04" PRIx16 "%04" PRIx16 "%04" PRIx16,
                   random_seed[0], random_seed[1], random_seed[2]);
}

static Datum explain_count_trackers(List *trackers)
{
  StringInfoData string;
  initStringInfo(&string);

  ListCell *cell;
  foreach (cell, trackers)
  {
    if (foreach_current_index(cell) > 0)
      appendStringInfo(&string, " \n");

    ContributionTrackerState *tracker = (ContributionTrackerState *)lfirst(cell);
    append_tracker_info(&string, tracker);
  }

  PG_RETURN_TEXT_P(cstring_to_text(string.data));
}

Datum anon_count_explain_finalfn(PG_FUNCTION_ARGS)
{
  List *trackers = get_aggregate_contribution_trackers(fcinfo, COUNT_AIDS_OFFSET, &count_descriptor);
  return explain_count_trackers(trackers);
}

Datum anon_count_any_explain_finalfn(PG_FUNCTION_ARGS)
{
  List *trackers = get_aggregate_contribution_trackers(fcinfo, COUNT_ANY_AIDS_OFFSET, &count_descriptor);
  return explain_count_trackers(trackers);
}

CountResult aggregate_count_contributions(
    uint64 seed, uint64 true_count, uint64 distinct_contributors, uint64 unacounted_for,
    const Contributors *top_contributors)
{
  seed = make_seed(seed);

  CountResult result = {0};

  result.random_seed = seed;
  result.true_count = true_count;

  if (distinct_contributors < g_config.outlier_count_min + g_config.top_count_min)
  {
    result.not_enough_aidvs = true;
    return result;
  }

  determine_outlier_top_counts(distinct_contributors, &seed, &result);

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
  double flattened_unaccounted_for = Max(unacounted_for - result.flattening, 0.0);

  result.flattened_count = result.true_count - result.flattening + flattened_unaccounted_for;

  double average = result.flattened_count / (double)distinct_contributors;
  double noise_scale = Max(average, 0.5 * top_average);
  result.noise_sigma = g_config.noise_sigma * noise_scale;
  result.noise = generate_noise(&seed, result.noise_sigma);

  return result;
}

static CountResult count_calculate_aid_result(const ContributionTrackerState *tracker)
{
  return aggregate_count_contributions(
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

  if (result->noise_sigma > accumulator->max_noise_sigma)
  {
    accumulator->max_noise_sigma = result->noise_sigma;
    accumulator->noise_with_max_sigma = result->noise;
  }
}

int64 finalize_count_result(const CountResultAccumulator *accumulator)
{
  int64 rounded_noisy_count = (int64)round(accumulator->count_for_flattening + accumulator->noise_with_max_sigma);
  return Max(rounded_noisy_count, 0);
}

static Datum count_calculate_final(PG_FUNCTION_ARGS, List *trackers)
{
  CountResultAccumulator result_accumulator = {0};

  ListCell *cell;
  foreach (cell, trackers)
  {
    ContributionTrackerState *tracker = (ContributionTrackerState *)lfirst(cell);
    CountResult result = count_calculate_aid_result(tracker);

    if (result.not_enough_aidvs)
      PG_RETURN_INT64(g_config.minimum_allowed_aid_values);

    accumulate_count_result(&result_accumulator, &result);
  }

  int64 finalized_count_result = finalize_count_result(&result_accumulator);
  PG_RETURN_INT64(Max(finalized_count_result, g_config.minimum_allowed_aid_values));
}

static bool all_aids_null(PG_FUNCTION_ARGS, int aids_offset, int ntrackers)
{
  for (int aid_index = aids_offset; aid_index < aids_offset + ntrackers; aid_index++)
  {
    if (!PG_ARGISNULL(aid_index))
      return false;
  }
  return true;
}

static void determine_outlier_top_counts(uint64 total_count, uint64 *seed, CountResult *result)
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
  result->noisy_outlier_count = next_uniform_int(
      seed,
      g_config.outlier_count_min,
      compact_outlier_count_max + 1);

  result->noisy_top_count = next_uniform_int(
      seed,
      g_config.top_count_min,
      compact_top_count_max + 1);
}
