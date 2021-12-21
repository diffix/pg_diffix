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

  ListCell *cell;
  foreach (cell, trackers)
  {
    int aid_index = foreach_current_index(cell) + COUNT_AIDS_OFFSET;
    if (!PG_ARGISNULL(aid_index))
    {
      ContributionTrackerState *tracker = (ContributionTrackerState *)lfirst(cell);
      aid_t aid = tracker->aid_descriptor.make_aid(PG_GETARG_DATUM(aid_index));
      contribution_tracker_update_contribution(tracker, aid, one_contribution);
    }
  }

  PG_RETURN_POINTER(trackers);
}

Datum anon_count_any_transfn(PG_FUNCTION_ARGS)
{
  List *trackers = get_aggregate_contribution_trackers(fcinfo, COUNT_ANY_AIDS_OFFSET, &count_descriptor);

  Assert(PG_NARGS() == list_length(trackers) + COUNT_ANY_AIDS_OFFSET);

  ListCell *cell;
  foreach (cell, trackers)
  {
    int aid_index = foreach_current_index(cell) + COUNT_ANY_AIDS_OFFSET;
    if (!PG_ARGISNULL(aid_index))
    {
      ContributionTrackerState *tracker = (ContributionTrackerState *)lfirst(cell);
      aid_t aid = tracker->aid_descriptor.make_aid(PG_GETARG_DATUM(aid_index));
      if (PG_ARGISNULL(VALUE_INDEX))
        /* count argument is NULL, so no contribution, only keep track of the AID value */
        contribution_tracker_update_aid(tracker, aid);
      else
        contribution_tracker_update_contribution(tracker, aid, one_contribution);
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

  if (result.low_count)
    appendStringInfo(string, ", insufficient AIDs");
  else
    appendStringInfo(string, ", flat=%" PRIi64 ", noise=%" PRIi64 ", SD=%.3f",
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
    uint64 seed, uint64 true_count, uint64 distinct_contributors,
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

  /* Determine outlier/top counts. */
  result.noisy_outlier_count = next_uniform_int(
      &seed,
      g_config.outlier_count_min,
      g_config.outlier_count_max + 1);

  result.noisy_top_count = next_uniform_int(
      &seed,
      g_config.top_count_min,
      g_config.top_count_max + 1);

  uint32 top_end_index = result.noisy_outlier_count + result.noisy_top_count;

  result.low_count = top_end_index > top_contributors->length;
  if (result.low_count)
    return result;

  /* Remove outliers from overall count. */
  result.flattened_count = result.true_count;
  for (uint32 i = 0; i < result.noisy_outlier_count; i++)
    result.flattened_count -= top_contributors->members[i].contribution.integer;

  /* Compute average of top values. */
  uint64 top_contribution = 0;
  for (uint32 i = result.noisy_outlier_count; i < top_end_index; i++)
    top_contribution += top_contributors->members[i].contribution.integer;
  double top_average = top_contribution / (double)result.noisy_top_count;

  /* Compensate for dropped outliers. */
  result.flattened_count += (int64)round(top_average * result.noisy_outlier_count);

  double average = result.flattened_count / (double)distinct_contributors;
  result.noise_sigma = g_config.noise_sigma * Max(average, 0.5 * top_average);
  result.noise = (int64)round(generate_noise(&seed, result.noise_sigma));

  return result;
}

static CountResult count_calculate_aid_result(const ContributionTrackerState *tracker)
{
  return aggregate_count_contributions(
      tracker->aid_seed,
      tracker->overall_contribution.integer,
      tracker->distinct_contributors,
      &tracker->top_contributors);
}

void accumulate_count_result(CountResultAccumulator *accumulator, const CountResult *result)
{
  Assert(result->low_count == false);

  int64 flattening = result->true_count - result->flattened_count;
  Assert(flattening >= 0);
  if (flattening >= accumulator->max_flattening)
  {
    accumulator->max_flattening = flattening;
    accumulator->max_flattened_count_with_max_flattening = Max(accumulator->max_flattened_count_with_max_flattening,
                                                               result->flattened_count);
  }

  if (result->noise_sigma > accumulator->max_noise_sigma)
  {
    accumulator->max_noise_sigma = result->noise_sigma;
    accumulator->noise_with_max_sigma = result->noise;
  }
}

int64 finalize_count_result(const CountResultAccumulator *accumulator)
{
  return Max(accumulator->max_flattened_count_with_max_flattening + accumulator->noise_with_max_sigma, 0);
}

static Datum count_calculate_final(PG_FUNCTION_ARGS, List *trackers)
{
  CountResultAccumulator result_accumulator = {0};

  ListCell *cell;
  foreach (cell, trackers)
  {
    ContributionTrackerState *tracker = (ContributionTrackerState *)lfirst(cell);
    CountResult result = count_calculate_aid_result(tracker);

    if (result.low_count)
      PG_RETURN_NULL();

    if (result.not_enough_aidvs) 
      PG_RETURN_INT64(g_config.minimum_allowed_aid_values);

    accumulate_count_result(&result_accumulator, &result);
  }

  PG_RETURN_INT64(finalize_count_result(&result_accumulator));
}
