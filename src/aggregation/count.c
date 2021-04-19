#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include <math.h>
#include <inttypes.h>

#include "pg_diffix/config.h"
#include "pg_diffix/utils.h"
#include "pg_diffix/aggregation/contribution_tracker.h"
#include "pg_diffix/aggregation/random.h"

typedef struct CountResult
{
  uint64 random_seed;
  int64 true_count;
  int64 flattened_count;
  uint32 noisy_outlier_count;
  uint32 noisy_top_count;
  double noise_sigma;
  int64 noise;
} CountResult;

static CountResult count_calculate_aid_result(const ContributionTrackerState *tracker);
static int64 count_calculate_final(List *trackers);

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

static const ContributionDescriptor count_descriptor = {
    .contribution_greater = contribution_greater,
    .contribution_equal = contribution_equal,
    .contribution_combine = contribution_combine,
    .contribution_initial = {.integer = 0},
};

static const contribution_t one_contribution = {.integer = 1};

static const int COUNT_AIDS_OFFSET = 1;
static const int COUNT_ANY_AIDS_OFFSET = 2;

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

  ListCell *lc;
  foreach (lc, trackers)
  {
    int aid_index = foreach_current_index(lc) + COUNT_AIDS_OFFSET;
    if (!PG_ARGISNULL(aid_index))
    {
      ContributionTrackerState *tracker = (ContributionTrackerState *)lfirst(lc);
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

  ListCell *lc;
  foreach (lc, trackers)
  {
    int aid_index = foreach_current_index(lc) + COUNT_ANY_AIDS_OFFSET;
    if (!PG_ARGISNULL(aid_index))
    {
      ContributionTrackerState *tracker = (ContributionTrackerState *)lfirst(lc);
      aid_t aid = tracker->aid_descriptor.make_aid(PG_GETARG_DATUM(aid_index));
      if (PG_ARGISNULL(1))
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
  PG_RETURN_INT64(count_calculate_final(trackers));
}

Datum anon_count_any_finalfn(PG_FUNCTION_ARGS)
{
  List *trackers = get_aggregate_contribution_trackers(fcinfo, COUNT_ANY_AIDS_OFFSET, &count_descriptor);
  PG_RETURN_INT64(count_calculate_final(trackers));
}

static void append_tracker_info(StringInfo string, const ContributionTrackerState *tracker)
{
  CountResult result = count_calculate_aid_result(tracker);

  appendStringInfo(string, "uniq=%" PRIu32, tracker->contribution_table->members);

  /* Top contributors */
  uint32 top_length = Min(tracker->top_contributors_length, tracker->distinct_contributors);
  appendStringInfo(string, ", top=[");

  for (uint32 i = 0; i < top_length; i++)
  {
    appendStringInfo(string, "%" CONTRIBUTION_INT_FMT "x%" AID_FMT,
                     tracker->top_contributors[i].contribution.integer,
                     tracker->top_contributors[i].aid);

    if (i == result.noisy_outlier_count - 1)
      appendStringInfo(string, " | ");
    else if (i < top_length - 1)
      appendStringInfo(string, ", ");
  }

  appendStringInfo(string, "]");

  appendStringInfo(string, ", true=%" PRIi64 ", flat=%" PRIi64 ", noise=%" PRIi64 ", SD=%.3f",
                   result.true_count, result.flattened_count, result.noise, result.noise_sigma);

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

  ListCell *lc;
  foreach (lc, trackers)
  {
    if (foreach_current_index(lc) > 0)
      appendStringInfo(&string, " \n");

    ContributionTrackerState *tracker = (ContributionTrackerState *)lfirst(lc);
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

static CountResult count_calculate_aid_result(const ContributionTrackerState *tracker)
{
  CountResult result = {0};
  uint64 seed = make_seed(tracker->aid_seed);

  result.random_seed = seed;
  result.true_count = tracker->overall_contribution.integer;

  /* Determine outlier/top counts. */
  result.noisy_outlier_count = next_uniform_int(
      &seed,
      g_config.outlier_count_min,
      g_config.outlier_count_max + 1);

  result.noisy_top_count = next_uniform_int(
      &seed,
      g_config.top_count_min,
      g_config.top_count_max + 1);

  /* Remove outliers from overall count. */
  result.flattened_count = result.true_count;
  uint32 top_length = Min(tracker->top_contributors_length, tracker->distinct_contributors);
  uint32 outlier_end_index = Min(top_length, result.noisy_outlier_count);
  for (uint32 i = 0; i < outlier_end_index; i++)
  {
    result.flattened_count -= tracker->top_contributors[i].contribution.integer;
  }

  /* Compensate for dropped outliers. */
  uint32 top_end_index = Min(top_length, result.noisy_outlier_count + result.noisy_top_count);
  uint32 actual_top_count = top_end_index - result.noisy_outlier_count;
  if (actual_top_count > 0)
  {
    uint64 top_contribution = 0;

    for (uint32 i = result.noisy_outlier_count; i < top_end_index; i++)
    {
      top_contribution += tracker->top_contributors[i].contribution.integer;
    }

    uint64 outlier_compensation = round((double)top_contribution * result.noisy_outlier_count / actual_top_count);
    result.flattened_count += outlier_compensation;
  }

  result.noise_sigma = g_config.noise_sigma;
  result.noise = (int64)round(generate_noise(&seed, result.noise_sigma));

  return result;
}

static int64 count_calculate_final(List *trackers)
{
  int64 max_flattening = -1;
  int64 max_flattened_count = 0;
  double max_noise_sigma = -1.0;
  int64 noise_with_max_sigma = 0;

  ListCell *lc;
  foreach (lc, trackers)
  {
    ContributionTrackerState *tracker = (ContributionTrackerState *)lfirst(lc);
    CountResult result = count_calculate_aid_result(tracker);

    int64 flattening = result.true_count - result.flattened_count;
    Assert(flattening >= 0);
    if (flattening > max_flattening)
    {
      max_flattening = flattening;
      /* Get the largest flattened count from the ones with the maximum flattening. */
      max_flattened_count = Max(max_flattened_count, result.flattened_count);
    }

    if (result.noise_sigma > max_noise_sigma)
    {
      max_noise_sigma = result.noise_sigma;
      noise_with_max_sigma = result.noise;
    }
  }

  return Max(max_flattened_count + noise_with_max_sigma, 0);
}
