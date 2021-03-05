#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include <math.h>
#include <inttypes.h>

#include "pg_diffix/config.h"
#include "pg_diffix/anon/contribution_tracker.h"
#include "pg_diffix/anon/random.h"

typedef struct CountResult
{
  uint64 random_seed;
  int64 true_count;
  int64 flattened_count;
  int64 noisy_count;
  uint32 noisy_outlier_count;
  uint32 noisy_top_count;
} CountResult;

static CountResult count_calculate_final(ContributionTrackerState *state);

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

PG_FUNCTION_INFO_V1(diffix_count_transfn);
PG_FUNCTION_INFO_V1(diffix_count_finalfn);
PG_FUNCTION_INFO_V1(diffix_count_explain_finalfn);

PG_FUNCTION_INFO_V1(diffix_count_any_transfn);
PG_FUNCTION_INFO_V1(diffix_count_any_finalfn);
PG_FUNCTION_INFO_V1(diffix_count_any_explain_finalfn);

Datum diffix_count_transfn(PG_FUNCTION_ARGS)
{
  ContributionTrackerState *state = get_aggregate_contribution_tracker(fcinfo, &count_descriptor);

  if (!PG_ARGISNULL(1))
  {
    aid_t aid = state->aid_descriptor.make_aid(PG_GETARG_DATUM(1));
    contribution_tracker_update_contribution(state, aid, one_contribution);
  }

  PG_RETURN_POINTER(state);
}

Datum diffix_count_any_transfn(PG_FUNCTION_ARGS)
{
  ContributionTrackerState *state = get_aggregate_contribution_tracker(fcinfo, &count_descriptor);

  if (!PG_ARGISNULL(1))
  {
    aid_t aid = state->aid_descriptor.make_aid(PG_GETARG_DATUM(1));
    if (PG_ARGISNULL(2))
    {
      contribution_tracker_update_aid(state, aid);
    }
    else
    {
      contribution_tracker_update_contribution(state, aid, one_contribution);
    }
  }

  PG_RETURN_POINTER(state);
}

Datum diffix_count_finalfn(PG_FUNCTION_ARGS)
{
  ContributionTrackerState *state = get_aggregate_contribution_tracker(fcinfo, &count_descriptor);
  CountResult result = count_calculate_final(state);
  PG_RETURN_INT64(result.noisy_count);
}

Datum diffix_count_any_finalfn(PG_FUNCTION_ARGS)
{
  return diffix_count_finalfn(fcinfo);
}

Datum diffix_count_explain_finalfn(PG_FUNCTION_ARGS)
{
  ContributionTrackerState *state = get_aggregate_contribution_tracker(fcinfo, &count_descriptor);
  CountResult result = count_calculate_final(state);

  StringInfoData string;
  initStringInfo(&string);

  appendStringInfo(&string, "uniq=%" PRIu32, state->contribution_table->members);

  /* Print only effective part of the seed. */
  uint16 *random_seed = (uint16 *)(&result.random_seed);
  appendStringInfo(&string,
                   ", seed=%04" PRIx16 "%04" PRIx16 "%04" PRIx16,
                   random_seed[0], random_seed[1], random_seed[2]);

  /* Top contributors */
  uint32 top_length = Min(state->top_contributors_length, state->distinct_contributors);
  appendStringInfo(&string, "\ntop=[");

  for (uint32 i = 0; i < top_length; i++)
  {
    appendStringInfo(&string, "%" AID_FMT "\u2794%" CONTRIBUTION_INT_FMT,
                     state->top_contributors[i].aid,
                     state->top_contributors[i].contribution.integer);

    if (i == result.noisy_outlier_count - 1)
    {
      appendStringInfo(&string, " | ");
    }
    else if (i < top_length - 1)
    {
      appendStringInfo(&string, ", ");
    }
  }

  appendStringInfo(&string, "]");

  appendStringInfo(&string, "\ntrue=%" PRIi64 ", flat=%" PRIi64 ", final=%" PRIi64,
                   result.true_count,
                   result.flattened_count,
                   result.noisy_count);

  PG_RETURN_TEXT_P(cstring_to_text(string.data));
}

Datum diffix_count_any_explain_finalfn(PG_FUNCTION_ARGS)
{
  return diffix_count_explain_finalfn(fcinfo);
}

static CountResult count_calculate_final(ContributionTrackerState *state)
{
  CountResult result;
  uint64 seed = make_seed(state->aid_seed);

  result.random_seed = seed;
  result.true_count = state->overall_contribution.integer;

  /* Determine outlier/top counts. */
  result.noisy_outlier_count = next_uniform_int(
      &seed,
      Config.outlier_count_min,
      Config.outlier_count_max + 1);

  result.noisy_top_count = next_uniform_int(
      &seed,
      Config.top_count_min,
      Config.top_count_max + 1);

  /* Remove outliers from overall count. */
  result.flattened_count = result.true_count;
  uint32 top_length = Min(state->top_contributors_length, state->distinct_contributors);
  uint32 outlier_end_index = Min(top_length, result.noisy_outlier_count);
  for (uint32 i = 0; i < outlier_end_index; i++)
  {
    result.flattened_count -= state->top_contributors[i].contribution.integer;
  }

  /* Compensate for dropped outliers. */
  uint32 top_end_index = Min(top_length, result.noisy_outlier_count + result.noisy_top_count);
  uint32 actual_top_count = top_end_index - result.noisy_outlier_count;
  if (actual_top_count > 0)
  {
    uint64 top_contribution = 0;

    for (uint32 i = result.noisy_outlier_count; i < top_end_index; i++)
    {
      top_contribution += state->top_contributors[i].contribution.integer;
    }

    uint64 outlier_compensation = round((double)top_contribution * result.noisy_outlier_count / actual_top_count);
    result.flattened_count += outlier_compensation;
  }

  /* Apply noise to flattened count. */
  result.noisy_count = apply_noise(result.flattened_count, &seed);

  return result;
}
