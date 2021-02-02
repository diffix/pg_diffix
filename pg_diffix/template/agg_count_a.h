/*
 * Accepts AID type, generates standard diffix_count UDFs.
 * Does not clean up input macros.
 *
 * Input macros:
 *
 * AGG_AID_LABEL
 * AGG_AID_TYPE
 * AGG_AID_FMT
 * AGG_AID_EQUAL(a, b)
 * AGG_AID_HASH(aid)
 * AGG_AID_GETARG(a)
 * AGG_INIT_AID_HASH(state, aid, aid_hash) - optional
 * AGG_INIT_ENTRY(state, entry)            - optional
 *
 * Output:
 * diffix_<AID_LABEL>_count_<function>
 */

#ifndef PG_DIFFIX_AGG_COUNT_A_H
#define PG_DIFFIX_AGG_COUNT_A_H

#include "postgres.h"

#include <math.h>
#include <inttypes.h>

#include "fmgr.h"
#include "utils/builtins.h"
#include "lib/stringinfo.h"

#include "pg_diffix/config.h"
#include "pg_diffix/random.h"

typedef struct CountResult
{
  uint64 random_seed;
  uint64 true_count;
  uint64 flattened_count;
  uint64 noisy_count;
  int noisy_outlier_count;
  int noisy_top_count;
} CountResult;

static inline double clamp_noise(double noise)
{
  if (noise >= Config.noise_cutoff)
  {
    return Config.noise_cutoff;
  }

  if (noise <= -Config.noise_cutoff)
  {
    return -Config.noise_cutoff;
  }

  return noise;
}

#endif /* PG_DIFFIX_AGG_COUNT_A_H */

#define AGG_CONCAT_HELPER(a, b) CppConcat(a, b)
#define AGG_MAKE_NAME(name) \
  AGG_CONCAT_HELPER(AGG_CONCAT_HELPER(diffix_, AGG_AID_LABEL), AGG_CONCAT_HELPER(_, name))

/* Contribution state */
#define CS_PREFIX AGG_MAKE_NAME(count)
#define CS_AID_TYPE AGG_AID_TYPE
#define CS_AID_EQUAL(a, b) AGG_AID_EQUAL(a, b)
#define CS_AID_HASH(aid) AGG_AID_HASH(aid)
#define CS_SEED_INITIAL 0x48a31f9f
#define CS_TRACK_CONTRIBUTION
#define CS_CONTRIBUTION_TYPE uint64
#define CS_CONTRIBUTION_GREATER(a, b) (a > b)
#define CS_CONTRIBUTION_EQUAL(a, b) (a == b)
#define CS_CONTRIBUTION_COMBINE(a, b) (a + b)
#define CS_OVERALL_CONTRIBUTION_CALCULATE
#define CS_OVERALL_CONTRIBUTION_INITIAL 0
#define CS_SCOPE static inline
#define CS_DECLARE
#define CS_DEFINE
#ifdef AGG_INIT_AID_HASH
#define CS_INIT_AID_HASH(state, aid, aid_hash) AGG_INIT_AID_HASH(state, aid, aid_hash)
#endif
#ifdef AGG_INIT_ENTRY
#define CS_INIT_ENTRY(state, entry) AGG_INIT_ENTRY(state, entry)
#endif
#include "pg_diffix/template/contribution_state.h"

/* Exported UDFs */
#define AGG_STAR_TRANSFN AGG_MAKE_NAME(count_star_transfn)
#define AGG_TRANSFN AGG_MAKE_NAME(count_transfn)
#define AGG_FINALFN AGG_MAKE_NAME(count_finalfn)
#define AGG_EXPLAIN_FINALFN AGG_MAKE_NAME(count_explain_finalfn)

/* Functions from contribution_state.h */
#define AGG_CONTRIBUTION_STATE AGG_MAKE_NAME(count_ContributionState)
#define AGG_GET_STATE AGG_MAKE_NAME(count_state_getarg)
#define AGG_UPDATE_AID AGG_MAKE_NAME(count_state_update_aid)
#define AGG_UPDATE_CONTRIBUTION AGG_MAKE_NAME(count_state_update_contribution)

/* Helpers */
#define ARGS fcinfo
#define AGG_CALCULATE_FINAL AGG_MAKE_NAME(count_calculcate_final)
static inline CountResult AGG_CALCULATE_FINAL(AGG_CONTRIBUTION_STATE *state);

PG_FUNCTION_INFO_V1(AGG_STAR_TRANSFN);
PG_FUNCTION_INFO_V1(AGG_TRANSFN);
PG_FUNCTION_INFO_V1(AGG_FINALFN);
PG_FUNCTION_INFO_V1(AGG_EXPLAIN_FINALFN);

Datum AGG_STAR_TRANSFN(PG_FUNCTION_ARGS)
{
  AGG_CONTRIBUTION_STATE *state = AGG_GET_STATE(ARGS, 0);

  if (!PG_ARGISNULL(1))
  {
    AGG_UPDATE_CONTRIBUTION(state, AGG_AID_GETARG(state, 1), 1);
  }

  PG_RETURN_POINTER(state);
}

Datum AGG_TRANSFN(PG_FUNCTION_ARGS)
{
  AGG_CONTRIBUTION_STATE *state = AGG_GET_STATE(ARGS, 0);

  if (!PG_ARGISNULL(1))
  {
    if (PG_ARGISNULL(2))
    {
      AGG_UPDATE_AID(state, AGG_AID_GETARG(state, 1));
    }
    else
    {
      AGG_UPDATE_CONTRIBUTION(state, AGG_AID_GETARG(state, 1), 1);
    }
  }

  PG_RETURN_POINTER(state);
}

Datum AGG_FINALFN(PG_FUNCTION_ARGS)
{
  AGG_CONTRIBUTION_STATE *state = AGG_GET_STATE(ARGS, 0);
  CountResult result = AGG_CALCULATE_FINAL(state);
  PG_RETURN_INT64(result.noisy_count);
}

Datum AGG_EXPLAIN_FINALFN(PG_FUNCTION_ARGS)
{
  AGG_CONTRIBUTION_STATE *state = AGG_GET_STATE(ARGS, 0);
  StringInfoData string;
  int top_length = Min(state->top_contributors_length, state->distinct_aids);
  CountResult result = AGG_CALCULATE_FINAL(state);

  /*
   * Split seed into 4 shorts, because only the first 3 will be effective in the rng.
   */
  uint16 *random_seed = (uint16 *)(&result.random_seed);

  top_length = Min(top_length, result.noisy_outlier_count + result.noisy_top_count);

  initStringInfo(&string);

  appendStringInfo(&string, "uniq=%" PRIu64, state->distinct_aids);

  /* Print only effective part of the seed. */
  appendStringInfo(&string,
                   ", seed=%04" PRIx16 "%04" PRIx16 "%04" PRIx16,
                   random_seed[0], random_seed[1], random_seed[2]);

  appendStringInfo(&string, "\ntop=[");

  for (int i = 0; i < top_length; i++)
  {
    appendStringInfo(&string, AGG_AID_FMT "\u2794%" PRIu64,
                     state->top_contributors[i].aid,
                     state->top_contributors[i].contribution);

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

  appendStringInfo(&string, "\ntrue=%" PRIu64 ", flat=%" PRIu64 ", final=%" PRIu64,
                   result.true_count,
                   result.flattened_count,
                   result.noisy_count);

  PG_RETURN_TEXT_P(cstring_to_text(string.data));
}

static inline CountResult AGG_CALCULATE_FINAL(AGG_CONTRIBUTION_STATE *state)
{
  CountResult result;
  uint64 seed = make_seed(state->aid_seed);
  int top_length = Min(state->top_contributors_length, state->distinct_aids);
  int actual_top_count;

  int outlier_end_index;
  int top_end_index;
  int64 count_noise;

  result.random_seed = seed;
  result.true_count = state->overall_contribution;

  result.noisy_outlier_count = next_uniform_int(
      &seed,
      Config.outlier_count_min,
      Config.outlier_count_max + 1);

  result.noisy_top_count = next_uniform_int(
      &seed,
      Config.top_count_min,
      Config.top_count_max + 1);

  result.flattened_count = result.true_count;

  outlier_end_index = Min(top_length, result.noisy_outlier_count);
  for (int i = 0; i < outlier_end_index; i++)
  {
    result.flattened_count -= state->top_contributors[i].contribution;
  }

  top_end_index = Min(top_length, result.noisy_outlier_count + result.noisy_top_count);
  actual_top_count = top_end_index - result.noisy_outlier_count;

  if (actual_top_count > 0)
  {
    uint64 outlier_compensation;
    uint64 top_contribution = 0;

    for (int i = result.noisy_outlier_count; i < top_end_index; i++)
    {
      top_contribution += state->top_contributors[i].contribution;
    }

    outlier_compensation = round((double)top_contribution * result.noisy_outlier_count / actual_top_count);
    result.flattened_count += outlier_compensation;
  }

  result.noisy_count = result.flattened_count;

  count_noise = round(clamp_noise(next_gaussian_double(&seed, Config.noise_sigma)));

  /* Make sure not to accidentally overflow by subtracting. */
  if (count_noise < 0 && result.noisy_count < -count_noise)
  {
    result.noisy_count = 0;
  }
  else
  {
    result.noisy_count += count_noise;
  }

  /* Make sure final count is at or above the min LCF threshold. */
  if (result.noisy_count < Config.low_count_threshold_min)
  {
    result.noisy_count = Config.low_count_threshold_min;
  }

  return result;
}

/* Clean up only locally used macros. */
#undef AGG_CONCAT_HELPER
#undef AGG_MAKE_NAME

#undef AGG_STAR_TRANSFN
#undef AGG_TRANSFN
#undef AGG_FINALFN
#undef AGG_EXPLAIN_FINALFN

#undef AGG_CONTRIBUTION_STATE
#undef AGG_GET_STATE
#undef AGG_UPDATE_AID
#undef AGG_UPDATE_CONTRIBUTION

#undef ARGS
#undef AGG_CALCULATE_FINAL
