#ifndef PG_DIFFIX_COUNT_H
#define PG_DIFFIX_COUNT_H

#include "pg_diffix/aggregation/contribution_tracker.h"
#include "pg_diffix/aggregation/noise.h"

extern const ContributionDescriptor count_descriptor;

static const contribution_t one_contribution = {.integer = 1};

/* Describes the anonymized count aggregation for one AID instance. */
typedef struct CountResult
{
  seed_t aid_seed;
  int64 true_count;
  double flattening;
  double flattened_count;
  uint32 noisy_outlier_count;
  uint32 noisy_top_count;
  double noise_sd;
  double noise;
  bool not_enough_aidvs;
} CountResult;

extern CountResult aggregate_count_contributions(
    seed_t bucket_seed, seed_t aid_seed,
    uint64 true_count, uint64 distinct_contributors, uint64 unacounted_for,
    const Contributors *top_contributors);

/*
 * Helper data structure and functions to combine multiple count results into one value.
 */

typedef struct CountResultAccumulator
{
  double max_flattening;
  double count_for_flattening;
  double max_noise_sd;
  double noise_with_max_sd;
} CountResultAccumulator;

extern void accumulate_count_result(CountResultAccumulator *accumulator, const CountResult *result);
extern int64 finalize_count_result(const CountResultAccumulator *accumulator);

#endif /* PG_DIFFIX_COUNT_H */