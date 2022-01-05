#ifndef PG_DIFFIX_COUNT_H
#define PG_DIFFIX_COUNT_H

#include "pg_diffix/aggregation/contribution_tracker.h"

extern const ContributionDescriptor count_descriptor;

static const contribution_t one_contribution = {.integer = 1};

/* Describes the anonymized count aggregation for one AID instance. */
typedef struct CountResult
{
  uint64 random_seed;
  int64 true_count;
  int64 flattened_count;
  uint32 noisy_outlier_count;
  uint32 noisy_top_count;
  double noise_sigma;
  int64 noise;
  bool low_count;
} CountResult;

extern CountResult aggregate_count_contributions(
    uint64 seed, uint64 true_count, uint64 distinct_contributors,
    const Contributors *top_contributors);

/*
 * Helper data structure and functions to combine multiple count results into one value.
 */

typedef struct CountResultAccumulator
{
  int64 max_flattening;
  int64 max_flattened_count_with_max_flattening;
  double max_noise_sigma;
  int64 noise_with_max_sigma;
} CountResultAccumulator;

extern void accumulate_count_result(CountResultAccumulator *accumulator, const CountResult *result);
extern int64 finalize_count_result(const CountResultAccumulator *accumulator);

#endif /* PG_DIFFIX_COUNT_H */