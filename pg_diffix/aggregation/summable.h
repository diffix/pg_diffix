#ifndef PG_DIFFIX_SUMMABLE_H
#define PG_DIFFIX_SUMMABLE_H

#include "pg_diffix/aggregation/contribution_tracker.h"
#include "pg_diffix/aggregation/noise.h"

extern const ContributionDescriptor integer_descriptor;
extern const ContributionDescriptor real_descriptor;

/* Describes the anonymized aggregation for one AID instance. */
typedef struct SummableResult
{
  seed_t aid_seed;
  double flattening;
  double flattened_sum;
  uint32 noisy_outlier_count;
  uint32 noisy_top_count;
  double noise_sd;
  double noise;
  bool not_enough_aid_values;
} SummableResult;

extern SummableResult aggregate_contributions(
    seed_t bucket_seed,
    seed_t aid_seed,
    contribution_t true_sum,
    uint64 distinct_contributors,
    contribution_t unaccounted_for,
    ContributionToDoubleFunc contribution_to_double,
    const Contributors *top_contributors);

/*
 * Helper data structure and functions to combine multiple results into one value.
 */
typedef struct SummableResultAccumulator
{
  double max_flattening;
  double sum_for_flattening;
  double max_noise_sd;
  double noise_with_max_sd;
  bool not_enough_aid_values;
} SummableResultAccumulator;

extern void merge_trackers(
    int dst_trackers_count,
    int src_trackers_count,
    ContributionTrackerState *dst_trackers[],
    ContributionTrackerState *const src_trackers[]);

extern SummableResult calculate_result(seed_t bucket_seed, const ContributionTrackerState *tracker);

extern void accumulate_result(SummableResultAccumulator *accumulator, const SummableResult *result);

extern double finalize_noise_result(const SummableResultAccumulator *accumulator);

#endif /* PG_DIFFIX_SUMMABLE_H */
