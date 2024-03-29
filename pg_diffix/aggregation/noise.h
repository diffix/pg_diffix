#ifndef PG_DIFFIX_NOISE_H
#define PG_DIFFIX_NOISE_H

#include "pg_diffix/utils.h"

/*
 * Returns a uniform integer in the positive interval [min, max] for the given seed and step name.
 */
extern int generate_uniform_noise(seed_t seed, const char *step_name, int min, int max);

/*
 * Returns the combined zero-mean gaussian noise value for the given noise layers and step name.
 */
extern double generate_layered_noise(const seed_t *seeds, int seeds_count,
                                     const char *step_name, double layer_sd);

/*
 * Returns the noisy LCF threshold for the given noise layer.
 */
extern double generate_lcf_threshold(seed_t seed);

#endif /* PG_DIFFIX_NOISE_H */
