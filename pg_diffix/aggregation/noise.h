#ifndef PG_DIFFIX_NOISE_H
#define PG_DIFFIX_NOISE_H

#include "pg_diffix/utils.h"

typedef hash_t seed_t;

/*
 * Returns a uniform integer in the positive interval [min, max] for the given seed and step name.
 */
extern int generate_uniform_noise(seed_t seed, const char *step_name, int min, int max);

/*
 * Returns a zero-mean gaussian double with the specified standard deviation for the given seed and step name.
 */
extern double generate_normal_noise(seed_t seed, const char *step_name, double sd);

/*
 * Returns the noisy LCF threshold for the given seed.
 */
extern int generate_lcf_threshold(seed_t seed);

#endif /* PG_DIFFIX_NOISE_H */
