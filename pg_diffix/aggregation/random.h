#ifndef PG_DIFFIX_RANDOM_H
#define PG_DIFFIX_RANDOM_H

#include "c.h"

/*
 * Combines the given 64-bit AID seed with
 * the seed from config and produces a 64-bit output.
 */
extern uint64 make_seed(uint64 aid_seed);

/*
 * Generates a zero-mean gaussian double with given standard deviation.
 */
extern double next_gaussian_double(uint64 *seed, double sigma);

/*
 * Generates a uniform integer in interval [min, max).
 */
extern int next_uniform_int(uint64 *seed, int min, int max);

/*
 * Generates gaussian noise with the specified standard deviation.
 */
extern double generate_noise(uint64 *seed, double sigma);

/*
 * Generates a LCF threshold value.
 */
extern int generate_lcf_threshold(uint64 *seed);

#endif /* PG_DIFFIX_RANDOM_H */
