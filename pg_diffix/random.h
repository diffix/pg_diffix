#ifndef PG_DIFFIX_RANDOM_H
#define PG_DIFFIX_RANDOM_H

#include "postgres.h"

/*
 * Combines the given 64-bit noise layer seed with
 * the seed from config and produces a 64-bit output.
 */
extern uint64 make_seed(uint64 noise_layer_seed);

/*
 * Generates a zero-mean gaussian double with given stddev.
 */
extern double next_gaussian_double(uint64 *seed, double stddev);

/*
 * Generates a uniform integer in interval [min, max).
 */
extern int next_uniform_int(uint64 *seed, int min, int max);

#endif /* PG_DIFFIX_RANDOM_H */
