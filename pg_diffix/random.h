#ifndef PG_DIFFIX_RANDOM_H
#define PG_DIFFIX_RANDOM_H

#include "postgres.h"

extern uint64 make_seed(uint32 aid_seed);

extern double next_double(
    uint64 *seed,
    double stddev);

extern int next_int_in_range(
    uint64 *seed,
    int min, int max, double stddev);

#endif /* PG_DIFFIX_RANDOM_H */
