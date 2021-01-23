#include <math.h>
#include "pg_opendiffix/random.h"

/* TODO */

uint64 make_seed(uint32 aid_seed)
{
  return aid_seed;
}

double next_double(
    uint64 *seed,
    double stddev)
{
  return 0.5;
}

int next_int_in_range(
    uint64 *seed,
    int min, int max, double stddev)
{
  return min + (int)round((max - min) / 2);
}
