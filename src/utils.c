#include "postgres.h"

#include <math.h>

#include "pg_diffix/utils.h"

seed_t hash_set_to_seed(const List *hash_set)
{
  ListCell *cell = NULL;
  seed_t accumulator = 0;
  foreach (cell, hash_set)
  {
    hash_t hash = (hash_t)lfirst(cell);
    accumulator ^= hash;
  }
  return accumulator;
}

const double MONEY_ROUND_MIN = 1e-10;
const double MONEY_ROUND_DELTA = 1e-10 / 100;

/* Works with `value` between 1.0 and 10.0. */
static double money_round_internal(double x)
{
  if (x < 1.5)
    return 1.0;
  else if (x < 3.5)
    return 2.0;
  else if (x < 7.5)
    return 5.0;
  else
    return 10.0;
}

double money_round(double x)
{
  if (x < MONEY_ROUND_MIN)
  {
    return 0.0;
  }
  else
  {
    const double tens = pow(10.0, floor(log10(x)));
    return tens * money_round_internal(x / tens);
  }
}

bool is_money_rounded(double x)
{
  double delta = fabs(x - money_round(x));
  return delta < MONEY_ROUND_DELTA;
}
