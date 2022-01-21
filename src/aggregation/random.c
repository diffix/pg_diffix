#include "postgres.h"

#include <math.h>

#include "pg_diffix/config.h"
#include "pg_diffix/aggregation/random.h"
#include "pg_diffix/utils.h"

extern double pg_erand48(unsigned short xseed[3]);

uint64 make_seed(uint64 aid_seed)
{
  uint64 salt_hash = hash_bytes(g_config.salt, strlen(g_config.salt));
  return hash_combine(salt_hash, aid_seed);
}

double next_gaussian_double(uint64 *seed, double sigma)
{
  /*
   * Marsaglia polar method
   * Source: http://c-faq.com/lib/gaussian.html
   */
  double v1, v2, s;
  do
  {
    double u1 = pg_erand48((unsigned short *)seed);
    double u2 = pg_erand48((unsigned short *)seed);

    v1 = 2 * u1 - 1;
    v2 = 2 * u2 - 1;
    s = v1 * v1 + v2 * v2;
  } while (s >= 1 || s == 0);

  /*
   * We discard one sample...
   * Needs optimization in case we need to generate multiple numbers.
   */
  return sigma * v1 * sqrt(-2 * log(s) / s);
}

int next_uniform_int(uint64 *seed, int min, int max)
{
  return min + pg_erand48((unsigned short *)seed) * (max - min);
}

double generate_noise(uint64 *seed, double sigma)
{
  return next_gaussian_double(seed, sigma);
}

int generate_lcf_threshold(uint64 *seed)
{
  double threshold_mean = (double)g_config.low_count_min_threshold +
                          g_config.low_count_mean_gap * g_config.low_count_layer_sd;
  return (int)(threshold_mean + next_gaussian_double(seed, g_config.low_count_layer_sd));
}
