#include "postgres.h"
#include "common/hashfn.h"

#include <math.h>

#include "pg_diffix/config.h"
#include "pg_diffix/aggregation/random.h"

extern double pg_erand48(unsigned short xseed[3]);

static inline double clamp_noise(double noise, double sigma)
{
  double absolute_cutoff = sigma * g_config.noise_cutoff;
  return fmin(fmax(noise, -absolute_cutoff), absolute_cutoff);
}

uint64 make_seed(uint64 aid_seed)
{
  uint64 base_seed = hash_bytes_extended(
      (unsigned char *)g_config.noise_seed,
      strlen(g_config.noise_seed),
      0);

  return hash_combine64(base_seed, aid_seed);
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
  return clamp_noise(next_gaussian_double(seed, sigma), sigma);
}

int generate_lcf_threshold(uint64 *seed)
{
  /* Pick an integer in interval [min, min + lcf_range]. */
  return next_uniform_int(
      seed,
      g_config.minimum_allowed_aid_values,
      g_config.minimum_allowed_aid_values + g_config.lcf_range + 1); /* +1 because max is exclusive. */
}
