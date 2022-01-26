#include "postgres.h"

#include <math.h>
#include <limits.h>

#include "pg_diffix/aggregation/noise.h"
#include "pg_diffix/config.h"

/*
 * Prepares a seed for generating a new noise value by mixing it with
 * the configured salt hash and the current step name hash.
 */
static seed_t prepare_seed(seed_t seed, const char *step_name)
{
  hash_t salt_hash = hash_bytes(g_config.salt, strlen(g_config.salt));
  hash_t step_hash = hash_bytes(step_name, strlen(step_name));

  return seed ^ salt_hash ^ step_hash;
}

/*
 * The noise seeds are hash values.
 * From each seed we generate a single noise value, with either a uniform or a normal distribution.
 * Any decent hash function should produce values that are uniformly distributed over the output space.
 * Hence, we only need to limit the seed to the requested interval to get a uniformly distributed integer.
 * To get a normally distributed float, we use the Box-Muller method on two uniformly distributed integers.
 */

int generate_uniform_noise(seed_t seed, const char *step_name, int min, int max)
{
  Assert(max > min);
  Assert(min >= 0);

  seed = prepare_seed(seed, step_name);

  /* Mix higher and lower dwords together. */
  uint32 uniform = (uint32)((seed >> 32) ^ seed);

  /*
   * While using modulo to bound values produces biased output, we are using very small ranges
   * (typically less than 10), for which the bias is insignificant.
   */
  uint32 bounded_uniform = uniform % (uint32)(max - min);

  return min + (int)bounded_uniform;
}

double generate_normal_noise(seed_t seed, const char *step_name, double sd)
{
  seed = prepare_seed(seed, step_name);

  /* Get the input uniform values to the Box-Muller method from the upper and lower dwords. */
  const double MAX_UINT32 = 4294967295.0;
  double u1 = (uint32)seed / MAX_UINT32;
  double u2 = (uint32)(seed >> 32) / MAX_UINT32;

  double normal = sqrt(-2.0 * log(u1)) * sin(2.0 * M_PI * u2);
  return sd * normal;
}

int generate_lcf_threshold(seed_t seed)
{
  double threshold_mean = (double)g_config.low_count_min_threshold +
                          g_config.low_count_mean_gap * g_config.low_count_layer_sd;
  double noise = generate_normal_noise(seed, "suppress", g_config.low_count_layer_sd);
  int noisy_threshold = (int)(threshold_mean + noise);
  return Max(noisy_threshold, g_config.low_count_min_threshold);
}
