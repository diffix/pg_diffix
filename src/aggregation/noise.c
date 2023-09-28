#include "postgres.h"

#include "common/sha2.h"

#include <limits.h>
#define _USE_MATH_DEFINES
#include <math.h>

#include "pg_diffix/aggregation/noise.h"
#include "pg_diffix/config.h"

#if PG_MAJORVERSION_NUM == 13

static hash_t crypto_hash_salted_seed(seed_t seed)
{
  pg_sha256_ctx hash_ctx;
  pg_sha256_init(&hash_ctx);

  pg_sha256_update(&hash_ctx, (const uint8 *)g_config.salt, strlen(g_config.salt));
  pg_sha256_update(&hash_ctx, (const uint8 *)&seed, sizeof(seed));

  uint8 crypto_hash[PG_SHA256_DIGEST_LENGTH];
  pg_sha256_final(&hash_ctx, crypto_hash);

  Assert(sizeof(hash_t) < sizeof(crypto_hash));
  return *(hash_t *)crypto_hash;
}

#else

#include "common/cryptohash.h"

static hash_t crypto_hash_salted_seed(seed_t seed)
{
  pg_cryptohash_ctx *hash_ctx = pg_cryptohash_create(PG_SHA256);
  if (hash_ctx == NULL)
    FAILWITH_CODE(ERRCODE_OUT_OF_MEMORY, "Out of memory while salting seed.");

  uint8 crypto_hash[PG_SHA256_DIGEST_LENGTH];
  bool hash_error = pg_cryptohash_init(hash_ctx) < 0 ||
                    pg_cryptohash_update(hash_ctx, (const uint8 *)g_config.salt, strlen(g_config.salt)) < 0 ||
                    pg_cryptohash_update(hash_ctx, (const uint8 *)&seed, sizeof(seed)) < 0 ||
                    pg_cryptohash_final(hash_ctx, crypto_hash, sizeof(crypto_hash)) < 0;

  pg_cryptohash_free(hash_ctx);

  if (hash_error)
    FAILWITH_CODE(ERRCODE_INTERNAL_ERROR, "Internal error while salting seed.");

  Assert(sizeof(hash_t) < sizeof(crypto_hash));
  return *(hash_t *)crypto_hash;
}

#endif

/*
 * Prepares a seed for generating a new noise value by mixing it with
 * the configured salt hash and the current step name hash.
 */
static seed_t prepare_seed(seed_t seed, const char *step_name)
{
  hash_t salted_seed_hash = crypto_hash_salted_seed(seed);
  hash_t step_hash = hash_string(step_name);
  return salted_seed_hash ^ step_hash;
}

/*
 * The noise layers' seeds are hash values.
 * From each seed we generate a single noise value, with either a uniform or a normal distribution.
 * Any decent hash function should produce values that are uniformly distributed over the output space.
 * Hence, we only need to limit the seed to the requested interval to get a uniformly distributed integer.
 * To get a normally distributed float, we use the Box-Muller method on two uniformly distributed integers.
 */

int generate_uniform_noise(seed_t seed, const char *step_name, int min, int max)
{
  Assert(max >= min);
  Assert(min >= 0);

  seed = prepare_seed(seed, step_name);

  /* Mix higher and lower dwords together. */
  uint32 uniform = (uint32)((seed >> 32) ^ seed);

  /*
   * While using modulo to bound values produces biased output, we are using very small ranges
   * (typically less than 10), for which the bias is insignificant.
   */
  uint32 bounded_uniform = uniform % (uint32)(max - min + 1);

  return min + (int)bounded_uniform;
}

static double generate_normal_noise(seed_t seed, const char *step_name, double sd)
{
  seed = prepare_seed(seed, step_name);

  /* Get the input uniform values to the Box-Muller method from the upper and lower dwords. */
  const double MAX_UINT32 = 4294967295.0;
  double u1 = (uint32)seed / MAX_UINT32;
  double u2 = (uint32)(seed >> 32) / MAX_UINT32;

  double normal = sqrt(-2.0 * log(u1)) * sin(2.0 * M_PI * u2);
  return sd * normal;
}

double generate_layered_noise(const seed_t *seeds, int seeds_count,
                              const char *step_name, double layer_sd)
{
  double noise = 0;
  for (int i = 0; i < seeds_count; i++)
    noise += generate_normal_noise(seeds[i], step_name, layer_sd);
  return noise;
}

double generate_lcf_threshold(seed_t seed)
{
  /* 
   * `low_count_mean_gap` is the number of (total!) standard deviations between
   * `low_count_min_threshold` and desired mean.
   */
  double threshold_mean = (double)g_config.low_count_min_threshold +
                          g_config.low_count_mean_gap * g_config.low_count_layer_sd * sqrt(2.0);
  double noise = generate_layered_noise(&seed, 1, "suppress", g_config.low_count_layer_sd);
  double noisy_threshold = threshold_mean + noise;
  return Max(noisy_threshold, g_config.low_count_min_threshold);
}
