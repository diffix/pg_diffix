#ifndef PG_DIFFIX_CONFIG_H
#define PG_DIFFIX_CONFIG_H

#include "c.h"

#define INITIAL_DEFAULT_ACCESS_LEVEL DIRECT_ACCESS

#define INITIAL_NOISE_SEED "diffix"
#define INITIAL_NOISE_SIGMA 1.0
#define INITIAL_NOISE_CUTOFF 5.0

#define INITIAL_MINIMUM_ALLOWED_AIDS 2

#define INITIAL_OUTLIER_COUNT_MIN 1
#define INITIAL_OUTLIER_COUNT_MAX 2

#define INITIAL_TOP_COUNT_MIN 4
#define INITIAL_TOP_COUNT_MAX 6

typedef enum AccessLevel
{
  DIRECT_ACCESS, /* No protection - access to raw data */
  PUBLISH_ACCESS /* Publish access level */
} AccessLevel;

/*
 * Root configuration object.
 */
typedef struct DiffixConfig
{
  int default_access_level;

  char *noise_seed;
  double noise_sigma;
  double noise_cutoff;

  int minimum_allowed_aids;

  int outlier_count_min;
  int outlier_count_max;

  int top_count_min;
  int top_count_max;
} DiffixConfig;

/*
 * Global instance of root configuration.
 */
extern DiffixConfig g_config;

/*
 * Formats config to a palloc'd string.
 */
extern char *config_to_string(DiffixConfig *config);

#endif /* PG_DIFFIX_CONFIG_H */
