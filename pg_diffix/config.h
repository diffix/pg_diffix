#ifndef PG_DIFFIX_CONFIG_H
#define PG_DIFFIX_CONFIG_H

#include "c.h"

/*
 * Root configuration object.
 */
typedef struct DiffixConfig
{
  int default_access_level;
  int session_access_level;

  char *noise_seed;
  double noise_sigma;

  int low_count_min_threshold;
  double low_count_mean_gap;
  double low_count_sigma;

  int outlier_count_min;
  int outlier_count_max;

  int top_count_min;
  int top_count_max;
} DiffixConfig;

/*
 * Global instance of root configuration.
 */
extern DiffixConfig g_config;

extern void config_init(void);

#endif /* PG_DIFFIX_CONFIG_H */
