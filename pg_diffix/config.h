#ifndef PG_DIFFIX_CONFIG_H
#define PG_DIFFIX_CONFIG_H

#include "c.h"

typedef enum AccessLevel
{
  ACCESS_DIRECT, /* No protection - access to raw data */
  ACCESS_PUBLISH /* Publish access level */
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

extern void config_init(void);

#endif /* PG_DIFFIX_CONFIG_H */
