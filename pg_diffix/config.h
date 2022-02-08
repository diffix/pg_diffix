#ifndef PG_DIFFIX_CONFIG_H
#define PG_DIFFIX_CONFIG_H

/*
 * Root configuration object.
 */
typedef struct DiffixConfig
{
  int default_access_level;
  int session_access_level;

  char *salt;

  double noise_layer_sd;

  int low_count_min_threshold;
  double low_count_mean_gap;
  double low_count_layer_sd;

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

/* 
 * Strict checking of the configuration variables.
 *
 * GUC's `check_hook` need to be NOTICE only, for all variables which are cross-dependent on each other. This function
 * provides a way to do a strict check after a full round of setting all variables has passed, e.g. after `config_init`
 */
extern void config_check(void);

#endif /* PG_DIFFIX_CONFIG_H */
