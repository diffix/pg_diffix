#ifndef PG_DIFFIX_CONFIG_H
#define PG_DIFFIX_CONFIG_H

/*
 * Root configuration object.
 */
typedef struct DiffixConfig
{
  int default_access_level; /* Stores default `AccessLevel` value. */
  int session_access_level; /* Stores current `AccessLevel` value. */

  bool treat_unmarked_tables_as_public;
  bool strict;

  char *salt;

  double noise_layer_sd;

  int low_count_min_threshold;
  double low_count_mean_gap;
  double low_count_layer_sd;

  int outlier_count_min;
  int outlier_count_max;

  int top_count_min;
  int top_count_max;

  bool compute_suppress_bin;
  char *text_label_for_suppress_bin;
} DiffixConfig;

/*
 * Global instance of root configuration.
 */
extern DiffixConfig g_config;

extern void config_init(void);

/*
 * Validates cross-dependent configuration variables and fails if they are mismatched.
 */
extern void config_validate(void);

/*
 * Returns true if the extension was activated for the current database.
 */
extern bool is_pg_diffix_active(void);

#endif /* PG_DIFFIX_CONFIG_H */
