#include "postgres.h"
#include "fmgr.h"

#include "utils/guc.h"

#include "pg_diffix/config.h"
#include "pg_diffix/hooks.h"
#include "pg_diffix/oid_cache.h"
#include "pg_diffix/utils.h"

#define MAX_NUMERIC_CONFIG 1000

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

void _PG_init(void)
{
  static int activation_count = 1;
  char *config_string;

  DEBUG_LOG("Activating Diffix extension (%i)...", activation_count++);

  load_diffix_config();
  config_string = config_to_string(&Config);

  DEBUG_PRINT("Config %s", config_string);
  pfree(config_string);

  /*
   * Variables
   */
  DefineCustomStringVariable(
      "pg_diffix.noise_seed",                     /* name */
      "Seed used for initializing noise layers.", /* short_desc */
      NULL,                                       /* long_desc */
      &Config.noise_seed,                         /* valueAddr */
      INITIAL_NOISE_SEED,                         /* bootValue */
      PGC_SUSET,                                  /* context */
      0,                                          /* flags */
      NULL,                                       /* check_hook */
      NULL,                                       /* assign_hook */
      NULL);                                      /* show_hook */

  DefineCustomRealVariable(
      "pg_diffix.noise_sigma",                            /* name */
      "Standard deviation of noise added to aggregates.", /* short_desc */
      NULL,                                               /* long_desc */
      &Config.noise_sigma,                                /* valueAddr */
      INITIAL_NOISE_SIGMA,                                /* bootValue */
      0,                                                  /* minValue */
      MAX_NUMERIC_CONFIG,                                 /* maxValue */
      PGC_SUSET,                                          /* context */
      0,                                                  /* flags */
      NULL,                                               /* check_hook */
      NULL,                                               /* assign_hook */
      NULL);                                              /* show_hook */

  DefineCustomRealVariable(
      "pg_diffix.noise_cutoff",        /* name */
      "Maximum absolute noise value.", /* short_desc */
      NULL,                            /* long_desc */
      &Config.noise_cutoff,            /* valueAddr */
      INITIAL_NOISE_CUTOFF,            /* bootValue */
      0,                               /* minValue */
      1e7,                             /* maxValue */
      PGC_SUSET,                       /* context */
      0,                               /* flags */
      NULL,                            /* check_hook */
      NULL,                            /* assign_hook */
      NULL);                           /* show_hook */

  DefineCustomIntVariable(
      "pg_diffix.low_count_threshold_min",        /* name */
      "Minimum low count threshold (inclusive).", /* short_desc */
      NULL,                                       /* long_desc */
      &Config.low_count_threshold_min,            /* valueAddr */
      INITIAL_LOW_COUNT_THRESHOLD_MIN,            /* bootValue */
      0,                                          /* minValue */
      MAX_NUMERIC_CONFIG,                         /* maxValue */
      PGC_SUSET,                                  /* context */
      0,                                          /* flags */
      NULL,                                       /* check_hook */
      NULL,                                       /* assign_hook */
      NULL);                                      /* show_hook */

  DefineCustomIntVariable(
      "pg_diffix.low_count_threshold_max",        /* name */
      "Maximum low count threshold (inclusive).", /* short_desc */
      NULL,                                       /* long_desc*/
      &Config.low_count_threshold_max,            /* valueAddr*/
      INITIAL_LOW_COUNT_THRESHOLD_MAX,            /* bootValue */
      0,                                          /* minValue */
      MAX_NUMERIC_CONFIG,                         /* maxValue */
      PGC_SUSET,                                  /* context */
      0,                                          /* flags */
      NULL,                                       /* check_hook */
      NULL,                                       /* assign_hook */
      NULL);                                      /* show_hook */

  DefineCustomIntVariable(
      "pg_diffix.outlier_count_min",        /* name */
      "Minimum outlier count (inclusive).", /* short_desc */
      NULL,                                 /* long_desc */
      &Config.outlier_count_min,            /* valueAddr */
      INITIAL_OUTLIER_COUNT_MIN,            /* bootValue */
      0,                                    /* minValue */
      MAX_NUMERIC_CONFIG,                   /* maxValue */
      PGC_SUSET,                            /* context */
      0,                                    /* flags */
      NULL,                                 /* check_hook */
      NULL,                                 /* assign_hook */
      NULL);                                /* show_hook */

  DefineCustomIntVariable(
      "pg_diffix.outlier_count_max",        /* name */
      "Maximum outlier count (inclusive).", /* short_desc */
      NULL,                                 /* long_desc */
      &Config.outlier_count_max,            /* valueAddr */
      INITIAL_OUTLIER_COUNT_MAX,            /* bootValue */
      0,                                    /* minValue */
      MAX_NUMERIC_CONFIG,                   /* maxValue */
      PGC_SUSET,                            /* context */
      0,                                    /* flags */
      NULL,                                 /* check_hook */
      NULL,                                 /* assign_hook */
      NULL);                                /* show_hook */

  DefineCustomIntVariable(
      "pg_diffix.top_count_min",                     /* name */
      "Minimum top contributors count (inclusive).", /* short_desc */
      NULL,                                          /* long_desc */
      &Config.top_count_min,                         /* valueAddr */
      INITIAL_TOP_COUNT_MIN,                         /* bootValue */
      0,                                             /* minValue */
      MAX_NUMERIC_CONFIG,                            /* maxValue */
      PGC_SUSET,                                     /* context */
      0,                                             /* flags */
      NULL,                                          /* check_hook */
      NULL,                                          /* assign_hook */
      NULL);                                         /* show_hook */

  DefineCustomIntVariable(
      "pg_diffix.top_count_max",                     /* name */
      "Maximum top contributors count (inclusive).", /* short_desc */
      NULL,                                          /* long_desc */
      &Config.top_count_max,                         /* valueAddr */
      INITIAL_TOP_COUNT_MAX,                         /* bootValue */
      0,                                             /* minValue */
      MAX_NUMERIC_CONFIG,                            /* maxValue */
      PGC_SUSET,                                     /* context */
      0,                                             /* flags */
      NULL,                                          /* check_hook */
      NULL,                                          /* assign_hook */
      NULL);                                         /* show_hook */

  /*
   * Hooks
   */
  prev_post_parse_analyze_hook = post_parse_analyze_hook;
  post_parse_analyze_hook = pg_diffix_post_parse_analyze;

  prev_planner_hook = planner_hook;
  planner_hook = pg_diffix_planner;

  prev_ExecutorStart_hook = ExecutorStart_hook;
  ExecutorStart_hook = pg_diffix_ExecutorStart;

  prev_ExecutorRun_hook = ExecutorRun_hook;
  ExecutorRun_hook = pg_diffix_ExecutorRun;

  prev_ExecutorFinish_hook = ExecutorFinish_hook;
  ExecutorFinish_hook = pg_diffix_ExecutorFinish;

  prev_ExecutorEnd_hook = ExecutorEnd_hook;
  ExecutorEnd_hook = pg_diffix_ExecutorEnd;
}

void _PG_fini(void)
{
  DEBUG_PRINT("Deactivating Diffix extension...");

  free_diffix_config();
  free_oid_cache();

  post_parse_analyze_hook = prev_post_parse_analyze_hook;
  planner_hook = prev_planner_hook;
  ExecutorStart_hook = prev_ExecutorStart_hook;
  ExecutorRun_hook = prev_ExecutorRun_hook;
  ExecutorFinish_hook = prev_ExecutorFinish_hook;
  ExecutorEnd_hook = prev_ExecutorEnd_hook;
}
