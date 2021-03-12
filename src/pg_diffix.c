#include "postgres.h"
#include "fmgr.h"

#include "utils/guc.h"

#include "pg_diffix/auth.h"
#include "pg_diffix/hooks.h"
#include "pg_diffix/utils.h"
#include "pg_diffix/query/oid_cache.h"

#define MAX_NUMERIC_CONFIG 1000

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

static const struct config_enum_entry default_access_level_options[] = {
    {"direct", DIRECT_ACCESS, false},
    {"publish", PUBLISH_ACCESS, false},
    {NULL, 0, false},
};

void _PG_init(void)
{
  static int activation_count = 1;
  DEBUG_LOG("Activating Diffix extension (%i)...", activation_count++);

  /*
   * Variables
   */
  DefineCustomEnumVariable(
      "pg_diffix.default_access_level",                /* name */
      "Access level for users without special roles.", /* short_desc */
      NULL,                                            /* long_desc */
      &g_config.default_access_level,                  /* valueAddr */
      INITIAL_DEFAULT_ACCESS_LEVEL,                    /* bootValue */
      default_access_level_options,                    /* options */
      PGC_SUSET,                                       /* context */
      0,                                               /* flags */
      NULL,                                            /* check_hook */
      NULL,                                            /* assign_hook */
      NULL);                                           /* show_hook */

  DefineCustomStringVariable(
      "pg_diffix.noise_seed",                     /* name */
      "Seed used for initializing noise layers.", /* short_desc */
      NULL,                                       /* long_desc */
      &g_config.noise_seed,                       /* valueAddr */
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
      &g_config.noise_sigma,                              /* valueAddr */
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
      &g_config.noise_cutoff,          /* valueAddr */
      INITIAL_NOISE_CUTOFF,            /* bootValue */
      0,                               /* minValue */
      1e7,                             /* maxValue */
      PGC_SUSET,                       /* context */
      0,                               /* flags */
      NULL,                            /* check_hook */
      NULL,                            /* assign_hook */
      NULL);                           /* show_hook */

  DefineCustomIntVariable(
      "pg_diffix.minimum_allowed_aids",                                        /* name */
      "The minimum number of distinct AIDs that can be in a reported bucket.", /* short_desc */
      NULL,                                                                    /* long_desc */
      &g_config.minimum_allowed_aids,                                          /* valueAddr */
      INITIAL_MINIMUM_ALLOWED_AIDS,                                            /* bootValue */
      2,                                                                       /* minValue */
      MAX_NUMERIC_CONFIG,                                                      /* maxValue */
      PGC_SUSET,                                                               /* context */
      0,                                                                       /* flags */
      NULL,                                                                    /* check_hook */
      NULL,                                                                    /* assign_hook */
      NULL);                                                                   /* show_hook */

  DefineCustomIntVariable(
      "pg_diffix.outlier_count_min",        /* name */
      "Minimum outlier count (inclusive).", /* short_desc */
      NULL,                                 /* long_desc */
      &g_config.outlier_count_min,          /* valueAddr */
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
      &g_config.outlier_count_max,          /* valueAddr */
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
      &g_config.top_count_min,                       /* valueAddr */
      INITIAL_TOP_COUNT_MIN,                         /* bootValue */
      1,                                             /* minValue */
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
      &g_config.top_count_max,                       /* valueAddr */
      INITIAL_TOP_COUNT_MAX,                         /* bootValue */
      1,                                             /* minValue */
      MAX_NUMERIC_CONFIG,                            /* maxValue */
      PGC_SUSET,                                     /* context */
      0,                                             /* flags */
      NULL,                                          /* check_hook */
      NULL,                                          /* assign_hook */
      NULL);                                         /* show_hook */

  char *config_str = config_to_string(&g_config);
  DEBUG_LOG("Config %s", config_str);
  pfree(config_str);

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
  DEBUG_LOG("Deactivating Diffix extension...");

  free_oid_cache();

  post_parse_analyze_hook = prev_post_parse_analyze_hook;
  planner_hook = prev_planner_hook;
  ExecutorStart_hook = prev_ExecutorStart_hook;
  ExecutorRun_hook = prev_ExecutorRun_hook;
  ExecutorFinish_hook = prev_ExecutorFinish_hook;
  ExecutorEnd_hook = prev_ExecutorEnd_hook;
}
