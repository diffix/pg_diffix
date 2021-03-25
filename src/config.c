#include "postgres.h"
#include "lib/stringinfo.h"
#include "fmgr.h"
#include "utils/guc.h"

#include "pg_diffix/config.h"
#include "pg_diffix/utils.h"

#define INITIAL_DEFAULT_ACCESS_LEVEL ACCESS_DIRECT

#define INITIAL_NOISE_SEED "diffix"
#define INITIAL_NOISE_SIGMA 1.0
#define INITIAL_NOISE_CUTOFF 5.0

#define INITIAL_MINIMUM_ALLOWED_AIDS 2

#define INITIAL_OUTLIER_COUNT_MIN 1
#define INITIAL_OUTLIER_COUNT_MAX 2

#define INITIAL_TOP_COUNT_MIN 4
#define INITIAL_TOP_COUNT_MAX 6

DiffixConfig g_config = {
    .default_access_level = INITIAL_DEFAULT_ACCESS_LEVEL,

    .noise_seed = INITIAL_NOISE_SEED,
    .noise_sigma = INITIAL_NOISE_SIGMA,
    .noise_cutoff = INITIAL_NOISE_CUTOFF,

    .minimum_allowed_aids = INITIAL_MINIMUM_ALLOWED_AIDS,

    .outlier_count_min = INITIAL_OUTLIER_COUNT_MIN,
    .outlier_count_max = INITIAL_OUTLIER_COUNT_MAX,

    .top_count_min = INITIAL_TOP_COUNT_MIN,
    .top_count_max = INITIAL_TOP_COUNT_MAX,
};

static const int MAX_NUMERIC_CONFIG = 1000;

static const struct config_enum_entry default_access_level_options[] = {
    {"direct", ACCESS_DIRECT, false},
    {"publish", ACCESS_PUBLISH, false},
    {NULL, 0, false},
};

static char *config_to_string(DiffixConfig *config)
{
  StringInfoData string;
  initStringInfo(&string);

  /* begin config */
  appendStringInfo(&string, "{DIFFIX_CONFIG");

  appendStringInfo(&string, " :default_access_level %i", config->default_access_level);
  appendStringInfo(&string, " :noise_seed \"%s\"", config->noise_seed);
  appendStringInfo(&string, " :noise_sigma %f", config->noise_sigma);
  appendStringInfo(&string, " :noise_cutoff %f", config->noise_cutoff);
  appendStringInfo(&string, " :minimum_allowed_aids %i", config->minimum_allowed_aids);
  appendStringInfo(&string, " :outlier_count_min %i", config->outlier_count_min);
  appendStringInfo(&string, " :outlier_count_max %i", config->outlier_count_max);
  appendStringInfo(&string, " :top_count_min %i", config->top_count_min);
  appendStringInfo(&string, " :top_count_max %i", config->top_count_max);

  appendStringInfo(&string, "}");
  /* end config */

  return string.data;
}

void config_init(void)
{
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
}
