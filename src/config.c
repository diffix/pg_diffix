#include "postgres.h"

#include "commands/extension.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/guc.h"

#include "pg_diffix/auth.h"
#include "pg_diffix/config.h"
#include "pg_diffix/utils.h"

DiffixConfig g_config; /* Gets initialized by config_init. */

static const int MAX_NUMERIC_CONFIG = 1000;

static const struct config_enum_entry access_level_options[] = {
    {"direct", ACCESS_DIRECT, false},
    {"anonymized_trusted", ACCESS_ANONYMIZED_TRUSTED, false},
    {"anonymized_untrusted", ACCESS_ANONYMIZED_UNTRUSTED, false},
    {NULL, 0, false},
};

static char *config_to_string(DiffixConfig *config)
{
  StringInfoData string;
  initStringInfo(&string);

  /* begin config */
  appendStringInfo(&string, "{DIFFIX_CONFIG");

  appendStringInfo(&string, " :default_access_level %i", config->default_access_level);
  appendStringInfo(&string, " :session_access_level %i", config->session_access_level);
  appendStringInfo(&string, " :treat_unmarked_tables_as_public %s", (config->treat_unmarked_tables_as_public ? "true" : "false"));
  appendStringInfo(&string, " :strict %s", (config->strict ? "true" : "false"));
  appendStringInfo(&string, " :salt \"%s\"", config->salt);
  appendStringInfo(&string, " :noise_layer_sd %f", config->noise_layer_sd);
  appendStringInfo(&string, " :low_count_min_threshold %i", config->low_count_min_threshold);
  appendStringInfo(&string, " :low_count_mean_gap %f", config->low_count_mean_gap);
  appendStringInfo(&string, " :low_count_layer_sd %f", config->low_count_layer_sd);
  appendStringInfo(&string, " :outlier_count_min %i", config->outlier_count_min);
  appendStringInfo(&string, " :outlier_count_max %i", config->outlier_count_max);
  appendStringInfo(&string, " :top_count_min %i", config->top_count_min);
  appendStringInfo(&string, " :top_count_max %i", config->top_count_max);

  appendStringInfo(&string, "}");
  /* end config */

  return string.data;
}

static bool g_initializing = false; /* Set to true during config initialization. */

/*
 * Because initialization can be done in order out of our control, we can fully validate only single
 * parameters using GUC hooks. Cross-dependent parameters such as intervals are only soft-validated
 * in GUC hooks, issuing warnings under some circumstances. Later they have to be re-validated
 * strictly via `config_validate`, which is called at runtime during query validation.
 */

static bool session_access_level_check(int *newval, void **extra, GucSource source)
{
  /* We can't get user access level during initialization when preloading library. */
  if (g_initializing)
    return true;

  if (!is_pg_diffix_active())
  {
    GUC_check_errmsg_string = "Invalid operation requested for the current session.";
    GUC_check_errdetail_string = "pg_diffix wasn't activated for the current database.";
    return false;
  }

  AccessLevel user_level = get_user_access_level();
  if (is_higher_access_level(*newval, user_level))
  {
    GUC_check_errmsg_string = "Invalid access level requested for the current session.";
    GUC_check_errdetail_string = "Session access level can't be higher than the user access level.";
    return false;
  }

  return true;
}

static const double MIN_STRICT_NOISE_LAYER_SD = 1.0;
static const int MIN_STRICT_LOW_COUNT_MIN_THRESHOLD = 2;
static const double MIN_STRICT_LOW_COUNT_MEAN_GAP = 2.0;
static const double MIN_STRICT_LOW_COUNT_LAYER_SD = 1.0;
static const int MIN_STRICT_OUTLIER_COUNT_MIN = 1;
static const int MIN_STRICT_OUTLIER_COUNT_MAX = 2;
static const int MIN_STRICT_TOP_COUNT_MIN = 2;
static const int MIN_STRICT_TOP_COUNT_MAX = 3;
static const int MIN_STRICT_INTERVAL_SIZE = 1;

static bool strict_check_hook(bool *newval, void **extra, GucSource source)
{
  if (source > PGC_S_DYNAMIC_DEFAULT && *newval)
  {
    bool incorrect = g_config.noise_layer_sd < MIN_STRICT_NOISE_LAYER_SD ||
                     g_config.low_count_min_threshold < MIN_STRICT_LOW_COUNT_MIN_THRESHOLD ||
                     g_config.low_count_mean_gap < MIN_STRICT_LOW_COUNT_MEAN_GAP ||
                     g_config.low_count_layer_sd < MIN_STRICT_LOW_COUNT_LAYER_SD ||
                     g_config.outlier_count_min < MIN_STRICT_OUTLIER_COUNT_MIN ||
                     g_config.outlier_count_max < MIN_STRICT_OUTLIER_COUNT_MAX ||
                     g_config.top_count_min < MIN_STRICT_TOP_COUNT_MIN ||
                     g_config.top_count_max < MIN_STRICT_TOP_COUNT_MAX ||
                     g_config.outlier_count_max - g_config.outlier_count_min < MIN_STRICT_INTERVAL_SIZE ||
                     g_config.top_count_max - g_config.top_count_min < MIN_STRICT_INTERVAL_SIZE;
    if (incorrect)
    {
      NOTICE_LOG("Current values of anonymization parameters do not conform to strict mode.");
      return false;
    }
  }
  return true;
}

static bool noise_layer_sd_check_hook(double *newval, void **extra, GucSource source)
{
  if (g_config.strict && *newval < MIN_STRICT_NOISE_LAYER_SD)
  {
    NOTICE_LOG("noise_layer_sd must be greater than or equal to %f.", MIN_STRICT_NOISE_LAYER_SD);
    return false;
  }
  return true;
}

static bool low_count_min_threshold_check_hook(int *newval, void **extra, GucSource source)
{
  if (g_config.strict && *newval < MIN_STRICT_LOW_COUNT_MIN_THRESHOLD)
  {
    NOTICE_LOG("low_count_min_threshold must be greater than or equal to %d.", MIN_STRICT_LOW_COUNT_MIN_THRESHOLD);
    return false;
  }
  return true;
}

static bool low_count_mean_gap_check_hook(double *newval, void **extra, GucSource source)
{
  if (g_config.strict && *newval < MIN_STRICT_LOW_COUNT_MEAN_GAP)
  {
    NOTICE_LOG("low_count_mean_gap must be greater than or equal to %f.", MIN_STRICT_LOW_COUNT_MEAN_GAP);
    return false;
  }
  return true;
}

static bool low_count_layer_sd_check_hook(double *newval, void **extra, GucSource source)
{
  if (g_config.strict && *newval < MIN_STRICT_LOW_COUNT_LAYER_SD)
  {
    NOTICE_LOG("low_count_layer_sd must be greater than or equal to %f.", MIN_STRICT_LOW_COUNT_LAYER_SD);
    return false;
  }
  return true;
}

/*
 * `check_hook`s for intervals only issue warnings for issues with _combinations_ of parameter values.
 * These only make sense in interactive mode.
 */
static bool interval_check_hook(int *new_bound, GucSource source, int other_bound, int min_strict_bound, bool for_min)
{
  int lower_bound = for_min ? *new_bound : other_bound;
  int upper_bound = for_min ? other_bound : *new_bound;
  if (source >= PGC_S_INTERACTIVE && lower_bound > upper_bound)
  {
    NOTICE_LOG("Interval invalid: (%d, %d). Set other bound to make it valid.", lower_bound, upper_bound);
  }
  if (g_config.strict && *new_bound < min_strict_bound)
  {
    NOTICE_LOG("Must be greater than or equal to %d.", min_strict_bound);
    return false;
  }
  if (source >= PGC_S_INTERACTIVE && g_config.strict && upper_bound - lower_bound < MIN_STRICT_INTERVAL_SIZE)
  {
    NOTICE_LOG("Bounds must differ by at least %d. Set other bound to make it valid.",
               MIN_STRICT_INTERVAL_SIZE);
  }
  return true;
}

static bool outlier_count_min_check_hook(int *newval, void **extra, GucSource source)
{
  return interval_check_hook(newval, source, g_config.outlier_count_max, MIN_STRICT_OUTLIER_COUNT_MIN, true);
}

static bool outlier_count_max_check_hook(int *newval, void **extra, GucSource source)
{
  return interval_check_hook(newval, source, g_config.outlier_count_min, MIN_STRICT_OUTLIER_COUNT_MAX, false);
}

static bool top_count_min_check_hook(int *newval, void **extra, GucSource source)
{
  return interval_check_hook(newval, source, g_config.top_count_max, MIN_STRICT_TOP_COUNT_MIN, true);
}

static bool top_count_max_check_hook(int *newval, void **extra, GucSource source)
{
  return interval_check_hook(newval, source, g_config.top_count_min, MIN_STRICT_TOP_COUNT_MAX, false);
}

void config_init(void)
{
  g_initializing = true;

  DefineCustomEnumVariable(
      "pg_diffix.session_access_level",    /* name */
      "Access level for current session.", /* short_desc */
      NULL,                                /* long_desc */
      &g_config.session_access_level,      /* valueAddr */
      ACCESS_DIRECT,                       /* bootValue */
      access_level_options,                /* options */
      PGC_USERSET,                         /* context */
      0,                                   /* flags */
      session_access_level_check,          /* check_hook */
      NULL,                                /* assign_hook */
      NULL);                               /* show_hook */

  DefineCustomEnumVariable(
      "pg_diffix.default_access_level",    /* name */
      "Access level for unlabeled users.", /* short_desc */
      NULL,                                /* long_desc */
      &g_config.default_access_level,      /* valueAddr */
      ACCESS_DIRECT,                       /* bootValue */
      access_level_options,                /* options */
      PGC_SUSET,                           /* context */
      0,                                   /* flags */
      NULL,                                /* check_hook */
      NULL,                                /* assign_hook */
      NULL);                               /* show_hook */

  DefineCustomBoolVariable(
      "pg_diffix.treat_unmarked_tables_as_public",                                 /* name */
      "Controls whether unmarked tables are readable and treated as public data.", /* short_desc */
      NULL,                                                                        /* long_desc */
      &g_config.treat_unmarked_tables_as_public,                                   /* valueAddr */
      false,                                                                       /* bootValue */
      PGC_SUSET,                                                                   /* context */
      0,                                                                           /* flags */
      NULL,                                                                        /* check_hook */
      NULL,                                                                        /* assign_hook */
      NULL);                                                                       /* show_hook */

  DefineCustomBoolVariable(
      "pg_diffix.strict", /* name */
      "Controls whether the anonymization parameters must be checked strictly, i.e. to ensure "
      "safe minimum level of anonymization.", /* short_desc */
      NULL,                                   /* long_desc */
      &g_config.strict,                       /* valueAddr */
      true,                                   /* bootValue */
      PGC_SUSET,                              /* context */
      0,                                      /* flags */
      &strict_check_hook,                     /* check_hook */
      NULL,                                   /* assign_hook */
      NULL);                                  /* show_hook */

  DefineCustomStringVariable(
      "pg_diffix.salt",                              /* name */
      "Secret value used for seeding noise layers.", /* short_desc */
      NULL,                                          /* long_desc */
      &g_config.salt,                                /* valueAddr */
      "",                                            /* bootValue */
      PGC_SUSET,                                     /* context */
      GUC_SUPERUSER_ONLY,                            /* flags */
      NULL,                                          /* check_hook */
      NULL,                                          /* assign_hook */
      NULL);                                         /* show_hook */

  DefineCustomRealVariable(
      "pg_diffix.noise_layer_sd",                                     /* name */
      "Standard deviation for each noise layer added to aggregates.", /* short_desc */
      NULL,                                                           /* long_desc */
      &g_config.noise_layer_sd,                                       /* valueAddr */
      1.0,                                                            /* bootValue */
      0,                                                              /* minValue */
      MAX_NUMERIC_CONFIG,                                             /* maxValue */
      PGC_SUSET,                                                      /* context */
      0,                                                              /* flags */
      &noise_layer_sd_check_hook,                                     /* check_hook */
      NULL,                                                           /* assign_hook */
      NULL);                                                          /* show_hook */

  DefineCustomIntVariable(
      "pg_diffix.low_count_min_threshold",              /* name */
      "Lower bound of the low count filter threshold.", /* short_desc */
      NULL,                                             /* long_desc */
      &g_config.low_count_min_threshold,                /* valueAddr */
      3,                                                /* bootValue */
      1,                                                /* minValue */
      MAX_NUMERIC_CONFIG,                               /* maxValue */
      PGC_SUSET,                                        /* context */
      0,                                                /* flags */
      &low_count_min_threshold_check_hook,              /* check_hook */
      NULL,                                             /* assign_hook */
      NULL);                                            /* show_hook */

  DefineCustomRealVariable(
      "pg_diffix.low_count_mean_gap",             /* name */
      "Number of standard deviations between the lower bound \
and the mean of the low count filter threshold.", /* short_desc */
      NULL,                                       /* long_desc */
      &g_config.low_count_mean_gap,               /* valueAddr */
      2.0,                                        /* bootValue */
      0,                                          /* minValue */
      MAX_NUMERIC_CONFIG,                         /* maxValue */
      PGC_SUSET,                                  /* context */
      0,                                          /* flags */
      &low_count_mean_gap_check_hook,             /* check_hook */
      NULL,                                       /* assign_hook */
      NULL);                                      /* show_hook */

  DefineCustomRealVariable(
      "pg_diffix.low_count_layer_sd",                                               /* name */
      "Standard deviation for each noise layer of the low count filter threshold.", /* short_desc */
      NULL,                                                                         /* long_desc */
      &g_config.low_count_layer_sd,                                                 /* valueAddr */
      1.0,                                                                          /* bootValue */
      0,                                                                            /* minValue */
      MAX_NUMERIC_CONFIG,                                                           /* maxValue */
      PGC_SUSET,                                                                    /* context */
      0,                                                                            /* flags */
      &low_count_layer_sd_check_hook,                                               /* check_hook */
      NULL,                                                                         /* assign_hook */
      NULL);                                                                        /* show_hook */

  DefineCustomIntVariable(
      "pg_diffix.outlier_count_min",                 /* name */
      "Minimum outlier count (inclusive).",          /* short_desc */
      "Must not be greater than outlier_count_max.", /* long_desc */
      &g_config.outlier_count_min,                   /* valueAddr */
      1,                                             /* bootValue */
      0,                                             /* minValue */
      MAX_NUMERIC_CONFIG,                            /* maxValue */
      PGC_SUSET,                                     /* context */
      0,                                             /* flags */
      &outlier_count_min_check_hook,                 /* check_hook */
      NULL,                                          /* assign_hook */
      NULL);                                         /* show_hook */

  DefineCustomIntVariable(
      "pg_diffix.outlier_count_max",                 /* name */
      "Maximum outlier count (inclusive).",          /* short_desc */
      "Must not be smaller than outlier_count_min.", /* long_desc */
      &g_config.outlier_count_max,                   /* valueAddr */
      2,                                             /* bootValue */
      0,                                             /* minValue */
      MAX_NUMERIC_CONFIG,                            /* maxValue */
      PGC_SUSET,                                     /* context */
      0,                                             /* flags */
      &outlier_count_max_check_hook,                 /* check_hook */
      NULL,                                          /* assign_hook */
      NULL);                                         /* show_hook */

  DefineCustomIntVariable(
      "pg_diffix.top_count_min",                     /* name */
      "Minimum top contributors count (inclusive).", /* short_desc */
      "Must not be greater than top_count_max.",     /* long_desc */
      &g_config.top_count_min,                       /* valueAddr */
      3,                                             /* bootValue */
      1,                                             /* minValue */
      MAX_NUMERIC_CONFIG,                            /* maxValue */
      PGC_SUSET,                                     /* context */
      0,                                             /* flags */
      &top_count_min_check_hook,                     /* check_hook */
      NULL,                                          /* assign_hook */
      NULL);                                         /* show_hook */

  DefineCustomIntVariable(
      "pg_diffix.top_count_max",                     /* name */
      "Maximum top contributors count (inclusive).", /* short_desc */
      "Must not be smaller than top_count_min.",     /* long_desc */
      &g_config.top_count_max,                       /* valueAddr */
      4,                                             /* bootValue */
      1,                                             /* minValue */
      MAX_NUMERIC_CONFIG,                            /* maxValue */
      PGC_SUSET,                                     /* context */
      0,                                             /* flags */
      &top_count_max_check_hook,                     /* check_hook */
      NULL,                                          /* assign_hook */
      NULL);                                         /* show_hook */

  DefineCustomBoolVariable(
      "pg_diffix.compute_suppress_bin",                                                 /* name */
      "Whether the suppress bin should be computed and included in the query results.", /* short_desc */
      NULL,                                                                             /* long_desc */
      &g_config.compute_suppress_bin,                                                   /* valueAddr */
      true,                                                                             /* bootValue */
      PGC_USERSET,                                                                      /* context */
      0,                                                                                /* flags */
      NULL,                                                                             /* check_hook */
      NULL,                                                                             /* assign_hook */
      NULL);                                                                            /* show_hook */

  DefineCustomStringVariable(
      "pg_diffix.text_label_for_suppress_bin",                                    /* name */
      "Value to use for the text-typed grouping labels in the suppress bin row.", /* short_desc */
      NULL,                                                                       /* long_desc */
      &g_config.text_label_for_suppress_bin,                                      /* valueAddr */
      "*",                                                                        /* bootValue */
      PGC_USERSET,                                                                /* context */
      0,                                                                          /* flags */
      NULL,                                                                       /* check_hook */
      NULL,                                                                       /* assign_hook */
      NULL);                                                                      /* show_hook */

  char *config_str = config_to_string(&g_config);
  DEBUG_LOG("Config %s", config_str);
  pfree(config_str);

  g_initializing = false;
}

void config_validate(void)
{
  if (g_config.top_count_min > g_config.top_count_max)
    FAILWITH("pg_diffix is misconfigured: top_count_min > top_count_max.");
  if (g_config.outlier_count_min > g_config.outlier_count_max)
    FAILWITH("pg_diffix is misconfigured: outlier_count_min > outlier_count_max.");
  if (g_config.strict && g_config.top_count_max - g_config.top_count_min < MIN_STRICT_INTERVAL_SIZE)
    FAILWITH("pg_diffix is misconfigured: top_count_max - top_count_min < %d.", MIN_STRICT_INTERVAL_SIZE);
  if (g_config.strict && g_config.outlier_count_max - g_config.outlier_count_min < MIN_STRICT_INTERVAL_SIZE)
    FAILWITH("pg_diffix is misconfigured: outlier_count_max - outlier_count_min < %d.", MIN_STRICT_INTERVAL_SIZE);
}

bool is_pg_diffix_active(void)
{
  return OidIsValid(get_extension_oid("pg_diffix", true));
}
