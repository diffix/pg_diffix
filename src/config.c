#include "postgres.h"

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
    {"publish_trusted", ACCESS_PUBLISH_TRUSTED, false},
    {"publish_untrusted", ACCESS_PUBLISH_UNTRUSTED, false},
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

  AccessLevel user_level = get_user_access_level();
  if (is_higher_access_level(*newval, user_level))
  {
    GUC_check_errmsg_string = "Invalid access level requested for the current session.";
    GUC_check_errdetail_string = "Session access level can't be higher than the user access level.";
    return false;
  }

  return true;
}

/* `check_hook`s for intervals only issue warnings, so they only make sense in interactive mode. */
static bool outlier_count_min_check_hook(int *newval, void **extra, GucSource source)
{
  if (source >= PGC_S_INTERACTIVE && *newval > g_config.outlier_count_max)
  {
    NOTICE_LOG("Outlier count interval invalid: (%d, %d). Set upper bound to make it valid.", *newval, g_config.outlier_count_max);
  }
  return true;
}

static bool outlier_count_max_check_hook(int *newval, void **extra, GucSource source)
{
  if (source >= PGC_S_INTERACTIVE && *newval < g_config.outlier_count_min)
  {
    NOTICE_LOG("Outlier count interval invalid: (%d, %d). Set lower bound to make it valid.", g_config.outlier_count_min, *newval);
  }
  return true;
}

static bool top_count_min_check_hook(int *newval, void **extra, GucSource source)
{
  if (source >= PGC_S_INTERACTIVE && *newval > g_config.top_count_max)
  {
    NOTICE_LOG("Top count interval invalid: (%d, %d). Set upper bound to make it valid.", *newval, g_config.top_count_max);
  }
  return true;
}

static bool top_count_max_check_hook(int *newval, void **extra, GucSource source)
{
  if (source >= PGC_S_INTERACTIVE && *newval < g_config.top_count_min)
  {
    NOTICE_LOG("Top count interval invalid: (%d, %d). Set lower bound to make it valid.", g_config.top_count_min, *newval);
  }
  return true;
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

  DefineCustomStringVariable(
      "pg_diffix.salt",                              /* name */
      "Secret value used for seeding noise layers.", /* short_desc */
      NULL,                                          /* long_desc */
      &g_config.salt,                                /* valueAddr */
      "diffix",                                      /* bootValue */
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
      NULL,                                                           /* check_hook */
      NULL,                                                           /* assign_hook */
      NULL);                                                          /* show_hook */

  DefineCustomIntVariable(
      "pg_diffix.low_count_min_threshold",              /* name */
      "Lower bound of the low count filter threshold.", /* short_desc */
      NULL,                                             /* long_desc */
      &g_config.low_count_min_threshold,                /* valueAddr */
      2,                                                /* bootValue */
      2,                                                /* minValue */
      MAX_NUMERIC_CONFIG,                               /* maxValue */
      PGC_SUSET,                                        /* context */
      0,                                                /* flags */
      NULL,                                             /* check_hook */
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
      NULL,                                       /* check_hook */
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
      NULL,                                                                         /* check_hook */
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
      4,                                             /* bootValue */
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
      6,                                             /* bootValue */
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
      PGC_SUSET,                                                                        /* context */
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
      PGC_SUSET,                                                                  /* context */
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
}
