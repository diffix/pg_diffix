#include "postgres.h"
#include "lib/stringinfo.h"
#include "fmgr.h"
#include "utils/guc.h"
#include "miscadmin.h"

#include "pg_diffix/config.h"
#include "pg_diffix/utils.h"
#include "pg_diffix/auth.h"

DiffixConfig g_config; /* Gets initialized by config_init. */

static const int MAX_NUMERIC_CONFIG = 1000;

static const struct config_enum_entry access_level_options[] = {
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
  appendStringInfo(&string, " :session_access_level %i", config->session_access_level);
  appendStringInfo(&string, " :noise_seed \"%s\"", config->noise_seed);
  appendStringInfo(&string, " :noise_sigma %f", config->noise_sigma);
  appendStringInfo(&string, " :noise_cutoff %f", config->noise_cutoff);
  appendStringInfo(&string, " :minimum_allowed_aid_values %i", config->minimum_allowed_aid_values);
  appendStringInfo(&string, " :outlier_count_min %i", config->outlier_count_min);
  appendStringInfo(&string, " :outlier_count_max %i", config->outlier_count_max);
  appendStringInfo(&string, " :top_count_min %i", config->top_count_min);
  appendStringInfo(&string, " :top_count_max %i", config->top_count_max);

  appendStringInfo(&string, "}");
  /* end config */

  return string.data;
}

static bool g_initializing = false; /* Set to true during config initialization. */

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
      "pg_diffix.noise_seed",                     /* name */
      "Seed used for initializing noise layers.", /* short_desc */
      NULL,                                       /* long_desc */
      &g_config.noise_seed,                       /* valueAddr */
      "diffix",                                   /* bootValue */
      PGC_SUSET,                                  /* context */
      GUC_SUPERUSER_ONLY,                         /* flags */
      NULL,                                       /* check_hook */
      NULL,                                       /* assign_hook */
      NULL);                                      /* show_hook */

  DefineCustomRealVariable(
      "pg_diffix.noise_sigma",                            /* name */
      "Standard deviation of noise added to aggregates.", /* short_desc */
      NULL,                                               /* long_desc */
      &g_config.noise_sigma,                              /* valueAddr */
      1.0,                                                /* bootValue */
      0,                                                  /* minValue */
      MAX_NUMERIC_CONFIG,                                 /* maxValue */
      PGC_SUSET,                                          /* context */
      0,                                                  /* flags */
      NULL,                                               /* check_hook */
      NULL,                                               /* assign_hook */
      NULL);                                              /* show_hook */

  DefineCustomRealVariable(
      "pg_diffix.noise_cutoff",                                  /* name */
      "Factor for noise SD used to limit absolute noise value.", /* short_desc */
      NULL,                                                      /* long_desc */
      &g_config.noise_cutoff,                                    /* valueAddr */
      3.0,                                                       /* bootValue */
      0,                                                         /* minValue */
      100,                                                       /* maxValue */
      PGC_SUSET,                                                 /* context */
      0,                                                         /* flags */
      NULL,                                                      /* check_hook */
      NULL,                                                      /* assign_hook */
      NULL);                                                     /* show_hook */

  DefineCustomIntVariable(
      "pg_diffix.minimum_allowed_aid_values",                                        /* name */
      "The minimum number of distinct AID values that can be in a reported bucket.", /* short_desc */
      NULL,                                                                          /* long_desc */
      &g_config.minimum_allowed_aid_values,                                          /* valueAddr */
      2,                                                                             /* bootValue */
      2,                                                                             /* minValue */
      MAX_NUMERIC_CONFIG,                                                            /* maxValue */
      PGC_SUSET,                                                                     /* context */
      0,                                                                             /* flags */
      NULL,                                                                          /* check_hook */
      NULL,                                                                          /* assign_hook */
      NULL);                                                                         /* show_hook */

  DefineCustomIntVariable(
      "pg_diffix.outlier_count_min",        /* name */
      "Minimum outlier count (inclusive).", /* short_desc */
      NULL,                                 /* long_desc */
      &g_config.outlier_count_min,          /* valueAddr */
      1,                                    /* bootValue */
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
      2,                                    /* bootValue */
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
      4,                                             /* bootValue */
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
      6,                                             /* bootValue */
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

  g_initializing = false;
}
