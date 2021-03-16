#include "postgres.h"
#include "lib/stringinfo.h"

#include "pg_diffix/config.h"

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

char *config_to_string(DiffixConfig *config)
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
