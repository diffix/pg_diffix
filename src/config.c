#include "postgres.h"
#include "catalog/namespace.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "lib/stringinfo.h"

#include "pg_diffix/config.h"

static RelationConfig *make_relation_config(
    char *rel_namespace_name,
    char *rel_name,
    char *aid_attname);

DiffixConfig Config = {
    .noise_seed = INITIAL_NOISE_SEED,
    .noise_sigma = INITIAL_NOISE_SIGMA,
    .noise_cutoff = INITIAL_NOISE_CUTOFF,

    .minimum_allowed_aids = INITIAL_MINIMUM_ALLOWED_AIDS,

    .outlier_count_min = INITIAL_OUTLIER_COUNT_MIN,
    .outlier_count_max = INITIAL_OUTLIER_COUNT_MAX,

    .top_count_min = INITIAL_TOP_COUNT_MIN,
    .top_count_max = INITIAL_TOP_COUNT_MAX,

    .relations = NIL,
};

void load_diffix_config(void)
{
  MemoryContext oldcontext;

  /* If it's a reload we make sure previous config is freed. */
  free_diffix_config();

  /* Global context isn't ideal, but we need to reuse the config multiple times. */
  oldcontext = MemoryContextSwitchTo(TopMemoryContext);

  /* Data will be fetched from config tables here... */

  Config.relations = list_make1(
      make_relation_config("public", "users", "id") /* Hard-coded for now. */
  );

  MemoryContextSwitchTo(oldcontext);
}

static RelationConfig *make_relation_config(
    char *rel_namespace_name,
    char *rel_name,
    char *aid_attname)
{
  RelationConfig *relation;
  Oid rel_namespace_oid;
  Oid rel_oid;
  AttrNumber aid_attnum;

  rel_namespace_oid = get_namespace_oid(rel_namespace_name, false);
  rel_oid = get_relname_relid(rel_name, rel_namespace_oid);
  aid_attnum = get_attnum(rel_oid, aid_attname);

  relation = palloc(sizeof(RelationConfig));
  relation->rel_namespace_name = rel_namespace_name;
  relation->rel_namespace_oid = rel_namespace_oid;
  relation->rel_name = rel_name;
  relation->rel_oid = rel_oid;
  relation->aid_attname = aid_attname;
  relation->aid_attnum = aid_attnum;
  relation->aid_atttype = get_atttype(rel_oid, aid_attnum);

  return relation;
}

void free_diffix_config()
{
  if (Config.relations)
  {
    list_free_deep(Config.relations);
    Config.relations = NIL;
  }
}

RelationConfig *get_relation_config(Oid rel_oid)
{
  if (rel_oid == InvalidOid)
  {
    /* A relation with zero OID cannot exist. */
    return NULL;
  }

  ListCell *lc;
  foreach (lc, Config.relations)
  {
    RelationConfig *relation = (RelationConfig *)lfirst(lc);
    if (relation->rel_oid == rel_oid)
    {
      return relation;
    }
  }

  return NULL;
}

char *config_to_string(DiffixConfig *config)
{
  StringInfoData string;
  ListCell *lc;

  initStringInfo(&string);

  /* begin config */
  appendStringInfo(&string, "{DIFFIX_CONFIG");

  appendStringInfo(&string, " :noise_seed \"%s\"", config->noise_seed);
  appendStringInfo(&string, " :noise_sigma %f", config->noise_sigma);
  appendStringInfo(&string, " :noise_cutoff %f", config->noise_cutoff);
  appendStringInfo(&string, " :minimum_allowed_aids %i", config->minimum_allowed_aids);
  appendStringInfo(&string, " :outlier_count_min %i", config->outlier_count_min);
  appendStringInfo(&string, " :outlier_count_max %i", config->outlier_count_max);
  appendStringInfo(&string, " :top_count_min %i", config->top_count_min);
  appendStringInfo(&string, " :top_count_max %i", config->top_count_max);

  /* begin config->tables */
  appendStringInfo(&string, " :sensitive_relations (");
  foreach (lc, config->relations)
  {
    RelationConfig *relation = (RelationConfig *)lfirst(lc);
    appendStringInfo(&string, "{SENSITIVE_RELATION_CONFIG"
                              " :rel_namespace_name \"%s\""
                              " :rel_namespace_oid %u"
                              " :rel_name \"%s\""
                              " :rel_oid %u"
                              " :aid_attname \"%s\""
                              " :aid_attnum %hi"
                              " :aid_atttype %u}",
                     relation->rel_namespace_name,
                     relation->rel_namespace_oid,
                     relation->rel_name,
                     relation->rel_oid,
                     relation->aid_attname,
                     relation->aid_attnum,
                     relation->aid_atttype);
  }
  appendStringInfo(&string, ")");
  /* end config->tables */

  appendStringInfo(&string, "}");
  /* end config */

  return string.data;
}
