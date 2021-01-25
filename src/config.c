#include "postgres.h"
#include "catalog/namespace.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "lib/stringinfo.h"

#include "pg_diffix/config.h"

static RelationConfig *
make_relation_config(char *rel_namespace_name, char *rel_name, char *aid_attname);

DiffixConfig Config = {
    .noise_seed = INITIAL_NOISE_SEED,
    .noise_sigma = INITIAL_NOISE_SIGMA,

    .low_count_threshold_min = INITIAL_LOW_COUNT_THRESHOLD_MIN,
    .low_count_threshold_max = INITIAL_LOW_COUNT_THRESHOLD_MAX,

    .outlier_count_min = INITIAL_OUTLIER_COUNT_MIN,
    .outlier_count_max = INITIAL_OUTLIER_COUNT_MAX,

    .top_count_min = INITIAL_TOP_COUNT_MIN,
    .top_count_max = INITIAL_TOP_COUNT_MAX,

    .relations = NIL};

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

void free_diffix_config()
{
  if (Config.relations)
  {
    list_free_deep(Config.relations);
    Config.relations = NIL;
  }
}

RelationConfig *get_relation_config(DiffixConfig *config, Oid rel_oid)
{
  ListCell *lc;

  foreach (lc, config->relations)
  {
    RelationConfig *relation = (RelationConfig *)lfirst(lc);
    if (relation->rel_oid == rel_oid)
    {
      return relation;
    }
  }

  return NULL;
}

static RelationConfig *
make_relation_config(char *rel_namespace_name, char *rel_name, char *aid_attname)
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

  return relation;
}

char *config_to_string(DiffixConfig *config)
{
  StringInfoData string;
  ListCell *lc;

  initStringInfo(&string);
  appendStringInfo(&string, "{DIFFIX_CONFIG :tables (");

  foreach (lc, config->relations)
  {
    RelationConfig *relation = (RelationConfig *)lfirst(lc);
    appendStringInfo(&string, "{TABLE_CONFIG "
                              ":rel_namespace_name \"%s\" "
                              ":rel_namespace_oid %u "
                              ":rel_name \"%s\" "
                              ":rel_oid %u "
                              ":aid_attname \"%s\" "
                              ":aid_attnum %hi}",
                     relation->rel_namespace_name,
                     relation->rel_namespace_oid,
                     relation->rel_name,
                     relation->rel_oid,
                     relation->aid_attname,
                     relation->aid_attnum);
  }

  appendStringInfo(&string, ")}");
  return string.data;
}
