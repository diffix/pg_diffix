#include "postgres.h"
#include "catalog/namespace.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "lib/stringinfo.h"

#include <inttypes.h>

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

  /* Hard-coded for now. */
  Config.relations = list_make2(
      make_relation_config("public", "users", "id"),
      make_relation_config("public", "test_customers", "id") /**/
  );
  Config.relations_loaded = true;

  MemoryContextSwitchTo(oldcontext);
}

static RelationConfig *make_relation_config(
    char *rel_namespace_name,
    char *rel_name,
    char *aid_attname)
{
  Oid rel_namespace_oid = get_namespace_oid(rel_namespace_name, false);
  Oid rel_oid = get_relname_relid(rel_name, rel_namespace_oid);
  AttrNumber aid_attnum = get_attnum(rel_oid, aid_attname);

  RelationConfig *relation = palloc(sizeof(RelationConfig));
  relation->rel_namespace_name = rel_namespace_name;
  relation->rel_namespace_oid = rel_namespace_oid;
  relation->rel_name = rel_name;
  relation->rel_oid = rel_oid;
  relation->aid_attname = aid_attname;
  relation->aid_attnum = aid_attnum;

  /* We don't want to crash for missing tables */
  if (rel_oid)
  {
    get_atttypetypmodcoll(rel_oid,
                          aid_attnum,
                          &relation->aid_atttype,
                          &relation->aid_typmod,
                          &relation->aid_collid);
  }
  else
  {
    relation->aid_atttype = InvalidOid;
    relation->aid_typmod = -1;
    relation->aid_collid = InvalidOid;
  }

  return relation;
}

void free_diffix_config()
{
  if (Config.relations != NIL)
  {
    list_free_deep(Config.relations);
    Config.relations = NIL;
  }

  Config.relations_loaded = false;
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
    appendStringInfo(&string,
                     "{SENSITIVE_RELATION_CONFIG"
                     " :rel_namespace_name \"%s\""
                     " :rel_namespace_oid %u"
                     " :rel_name \"%s\""
                     " :rel_oid %u"
                     " :aid_attname \"%s\""
                     " :aid_attnum %hi"
                     " :aid_atttype %u"
                     " :aid_typmod %" PRIi32
                     " :aid_collid %u}",
                     relation->rel_namespace_name,
                     relation->rel_namespace_oid,
                     relation->rel_name,
                     relation->rel_oid,
                     relation->aid_attname,
                     relation->aid_attnum,
                     relation->aid_atttype,
                     relation->aid_typmod,
                     relation->aid_collid);
  }
  appendStringInfo(&string, ")");
  /* end config->tables */

  appendStringInfo(&string, "}");
  /* end config */

  return string.data;
}
