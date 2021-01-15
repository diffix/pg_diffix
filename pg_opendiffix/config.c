#include "postgres.h"
#include "catalog/namespace.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "lib/stringinfo.h"

#include "pg_opendiffix/config.h"

static RelationConfig *make_relation_config(char *rel_namespace_name, char *rel_name, char *aid_attname);

static OpenDiffixConfig current_config = {NULL};

/*
 * Gets cached configuration.
 */
OpenDiffixConfig *get_opendiffix_config(void)
{
  return &current_config;
}

/*
 * Loads and caches configuration.
 */
OpenDiffixConfig *load_opendiffix_config(void)
{
  MemoryContext oldcontext;

  oldcontext = MemoryContextSwitchTo(TopMemoryContext);

  /* Data will be fetched from config tables here... */

  current_config.relations = list_make1(
      make_relation_config("public", "users", "id") /* Hard-coded for now. */
  );

  MemoryContextSwitchTo(oldcontext);

  return &current_config;
}

/*
 * Frees memory associated with cached configuration.
 */
void free_opendiffix_config()
{
  if (current_config.relations)
  {
    list_free_deep(current_config.relations);
    current_config.relations = NULL;
  }
}

char *config_to_string(OpenDiffixConfig *config)
{
  StringInfoData string;
  ListCell *cell;

  initStringInfo(&string);
  appendStringInfo(&string, "{OPENDIFFIX_CONFIG :tables (");

  foreach (cell, config->relations)
  {
    RelationConfig *relation = (RelationConfig *)lfirst(cell);
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