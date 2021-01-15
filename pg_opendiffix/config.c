#include "postgres.h"
#include "catalog/namespace.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "lib/stringinfo.h"

#include "pg_opendiffix/config.h"

static TableConfig *make_table_config(char *rel_namespace_name, char *rel_name, char *aid_attname);

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

  current_config.tables = list_make1(
      make_table_config("public", "users", "id") /* Hard-coded for now. */
  );

  MemoryContextSwitchTo(oldcontext);

  return &current_config;
}

/*
 * Frees memory associated with cached configuration.
 */
void free_opendiffix_config()
{
  if (current_config.tables)
  {
    list_free_deep(current_config.tables);
    current_config.tables = NULL;
  }
}

char *config_to_string(OpenDiffixConfig *config)
{
  StringInfoData string;
  ListCell *cell;

  initStringInfo(&string);
  appendStringInfo(&string, "{OPENDIFFIX_CONFIG :tables (");

  foreach (cell, config->tables)
  {
    TableConfig *table = (TableConfig *)lfirst(cell);
    appendStringInfo(&string, "{TABLE_CONFIG "
                              ":rel_namespace_name \"%s\" "
                              ":rel_namespace_oid %u "
                              ":rel_name \"%s\" "
                              ":rel_oid %u "
                              ":aid_attname \"%s\" "
                              ":aid_attnum %hi}",
                     table->rel_namespace_name,
                     table->rel_namespace_oid,
                     table->rel_name,
                     table->rel_oid,
                     table->aid_attname,
                     table->aid_attnum);
  }

  appendStringInfo(&string, ")}");
  return string.data;
}

static TableConfig *
make_table_config(char *rel_namespace_name, char *rel_name, char *aid_attname)
{
  TableConfig *table;
  Oid rel_namespace_oid;
  Oid rel_oid;
  AttrNumber aid_attnum;

  rel_namespace_oid = get_namespace_oid(rel_namespace_name, false);
  rel_oid = get_relname_relid(rel_name, rel_namespace_oid);
  aid_attnum = get_attnum(rel_oid, aid_attname);

  table = palloc(sizeof(TableConfig));
  table->rel_namespace_name = rel_namespace_name;
  table->rel_namespace_oid = rel_namespace_oid;
  table->rel_name = rel_name;
  table->rel_oid = rel_oid;
  table->aid_attname = aid_attname;
  table->aid_attnum = aid_attnum;

  return table;
}
