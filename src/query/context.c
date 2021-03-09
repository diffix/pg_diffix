#include "postgres.h"
#include "catalog/namespace.h"
#include "nodes/nodeFuncs.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"

#include "pg_diffix/query/context.h"

typedef struct RelationConfig
{
  char *rel_namespace_name;
  char *rel_name;
  char *aid_attname;
} RelationConfig;

/* Returns a list of RelationConfig for all configured relations. */
static List *get_all_configured_relations(void)
{
  static RelationConfig static_relations[] = {
      {"public", "users", "id"},
      {"public", "test_customers", "id"},
  };

  List *relations = list_make2(
      &static_relations[0],
      &static_relations[1] /**/
  );

  return relations;
}

typedef struct GatherRelationsContext
{
  List *rel_oids;
} GatherRelationsContext;

/* Walks query tree and gathers valid OIDs from all ranges. */
static bool gather_relations_walker(Node *node, GatherRelationsContext *context)
{
  if (node == NULL)
    return false;

  if (IsA(node, Query))
  {
    Query *query = (Query *)node;
    range_table_walker(
        query->rtable,
        gather_relations_walker,
        context,
        QTW_EXAMINE_RTES_BEFORE);
  }
  else if (IsA(node, RangeTblEntry))
  {
    RangeTblEntry *rte = (RangeTblEntry *)node;
    if (rte->relid)
      context->rel_oids = list_append_unique_oid(context->rel_oids, rte->relid);
  }

  return false;
}

/* Returns a list of OID for all relations in the query. */
static List *gather_relation_oids(Query *query)
{
  GatherRelationsContext context = {.rel_oids = NIL};
  range_table_walker(
      query->rtable,
      gather_relations_walker,
      &context,
      QTW_EXAMINE_RTES_BEFORE);
  return context.rel_oids;
}

static RelationConfig *find_config(List *relation_configs, char *rel_name, char *ns_name)
{
  ListCell *lc;
  foreach (lc, relation_configs)
  {
    RelationConfig *config = (RelationConfig *)lfirst(lc);
    if (strcmp(config->rel_name, rel_name) == 0 && strcmp(config->rel_namespace_name, ns_name) == 0)
      return config;
  }

  return NULL;
}

static RelationData *make_relation_data(RelationConfig *config, Oid rel_oid, Oid rel_namespace_oid)
{
  AttrNumber aid_attnum = get_attnum(rel_oid, config->aid_attname);
  RelationData *relation = palloc(sizeof(RelationData));
  relation->rel_namespace_name = config->rel_namespace_name;
  relation->rel_namespace_oid = rel_namespace_oid;
  relation->rel_name = config->rel_name;
  relation->rel_oid = rel_oid;
  relation->aid_attname = config->aid_attname;
  relation->aid_attnum = aid_attnum;
  get_atttypetypmodcoll(rel_oid,
                        aid_attnum,
                        &relation->aid_atttype,
                        &relation->aid_typmod,
                        &relation->aid_collid);
  return relation;
}

/* Returns a list (of RelationData) of all relations in the query. */
static List *gather_sensitive_relations(Query *query)
{
  List *rel_oids = gather_relation_oids(query);
  if (rel_oids == NIL)
    return NIL;

  List *all_relations = get_all_configured_relations();
  List *result = NIL; /* List with resulting RelationData */

  ListCell *lc;
  foreach (lc, rel_oids)
  {
    Oid rel_oid = lfirst_oid(lc);
    char *rel_name = get_rel_name(rel_oid);

    Oid rel_ns_oid = get_rel_namespace(rel_oid);
    char *rel_ns_name = get_namespace_name(rel_ns_oid);

    RelationConfig *config = find_config(all_relations, rel_name, rel_ns_name);
    if (config != NULL)
    {
      RelationData *rel_data = make_relation_data(config, rel_oid, rel_ns_oid);
      result = lappend(result, rel_data);
    }
  }

  return result;
}

QueryContext build_query_context(Query *query)
{
  QueryContext context = {
      .query = query,
      .relations = gather_sensitive_relations(query),
  };
  return context;
}
