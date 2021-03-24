#include "postgres.h"
#include "catalog/namespace.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "pg_diffix/utils.h"
#include "pg_diffix/query/context.h"

typedef struct RelationConfig
{
  char *rel_namespace_name;
  char *rel_name;
  char *aid_attname;
} RelationConfig;

/* Attribute order matches columns in DDL. */
#define ATTNUM_REL_NAMESPACE 1
#define ATTNUM_REL_NAME 2
#define ATTNUM_ATTNAME 3

static RelationConfig *map_config_tuple(HeapTuple heap_tuple, TupleDesc tuple_desc)
{
  bool is_null = false;
  Datum rel_namespace_name = heap_getattr(heap_tuple, ATTNUM_REL_NAMESPACE, tuple_desc, &is_null);
  Datum rel_name = heap_getattr(heap_tuple, ATTNUM_REL_NAME, tuple_desc, &is_null);
  Datum aid_attname = heap_getattr(heap_tuple, ATTNUM_ATTNAME, tuple_desc, &is_null);

  RelationConfig *config = palloc(sizeof(RelationConfig));
  config->rel_namespace_name = text_to_cstring(DatumGetTextPP(rel_namespace_name));
  config->rel_name = text_to_cstring(DatumGetTextPP(rel_name));
  config->aid_attname = text_to_cstring(DatumGetTextPP(aid_attname));

  return config;
}

/* Returns a list of RelationConfig for all configured relations. */
static List *get_all_configured_relations(void)
{
  return scan_table_by_name("diffix", "config", (MapTupleFunc)map_config_tuple);
}

static RelationConfig *find_config(List *relation_configs, char *rel_name, char *rel_ns_name)
{
  ListCell *lc;
  foreach (lc, relation_configs)
  {
    RelationConfig *config = (RelationConfig *)lfirst(lc);
    if (strcasecmp(config->rel_name, rel_name) == 0 && strcasecmp(config->rel_namespace_name, rel_ns_name) == 0)
      return config;
  }

  return NULL;
}

static SensitiveRelation *make_relation_data(RelationConfig *config, Oid rel_oid, Oid rel_namespace_oid, Index rel_index)
{
  AnonymizationID *aid = palloc(sizeof(AnonymizationID));
  aid->attname = config->aid_attname;
  aid->attnum = get_attnum(rel_oid, config->aid_attname);
  get_atttypetypmodcoll(rel_oid, aid->attnum, &aid->atttype, &aid->typmod, &aid->collid);

  SensitiveRelation *relation = palloc(sizeof(SensitiveRelation));
  relation->namespace_name = config->rel_namespace_name;
  relation->namespace_oid = rel_namespace_oid;
  relation->name = config->rel_name;
  relation->oid = rel_oid;
  relation->index = rel_index;
  relation->aids = list_make1(aid);

  return relation;
}

/* Returns a list (of DiffixRelation) of all relations in the query. */
static List *gather_sensitive_relations(Query *query)
{
  List *all_relations = get_all_configured_relations();
  List *result = NIL; /* List with resulting DiffixRelation */

  ListCell *lc;
  foreach (lc, query->rtable)
  {
    RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);
    if (!OidIsValid(rte->relid))
      continue;

    char *rel_name = get_rel_name(rte->relid);

    Oid rel_ns_oid = get_rel_namespace(rte->relid);
    char *rel_ns_name = get_namespace_name(rel_ns_oid);

    RelationConfig *config = find_config(all_relations, rel_name, rel_ns_name);
    if (config != NULL)
    {
      Index rel_index = foreach_current_index(lc) + 1;
      SensitiveRelation *rel_data = make_relation_data(config, rte->relid, rel_ns_oid, rel_index);
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
