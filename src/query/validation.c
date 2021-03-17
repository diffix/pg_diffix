#include "postgres.h"
#include "nodes/nodeFuncs.h"
#include "utils/elog.h"

#include "pg_diffix/config.h"
#include "pg_diffix/query/validation.h"
#include "pg_diffix/query/oid_cache.h"

#define FAILWITH(...) ereport(ERROR, (errmsg("[PG_DIFFIX] " __VA_ARGS__)))

#define NOT_SUPPORTED(cond, feature) \
  if (cond)                          \
    FAILWITH("Feature '%s' is not currently supported.", (feature));

static void verify_rtable(Query *query);
static void verify_join_tree(Query *query);
static void verify_aggregators(Query *query);

void verify_anonymization_requirements(QueryContext *context)
{
  Query *query = context->query;

  NOT_SUPPORTED(query->commandType != CMD_SELECT, "non-select query");
  NOT_SUPPORTED(query->cteList, "WITH");
  NOT_SUPPORTED(query->hasForUpdate, "FOR [KEY] UPDATE/SHARE");
  NOT_SUPPORTED(query->hasSubLinks, "SubLinks");
  NOT_SUPPORTED(query->hasTargetSRFs, "SRF functions");
  NOT_SUPPORTED(query->groupingSets, "GROUPING SETS");
  NOT_SUPPORTED(query->windowClause, "window functions");
  NOT_SUPPORTED(query->distinctClause, "DISTINCT");
  NOT_SUPPORTED(query->setOperations, "UNION/INTERSECT");

  verify_rtable(query);
  verify_join_tree(query);

  verify_aggregators(query);
}

static void verify_rtable(Query *query)
{
  RangeTblEntry *range_table;

  NOT_SUPPORTED(list_length(query->rtable) != 1, "multiple relations");

  range_table = linitial(query->rtable);
  NOT_SUPPORTED(range_table->rtekind != RTE_RELATION, "subqueries");
}

static void verify_join_tree(Query *query)
{
  List *from_list = query->jointree->fromlist;

  NOT_SUPPORTED(list_length(from_list) != 1, "joins");

  if (!IsA(linitial(from_list), RangeTblRef))
  {
    FAILWITH("Query range must be a single sensitive relation.");
  }
}

static bool verify_aggregator(Node *node, void *context)
{
  if (node == NULL)
    return false;

  if (IsA(node, Query))
    return query_tree_walker((Query *)node, verify_aggregator, context, 0);

  if (IsA(node, Aggref))
  {
    Aggref *aggref = (Aggref *)node;
    Oid aggoid = aggref->aggfnoid;

    if (aggoid != g_oid_cache.count && aggoid != g_oid_cache.count_any)
      FAILWITH("Unsupported aggregate in query.");
  }

  return expression_tree_walker(node, verify_aggregator, context);
}

static void verify_aggregators(Query *query)
{
  query_tree_walker(query, verify_aggregator, NULL, 0);
}