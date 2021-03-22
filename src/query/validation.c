#include "postgres.h"
#include "nodes/nodeFuncs.h"

#include "pg_diffix/config.h"
#include "pg_diffix/utils.h"
#include "pg_diffix/query/validation.h"
#include "pg_diffix/query/oid_cache.h"

#define NOT_SUPPORTED(cond, feature) \
  if (cond)                          \
    FAILWITH("Feature '%s' is not currently supported.", (feature));

static void verify_rtable(Query *query);
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

  verify_aggregators(query);
}

static void verify_rtable(Query *query)
{
  ListCell *lc = NULL;
  foreach (lc, query->rtable)
  {
    RangeTblEntry *range_table = lfirst_node(RangeTblEntry, lc);
    if (range_table->rtekind != RTE_RELATION && range_table->rtekind != RTE_JOIN)
      FAILWITH("Unsupported FROM clause.");
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
      FAILWITH_LOCATION(aggref->location, "Unsupported aggregate in query.");
  }

  return expression_tree_walker(node, verify_aggregator, context);
}

static void verify_aggregators(Query *query)
{
  query_tree_walker(query, verify_aggregator, NULL, 0);
}
