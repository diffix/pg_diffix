#include "postgres.h"
#include "nodes/nodeFuncs.h"
#include "utils/elog.h"

#include "pg_diffix/validation.h"
#include "pg_diffix/config.h"

#define FAILWITH(...) ereport(ERROR, (errmsg("[PG_DIFFIX] " __VA_ARGS__)))

#define NOT_SUPPORTED(cond, feature)                                 \
  if (cond)                                                          \
  {                                                                  \
    FAILWITH("Feature '%s' is not currently supported.", (feature)); \
  }

static bool configured_relation_walker(Node *node, void *context);
static void verify_rtable(Query *query);
static void verify_join_tree(Query *query);

bool requires_anonymization(Query *query)
{
  return configured_relation_walker((Node *)query, NULL);
}

/*
 * Returns true if the query tree contains any configured relation.
 */
static bool configured_relation_walker(Node *node, void *context)
{
  if (node == NULL)
  {
    return false;
  }

  if (IsA(node, Query))
  {
    Query *query = (Query *)node;

    /*
     * We don't care about non-SELECT queries.
     * Write permissions should be handled by other means.
     */
    if (query->commandType != CMD_SELECT)
    {
      return false;
    }

    return range_table_walker(
        query->rtable,
        configured_relation_walker,
        context,
        QTW_EXAMINE_RTES_BEFORE);
  }

  if (IsA(node, RangeTblEntry))
  {
    RangeTblEntry *rte = (RangeTblEntry *)node;
    if (rte->relid && get_relation_config(rte->relid) != NULL)
    {
      return true;
    }
  }

  return false;
}

void verify_anonymization_requirements(Query *query)
{
  NOT_SUPPORTED(query->commandType != CMD_SELECT, "non-select query");
  NOT_SUPPORTED(query->cteList, "WITH");
  NOT_SUPPORTED(query->hasForUpdate, "FOR [KEY] UPDATE/SHARE");
  NOT_SUPPORTED(query->hasSubLinks, "SubLinks");
  NOT_SUPPORTED(query->hasTargetSRFs, "SRF functions");
  NOT_SUPPORTED(query->groupingSets, "GROUPING SETS");
  NOT_SUPPORTED(query->havingQual, "HAVING");
  NOT_SUPPORTED(query->windowClause, "window functions");
  NOT_SUPPORTED(query->distinctClause, "DISTINCT");
  NOT_SUPPORTED(query->sortClause, "ORDER BY");
  NOT_SUPPORTED(query->limitOffset, "OFFSET");
  NOT_SUPPORTED(query->limitCount, "LIMIT");
  NOT_SUPPORTED(query->setOperations, "UNION/INTERSECT");

  verify_join_tree(query);
  verify_rtable(query);
}

static void verify_rtable(Query *query)
{
  RangeTblEntry *range_table;

  NOT_SUPPORTED(!query->rtable || query->rtable->length != 1, "multiple relations");

  range_table = linitial(query->rtable);
  NOT_SUPPORTED(range_table->rtekind != RTE_RELATION, "subqueries");
}

static void verify_join_tree(Query *query)
{
  List *from_list = query->jointree->fromlist;

  NOT_SUPPORTED(!from_list || from_list->length != 1, "joins");

  if (!IsA(linitial(from_list), RangeTblRef))
  {
    FAILWITH("Query range must be a configured relation.");
  }
}
