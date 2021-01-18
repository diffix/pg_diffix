#include "postgres.h"

#include "utils/elog.h"

#include "pg_opendiffix/validation.h"
#include "pg_opendiffix/config.h"

#define FAILWITH(...) ereport(ERROR, (errmsg("[PG_OPENDIFFIX] " __VA_ARGS__)))

#define NOT_SUPPORTED(cond, feature)                                 \
  if (cond)                                                          \
  {                                                                  \
    FAILWITH("Feature '%s' is not currently supported.", (feature)); \
  }

static void verify_rtable(Query *query);
static void verify_join_tree(Query *query);

/*
 * Returns true if query range contains any sensitive relation.
 * See config.h for relation configuration.
 */
bool requires_anonymization(Query *query)
{
  OpenDiffixConfig *config;
  ListCell *lc;

  /*
   * We don't care about non-SELECT queries.
   * Write permissions should be handled by other means.
   */
  if (query->commandType != CMD_SELECT)
  {
    return false;
  }

  config = get_opendiffix_config();

  foreach (lc, query->rtable)
  {
    RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);

    /* Check if relation OID is present in config. */
    if (rte->relid && get_relation_config(config, rte->relid) != NULL)
    {
      return true;
    }
    else if (rte->subquery && requires_anonymization(rte->subquery))
    {
      return true;
    }
  }

  /* No sensitive relations found in config. We consider it a regular query. */
  return false;
}

/*
 * Verifies that a query matches current anonymization restrictions and limitations.
 * If requirements are not met, an error is reported and execution is halted.
 */
void verify_anonymization_requirements(Query *query)
{
  NOT_SUPPORTED(query->commandType != CMD_SELECT, "non-select query");
  NOT_SUPPORTED(query->cteList, "WITH");
  NOT_SUPPORTED(query->hasForUpdate, "FOR [KEY] UPDATE/SHARE");
  NOT_SUPPORTED(query->hasSubLinks, "SubLinks");
  NOT_SUPPORTED(query->hasTargetSRFs, "SRF functions");
  NOT_SUPPORTED(query->groupClause, "GROUP BY");
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
