#include "postgres.h"

/* Current user and role checking */
#include "miscadmin.h"
#include "utils/acl.h"

#include "pg_diffix/hooks.h"
#include "pg_diffix/node_helpers.h"
#include "pg_diffix/oid_cache.h"
#include "pg_diffix/rewrite.h"
#include "pg_diffix/utils.h"
#include "pg_diffix/validation.h"

post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
planner_hook_type prev_planner_hook = NULL;
ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;
ExecutorFinish_hook_type prev_ExecutorFinish_hook = NULL;
ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

static inline bool requires_anonymization(Query *query)
{
  List *relations = gather_sensitive_relations(query, true);
  if (relations != NIL)
  {
    list_free(relations);
    return true;
  }
  else
  {
    /* No sensitive relations. */
    return false;
  }
}

void pg_diffix_post_parse_analyze(ParseState *pstate, Query *query)
{
  if (prev_post_parse_analyze_hook)
  {
    prev_post_parse_analyze_hook(pstate, query);
  }

  static uint64 next_query_id = 1;
  query->queryId = next_query_id++;

  /* If it's a non-anonymizing query we let it pass through. */
  if (!requires_anonymization(query))
  {
    DEBUG_LOG("Non-anonymizing query (Query ID=%lu) (User ID=%u) %s", query->queryId, GetUserId(), nodeToString(query));
    return;
  }

  /* At this point we have an anonymizing query. */
  DEBUG_LOG("Anonymizing query (Query ID=%lu) (User ID=%u) %s", query->queryId, GetUserId(), nodeToString(query));

  /*
   * We load OIDs later because experimentation shows that UDFs may return
   * INVALIDOID (0) during _PG_init. Does nothing if OIDs are already loaded.
   */
  load_oid_cache();

  /* Halts execution if requirements are not met. */
  verify_anonymization_requirements(query);

  rewrite_query(query);

  /* Print rewritten query. */
  DEBUG_LOG("Rewritten query (Query ID=%lu) (User ID=%u) %s", query->queryId, GetUserId(), nodeToString(query));
}

PlannedStmt *pg_diffix_planner(
    Query *parse,
    const char *query_string,
    int cursorOptions,
    ParamListInfo boundParams)
{
  PlannedStmt *plan;

  if (prev_planner_hook)
  {
    plan = prev_planner_hook(parse, query_string, cursorOptions, boundParams);
  }
  else
  {
    plan = standard_planner(parse, query_string, cursorOptions, boundParams);
  }

  return plan;
}

void pg_diffix_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
  if (prev_ExecutorStart_hook)
  {
    prev_ExecutorStart_hook(queryDesc, eflags);
  }
  else
  {
    standard_ExecutorStart(queryDesc, eflags);
  }
}

void pg_diffix_ExecutorRun(
    QueryDesc *queryDesc,
    ScanDirection direction,
    uint64 count,
    bool execute_once)
{
  if (prev_ExecutorRun_hook)
  {
    prev_ExecutorRun_hook(queryDesc, direction, count, execute_once);
  }
  else
  {
    standard_ExecutorRun(queryDesc, direction, count, execute_once);
  }
}

void pg_diffix_ExecutorFinish(QueryDesc *queryDesc)
{
  if (prev_ExecutorFinish_hook)
  {
    prev_ExecutorFinish_hook(queryDesc);
  }
  else
  {
    standard_ExecutorFinish(queryDesc);
  }
}

void pg_diffix_ExecutorEnd(QueryDesc *queryDesc)
{
  if (prev_ExecutorEnd_hook)
  {
    prev_ExecutorEnd_hook(queryDesc);
  }
  else
  {
    standard_ExecutorEnd(queryDesc);
  }
}
