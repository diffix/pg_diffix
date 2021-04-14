#include "postgres.h"
#include "miscadmin.h"

#include "pg_diffix/auth.h"
#include "pg_diffix/config.h"
#include "pg_diffix/hooks.h"
#include "pg_diffix/utils.h"
#include "pg_diffix/query/oid_cache.h"
#include "pg_diffix/query/rewrite.h"
#include "pg_diffix/query/validation.h"

/* Hooks type definitions */
#include "optimizer/planner.h"
#include "executor/executor.h"

planner_hook_type prev_planner_hook = NULL;
ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;
ExecutorFinish_hook_type prev_ExecutorFinish_hook = NULL;
ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

static void prepare_query(Query *query)
{
  static uint64 next_query_id = 1;
  query->queryId = next_query_id++;

  /* Do nothing for sessions with direct access. */
  if (get_session_access_level() == ACCESS_DIRECT)
  {
    DEBUG_LOG("Direct query (Query ID=%lu) (User ID=%u) %s", query->queryId, GetSessionUserId(), nodeToString(query));
    return;
  }

  QueryContext context = build_query_context(query);

  /* A query requires anonymization if it targets sensitive relations. */
  if (context.relations == NIL)
  {
    DEBUG_LOG("Non-anonymizing query (Query ID=%lu) (User ID=%u) %s", query->queryId, GetSessionUserId(), nodeToString(query));
    return;
  }

  /* At this point we have an anonymizing query. */
  DEBUG_LOG("Anonymizing query (Query ID=%lu) (User ID=%u) %s", query->queryId, GetSessionUserId(), nodeToString(query));

  /* We load OIDs lazily because experimentation shows that UDFs may return INVALIDOID (0) during _PG_init. */
  oid_cache_init();

  /* Halts execution if requirements are not met. */
  verify_anonymization_requirements(&context);

  rewrite_query(&context);

  /* Print rewritten query. */
  DEBUG_LOG("Rewritten query (Query ID=%lu) (User ID=%u) %s", query->queryId, GetSessionUserId(), nodeToString(query));
}

static PlannedStmt *pg_diffix_planner(
    Query *parse,
    const char *query_string,
    int cursorOptions,
    ParamListInfo boundParams)
{
  prepare_query(parse);

  PlannedStmt *plan;
  if (prev_planner_hook)
    plan = prev_planner_hook(parse, query_string, cursorOptions, boundParams);
  else
    plan = standard_planner(parse, query_string, cursorOptions, boundParams);

  return plan;
}

static void pg_diffix_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
  if (prev_ExecutorStart_hook)
    prev_ExecutorStart_hook(queryDesc, eflags);
  else
    standard_ExecutorStart(queryDesc, eflags);
}

static void pg_diffix_ExecutorRun(
    QueryDesc *queryDesc,
    ScanDirection direction,
    uint64 count,
    bool execute_once)
{
  if (prev_ExecutorRun_hook)
    prev_ExecutorRun_hook(queryDesc, direction, count, execute_once);
  else
    standard_ExecutorRun(queryDesc, direction, count, execute_once);
}

static void pg_diffix_ExecutorFinish(QueryDesc *queryDesc)
{
  if (prev_ExecutorFinish_hook)
    prev_ExecutorFinish_hook(queryDesc);
  else
    standard_ExecutorFinish(queryDesc);
}

static void pg_diffix_ExecutorEnd(QueryDesc *queryDesc)
{
  if (prev_ExecutorEnd_hook)
    prev_ExecutorEnd_hook(queryDesc);
  else
    standard_ExecutorEnd(queryDesc);
}

void hooks_init(void)
{
  prev_planner_hook = planner_hook;
  planner_hook = pg_diffix_planner;

  prev_ExecutorStart_hook = ExecutorStart_hook;
  ExecutorStart_hook = pg_diffix_ExecutorStart;

  prev_ExecutorRun_hook = ExecutorRun_hook;
  ExecutorRun_hook = pg_diffix_ExecutorRun;

  prev_ExecutorFinish_hook = ExecutorFinish_hook;
  ExecutorFinish_hook = pg_diffix_ExecutorFinish;

  prev_ExecutorEnd_hook = ExecutorEnd_hook;
  ExecutorEnd_hook = pg_diffix_ExecutorEnd;
}

void hooks_cleanup(void)
{
  planner_hook = prev_planner_hook;
  ExecutorStart_hook = prev_ExecutorStart_hook;
  ExecutorRun_hook = prev_ExecutorRun_hook;
  ExecutorFinish_hook = prev_ExecutorFinish_hook;
  ExecutorEnd_hook = prev_ExecutorEnd_hook;
}
