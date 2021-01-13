#include "postgres.h"

/* Current user and role checking */
#include "miscadmin.h"
#include "utils/acl.h"

#include "pg_opendiffix/hooks.h"
#include "pg_opendiffix/utils.h"
#include "pg_opendiffix/validation.h"

post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
planner_hook_type prev_planner_hook = NULL;
ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;
ExecutorFinish_hook_type prev_ExecutorFinish_hook = NULL;
ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

/* --- Useful functions ---
 * GetUserId()
 * GetSessionUserId()
 * get_role_oid(...)
 * is_member_of_role(...)
 */

void pg_opendiffix_post_parse_analyze(ParseState *pstate, Query *query)
{
  static uint64 next_query_id = 1;
  uint64 query_id;

  /* If it's a non-sensitive query we let it pass through. */
  if (!requires_anonymization(query))
  {
    /* We make sure to call existing hooks for consistency. */
    if (prev_post_parse_analyze_hook)
    {
      prev_post_parse_analyze_hook(pstate, query);
    }

    return;
  }

  /* At this point we have a sensitive query. */

  /* Stops execution and raises an error if anon requirements are not met. */
  verify_anonymization_requirements(query);

  /* We give queries a unique ID to track their identity across hooks. */
  query_id = next_query_id++;
  query->queryId = query_id;

  if (prev_post_parse_analyze_hook)
  {
    prev_post_parse_analyze_hook(pstate, query);

    /* A hook may have changed the ID. */
    if (query->queryId != query_id)
    {
      LOG_DEBUG("Query ID was changed by another extension.");
      query_id = query->queryId;
    }
  }

  LOG_DEBUG("User ID %u", GetUserId());
  LOG_DEBUG("pg_opendiffix_post_parse_analyze (Query ID: %lu)", query->queryId);
}

PlannedStmt *
pg_opendiffix_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams)
{
  PlannedStmt *plan;

  LOG_DEBUG("pg_opendiffix_planner (Query ID: %lu)", parse->queryId);
  DUMP_NODE("Parse tree", parse);

  if (prev_planner_hook)
  {
    plan = prev_planner_hook(parse, query_string, cursorOptions, boundParams);
  }
  else
  {
    plan = standard_planner(parse, query_string, cursorOptions, boundParams);
  }

  /* DUMP_NODE("Query plan", plan); */
  return plan;
}

void pg_opendiffix_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
  LOG_DEBUG("pg_opendiffix_ExecutorStart (Query ID: %lu)", queryDesc->plannedstmt->queryId);

  if (prev_ExecutorStart_hook)
  {
    prev_ExecutorStart_hook(queryDesc, eflags);
  }
  else
  {
    standard_ExecutorStart(queryDesc, eflags);
  }
}

void pg_opendiffix_ExecutorRun(
    QueryDesc *queryDesc,
    ScanDirection direction,
    uint64 count,
    bool execute_once)
{
  LOG_DEBUG("pg_opendiffix_ExecutorRun (Query ID: %lu)", queryDesc->plannedstmt->queryId);

  if (prev_ExecutorRun_hook)
  {
    prev_ExecutorRun_hook(queryDesc, direction, count, execute_once);
  }
  else
  {
    standard_ExecutorRun(queryDesc, direction, count, execute_once);
  }
}

void pg_opendiffix_ExecutorFinish(QueryDesc *queryDesc)
{
  LOG_DEBUG("pg_opendiffix_ExecutorFinish (Query ID: %lu)", queryDesc->plannedstmt->queryId);

  if (prev_ExecutorFinish_hook)
  {
    prev_ExecutorFinish_hook(queryDesc);
  }
  else
  {
    standard_ExecutorFinish(queryDesc);
  }
}

void pg_opendiffix_ExecutorEnd(QueryDesc *queryDesc)
{
  LOG_DEBUG("pg_opendiffix_ExecutorEnd (Query ID: %lu)", queryDesc->plannedstmt->queryId);

  if (prev_ExecutorEnd_hook)
  {
    prev_ExecutorEnd_hook(queryDesc);
  }
  else
  {
    standard_ExecutorEnd(queryDesc);
  }
}
