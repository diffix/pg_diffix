#include "postgres.h"

#include "executor/executor.h"
#include "miscadmin.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "utils/acl.h"

#include "pg_diffix/auth.h"
#include "pg_diffix/config.h"
#include "pg_diffix/hooks.h"
#include "pg_diffix/oid_cache.h"
#include "pg_diffix/query/allowed_functions.h"
#include "pg_diffix/query/anonymization.h"
#include "pg_diffix/query/relation.h"
#include "pg_diffix/query/validation.h"
#include "pg_diffix/utils.h"

post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
planner_hook_type prev_planner_hook = NULL;
ExecutorCheckPerms_hook_type prev_ExecutorCheckPerms_hook = NULL;
ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;
ExecutorFinish_hook_type prev_ExecutorFinish_hook = NULL;
ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

/* Other means of learning this are troublesome at the planner/executor stages. */
static bool is_explain(const char *query_string)
{
  return strncasecmp(query_string, "explain ", 8) == 0;
}

#if PG_MAJORVERSION_NUM == 13
static void pg_diffix_post_parse_analyze(ParseState *pstate, Query *query)
{
  if (query->commandType == CMD_UTILITY)
    verify_utility_command(query->utilityStmt);

  if (prev_post_parse_analyze_hook)
    prev_post_parse_analyze_hook(pstate, query);
}
#elif PG_MAJORVERSION_NUM >= 14
static void pg_diffix_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate)
{
  if (query->commandType == CMD_UTILITY)
    verify_utility_command(query->utilityStmt);

  if (prev_post_parse_analyze_hook)
    prev_post_parse_analyze_hook(pstate, query, jstate);
}
#endif

static AnonQueryLinks *prepare_query(Query *query)
{
  static uint64 next_query_id = 1;
  query->queryId = next_query_id++;

  /* Do nothing for sessions with direct access. */
  if (get_session_access_level() == ACCESS_DIRECT)
  {
    DEBUG_LOG("Direct query (Query ID=%lu) (User ID=%u) %s", query->queryId, GetSessionUserId(), nodeToString(query));
    return NULL;
  }

  List *sensitive_relations = gather_sensitive_relations(query);

  /* A query requires anonymization if it targets sensitive relations. */
  if (sensitive_relations == NIL)
  {
    DEBUG_LOG("Non-anonymizing query (Query ID=%lu) (User ID=%u) %s", query->queryId, GetSessionUserId(), nodeToString(query));
    return NULL;
  }

  /* At this point we have an anonymizing query. */
  DEBUG_LOG("Anonymizing query (Query ID=%lu) (User ID=%u) %s", query->queryId, GetSessionUserId(), nodeToString(query));

  /* We load OIDs lazily because experimentation shows that UDFs may return INVALIDOID (0) during _PG_init. */
  oid_cache_init();

  AnonQueryLinks *links = compile_anonymizing_query(query, sensitive_relations);

  DEBUG_LOG("Rewritten query (Query ID=%lu) (User ID=%u) %s", query->queryId, GetSessionUserId(), nodeToString(query));

  return links;
}

static PlannedStmt *pg_diffix_planner(
    Query *parse,
    const char *query_string,
    int cursorOptions,
    ParamListInfo boundParams)
{
  DEBUG_LOG("STATEMENT: %s", query_string);
  AnonQueryLinks *links = prepare_query(parse);

  PlannedStmt *plan;
  if (prev_planner_hook)
    plan = prev_planner_hook(parse, query_string, cursorOptions, boundParams);
  else
    plan = standard_planner(parse, query_string, cursorOptions, boundParams);

  plan->planTree = rewrite_plan(plan->planTree, links);

  if (is_explain(query_string) && !superuser())
  {
    bool is_anonymizing = false;
    censor_plan_rows(plan->planTree, &is_anonymizing);
  }

  return plan;
}

static void pg_diffix_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
  if (prev_ExecutorStart_hook)
    prev_ExecutorStart_hook(queryDesc, eflags);
  else
    standard_ExecutorStart(queryDesc, eflags);
}

static bool pg_diffix_ExecutorCheckPerms(List *range_tables, bool should_abort)
{
  if (get_session_access_level() != ACCESS_DIRECT && !superuser())
  {
    if (!verify_pg_catalog_access(range_tables))
    {
      if (should_abort)
        aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_SCHEMA, "pg_catalog");
      else
        return false;
    }
  }

  if (prev_ExecutorCheckPerms_hook)
    return prev_ExecutorCheckPerms_hook(range_tables, should_abort);
  return true;
}

static void pg_diffix_ExecutorRun(
    QueryDesc *queryDesc,
    ScanDirection direction,
    uint64 count,
    bool execute_once)
{
  if (is_explain(queryDesc->sourceText) && !superuser())
  {
    bool is_anonymizing = false;
    censor_instrumentation(queryDesc->planstate, &is_anonymizing);
  }

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
  prev_post_parse_analyze_hook = post_parse_analyze_hook;
  post_parse_analyze_hook = pg_diffix_post_parse_analyze;

  prev_planner_hook = planner_hook;
  planner_hook = pg_diffix_planner;

  prev_ExecutorCheckPerms_hook = ExecutorCheckPerms_hook;
  ExecutorCheckPerms_hook = pg_diffix_ExecutorCheckPerms;

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
  post_parse_analyze_hook = prev_post_parse_analyze_hook;
  planner_hook = prev_planner_hook;
  ExecutorCheckPerms_hook = prev_ExecutorCheckPerms_hook;
  ExecutorStart_hook = prev_ExecutorStart_hook;
  ExecutorRun_hook = prev_ExecutorRun_hook;
  ExecutorFinish_hook = prev_ExecutorFinish_hook;
  ExecutorEnd_hook = prev_ExecutorEnd_hook;
}
