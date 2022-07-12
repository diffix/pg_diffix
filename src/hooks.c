#include "postgres.h"

#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/value.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "tcop/utility.h"
#include "utils/acl.h"

#include "pg_diffix/auth.h"
#include "pg_diffix/config.h"
#include "pg_diffix/hooks.h"
#include "pg_diffix/oid_cache.h"
#include "pg_diffix/query/allowed_objects.h"
#include "pg_diffix/query/anonymization.h"
#include "pg_diffix/query/relation.h"
#include "pg_diffix/query/validation.h"
#include "pg_diffix/utils.h"

post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
planner_hook_type prev_planner_hook = NULL;
ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;
ExecutorCheckPerms_hook_type prev_ExecutorCheckPerms_hook = NULL;
ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;
ExecutorFinish_hook_type prev_ExecutorFinish_hook = NULL;
ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

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

static AnonQueryLinks *prepare_query(Query *query, ParamListInfo bound_params)
{
  /* Do nothing for sessions with direct access. */
  if (get_session_access_level() == ACCESS_DIRECT)
    return NULL;

  List *personal_relations = gather_personal_relations(query);
  /* A query requires anonymization if it targets personal relations. */
  if (personal_relations == NIL)
    return NULL;

  /* At this point we have an anonymizing query. */
  DEBUG_LOG("Anonymizing query (Query ID=%lu) (User ID=%u) %s", query->queryId, GetSessionUserId(), nodeToString(query));

  /* We load OIDs lazily because experimentation shows that UDFs may return INVALIDOID (0) during _PG_init. */
  oid_cache_init();

  /*
   * Since we cannot easily validate cross-dependent parameters using GUC,
   * we verify those here and fail if they are misconfigured.
   */
  config_validate();

  AnonQueryLinks *links = compile_query(query, personal_relations, bound_params);

  DEBUG_LOG("Compiled query (Query ID=%lu) (User ID=%u) %s", query->queryId, GetSessionUserId(), nodeToString(query));

  return links;
}

static PlannedStmt *pg_diffix_planner(
    Query *query,
    const char *query_string,
    int cursorOptions,
    ParamListInfo boundParams)
{
  static uint64 next_query_id = 1;
  query->queryId = next_query_id++;

  DEBUG_LOG("Statement (Query ID=%lu) (User ID=%u): %s", query->queryId, GetSessionUserId(), query_string);

  AnonQueryLinks *links = prepare_query(query, boundParams);

  planner_hook_type planner = (prev_planner_hook ? prev_planner_hook : standard_planner);
  PlannedStmt *plan = planner(query, query_string, cursorOptions, boundParams);

  plan->planTree = rewrite_plan(plan->planTree, links);
  rewrite_plan_list(plan->subplans, links);

  return plan;
}

static DefElem *make_bool_option(char *name, bool value)
{
  Value *bool_string_value = value ? makeString("true") : makeString("false");
  DefElem *def_elem = makeNode(DefElem);

  def_elem->defnamespace = NULL;
  def_elem->defname = name;
  def_elem->arg = (Node *)bool_string_value;
  def_elem->defaction = DEFELEM_UNSPEC;
  def_elem->location = -1;
  return def_elem;
}

static void prepare_utility(PlannedStmt *pstmt)
{
  if (IsA(pstmt->utilityStmt, ExplainStmt))
  {
    if (get_session_access_level() == ACCESS_DIRECT)
      return;

    ExplainStmt *explain = (ExplainStmt *)pstmt->utilityStmt;
    if (involves_personal_relations((Query *)explain->query))
    {
      verify_explain_options(explain);
      /* Unless given a conflicting `COSTS true` (verified above), set `COSTS false`, which censors the `EXPLAIN`. */
      explain->options = lappend(explain->options, make_bool_option("costs", false));
    }
  }
}

#if PG_MAJORVERSION_NUM == 13
static void pg_diffix_ProcessUtility(PlannedStmt *pstmt,
                                     const char *queryString,
                                     ProcessUtilityContext context,
                                     ParamListInfo params,
                                     QueryEnvironment *queryEnv,
                                     DestReceiver *dest, QueryCompletion *qc)
{
  prepare_utility(pstmt);
  if (prev_ProcessUtility_hook)
    prev_ProcessUtility_hook(pstmt, queryString, context, params, queryEnv, dest, qc);
  else
    standard_ProcessUtility(pstmt, queryString, context, params, queryEnv, dest, qc);
}
#else
static void pg_diffix_ProcessUtility(PlannedStmt *pstmt,
                                     const char *queryString,
                                     bool readOnlyTree,
                                     ProcessUtilityContext context,
                                     ParamListInfo params,
                                     QueryEnvironment *queryEnv,
                                     DestReceiver *dest, QueryCompletion *qc)
{
  prepare_utility(pstmt);
  if (prev_ProcessUtility_hook)
    prev_ProcessUtility_hook(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
  else
    standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
}
#endif

static void pg_diffix_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
  if (prev_ExecutorStart_hook)
    prev_ExecutorStart_hook(queryDesc, eflags);
  else
    standard_ExecutorStart(queryDesc, eflags);
}

static bool pg_diffix_ExecutorCheckPerms(List *range_tables, bool should_abort)
{
  if (get_session_access_level() != ACCESS_DIRECT)
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

  prev_ProcessUtility_hook = ProcessUtility_hook;
  ProcessUtility_hook = pg_diffix_ProcessUtility;

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
  ProcessUtility_hook = prev_ProcessUtility_hook;
  ExecutorCheckPerms_hook = prev_ExecutorCheckPerms_hook;
  ExecutorStart_hook = prev_ExecutorStart_hook;
  ExecutorRun_hook = prev_ExecutorRun_hook;
  ExecutorFinish_hook = prev_ExecutorFinish_hook;
  ExecutorEnd_hook = prev_ExecutorEnd_hook;
}
