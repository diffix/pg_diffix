#include "postgres.h"

#include "pg_opendiffix/executor.h"
#include "pg_opendiffix/utils.h"

/* Function manager. */
#include "fmgr.h"

/* Post parse analyze hook */
#include "parser/analyze.h"

/* Planner hook */
#include "optimizer/planner.h"

/* PG extension setup */

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

/* Hooks */

static void pg_opendiffix_post_parse_analyze(ParseState *pstate, Query *query);
static PlannedStmt *pg_opendiffix_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams);

static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

/* Definitions */

static int activationCount = 1;

void _PG_init(void)
{
  LOG_DEBUG("Activating OpenDiffix extension (%i)...", activationCount++);

  prev_post_parse_analyze_hook = post_parse_analyze_hook;
  post_parse_analyze_hook = pg_opendiffix_post_parse_analyze;

  prev_planner_hook = planner_hook;
  planner_hook = pg_opendiffix_planner;

  prev_ExecutorStart_hook = ExecutorStart_hook;
  ExecutorStart_hook = pg_opendiffix_ExecutorStart;

  prev_ExecutorRun_hook = ExecutorRun_hook;
  ExecutorRun_hook = pg_opendiffix_ExecutorRun;

  prev_ExecutorFinish_hook = ExecutorFinish_hook;
  ExecutorFinish_hook = pg_opendiffix_ExecutorFinish;

  prev_ExecutorEnd_hook = ExecutorEnd_hook;
  ExecutorEnd_hook = pg_opendiffix_ExecutorEnd;
}

void _PG_fini(void)
{
  LOG_DEBUG("Deactivating OpenDiffix extension...");

  post_parse_analyze_hook = prev_post_parse_analyze_hook;
  planner_hook = prev_planner_hook;
  ExecutorStart_hook = prev_ExecutorStart_hook;
  ExecutorRun_hook = prev_ExecutorRun_hook;
  ExecutorFinish_hook = prev_ExecutorFinish_hook;
  ExecutorEnd_hook = prev_ExecutorEnd_hook;
}

static void
pg_opendiffix_post_parse_analyze(ParseState *pstate, Query *query)
{
}

static PlannedStmt *
pg_opendiffix_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams)
{
  PlannedStmt *plan;

  DUMP_NODE("Parse tree", parse);
  plan = standard_planner(parse, query_string, cursorOptions, boundParams);
  DUMP_NODE("Query plan", plan);
  return plan;
}
