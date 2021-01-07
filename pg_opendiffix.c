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
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;

static PlannedStmt *pg_opendiffix_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams);
static planner_hook_type prev_planner_hook = NULL;

ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;
ExecutorFinish_hook_type prev_ExecutorFinish_hook = NULL;
ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

/* Definitions */

void _PG_init(void)
{
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
  LOG_DEBUG("Parsed a query %s", pstate->p_sourcetext);
}

static PlannedStmt *
pg_opendiffix_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams)
{
  LOG_DEBUG("pg_opendiffix_planner");
  return standard_planner(parse, query_string, cursorOptions, boundParams);
}
