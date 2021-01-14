#include "postgres.h"

/* Function manager. */
#include "fmgr.h"

#include "pg_opendiffix/utils.h"
#include "pg_opendiffix/hooks.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

void _PG_init(void)
{
  static int activation_count = 1;

  DEBUG_LOG("Activating OpenDiffix extension (%i)...", activation_count++);

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
  DEBUG_LOG("Deactivating OpenDiffix extension...");

  post_parse_analyze_hook = prev_post_parse_analyze_hook;
  planner_hook = prev_planner_hook;
  ExecutorStart_hook = prev_ExecutorStart_hook;
  ExecutorRun_hook = prev_ExecutorRun_hook;
  ExecutorFinish_hook = prev_ExecutorFinish_hook;
  ExecutorEnd_hook = prev_ExecutorEnd_hook;
}
