#include "postgres.h"
#include "fmgr.h"

#include "pg_diffix/auth.h"
#include "pg_diffix/hooks.h"
#include "pg_diffix/config.h"
#include "pg_diffix/utils.h"
#include "pg_diffix/query/oid_cache.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

void _PG_init(void)
{
  static int activation_count = 1;
  DEBUG_LOG("Activating Diffix extension (%i)...", activation_count++);

  config_init();

  /*
   * Hooks
   */
  prev_post_parse_analyze_hook = post_parse_analyze_hook;
  post_parse_analyze_hook = pg_diffix_post_parse_analyze;

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

void _PG_fini(void)
{
  DEBUG_LOG("Deactivating Diffix extension...");

  free_oid_cache();

  post_parse_analyze_hook = prev_post_parse_analyze_hook;
  planner_hook = prev_planner_hook;
  ExecutorStart_hook = prev_ExecutorStart_hook;
  ExecutorRun_hook = prev_ExecutorRun_hook;
  ExecutorFinish_hook = prev_ExecutorFinish_hook;
  ExecutorEnd_hook = prev_ExecutorEnd_hook;
}
