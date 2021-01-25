#include "postgres.h"
#include "fmgr.h"

#include "pg_diffix/utils.h"
#include "pg_diffix/hooks.h"
#include "pg_diffix/config.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

void _PG_init(void)
{
  static int activation_count = 1;
  char *config_string;

  DEBUG_LOG("Activating Diffix extension (%i)...", activation_count++);

  load_diffix_config();
  config_string = config_to_string(&Config);

  DEBUG_LOG("Config %s", config_string);
  pfree(config_string);

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

  free_diffix_config();

  post_parse_analyze_hook = prev_post_parse_analyze_hook;
  planner_hook = prev_planner_hook;
  ExecutorStart_hook = prev_ExecutorStart_hook;
  ExecutorRun_hook = prev_ExecutorRun_hook;
  ExecutorFinish_hook = prev_ExecutorFinish_hook;
  ExecutorEnd_hook = prev_ExecutorEnd_hook;
}
