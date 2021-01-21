#include "postgres.h"
#include "fmgr.h"

#include "pg_opendiffix/utils.h"
#include "pg_opendiffix/hooks.h"
#include "pg_opendiffix/config.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

void _PG_init(void)
{
  static int activation_count = 1;

  OpenDiffixConfig *config;
  char *config_string;

  DEBUG_LOG("Activating OpenDiffix extension (%i)...", activation_count++);

  config = load_opendiffix_config();
  config_string = config_to_string(config);

  DEBUG_LOG("Config %s", config_string);
  pfree(config_string);

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

  free_opendiffix_config();

  post_parse_analyze_hook = prev_post_parse_analyze_hook;
  planner_hook = prev_planner_hook;
  ExecutorStart_hook = prev_ExecutorStart_hook;
  ExecutorRun_hook = prev_ExecutorRun_hook;
  ExecutorFinish_hook = prev_ExecutorFinish_hook;
  ExecutorEnd_hook = prev_ExecutorEnd_hook;
}
