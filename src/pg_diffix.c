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
  hooks_init();
}

void _PG_fini(void)
{
  DEBUG_LOG("Deactivating Diffix extension...");

  oid_cache_cleanup();
  hooks_cleanup();
}
