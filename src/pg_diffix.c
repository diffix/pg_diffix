#include "postgres.h"
#include "fmgr.h"

#include "pg_diffix/auth.h"
#include "pg_diffix/config.h"
#include "pg_diffix/hooks.h"
#include "pg_diffix/oid_cache.h"
#include "pg_diffix/query/allowed_functions.h"
#include "pg_diffix/utils.h"

#include <limits.h>
#if __WORDSIZE != 64
#error "This module requires a 64-bit target architecture!"
#endif

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

void _PG_init(void)
{
  DEBUG_LOG("Activating Diffix extension...");

  auth_init();
  config_init();
  hooks_init();
}

void _PG_fini(void)
{
  DEBUG_LOG("Deactivating Diffix extension...");

  allowed_functions_cleanup();
  oid_cache_cleanup();
  hooks_cleanup();
}
