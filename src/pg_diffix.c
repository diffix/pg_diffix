#include "postgres.h"

#include "fmgr.h"

#include "pg_diffix/aggregation/bucket_scan.h"
#include "pg_diffix/auth.h"
#include "pg_diffix/config.h"
#include "pg_diffix/hooks.h"
#include "pg_diffix/oid_cache.h"
#include "pg_diffix/query/allowed_objects.h"
#include "pg_diffix/utils.h"

#include <limits.h>

#ifdef _WIN64
#define __WORDSIZE 64
#endif

#if __WORDSIZE != 64
#error "This module requires a 64-bit target architecture!"
#endif

PG_MODULE_MAGIC;

PGDLLEXPORT void _PG_init(void);
PGDLLEXPORT void _PG_fini(void);

void _PG_init(void)
{
  DEBUG_LOG("Initializing Diffix extension...");

  auth_init();
  config_init();
  config_validate();
  register_bucket_scan_nodes();
  hooks_init();
}

void _PG_fini(void)
{
  DEBUG_LOG("Deactivating Diffix extension...");

  oid_cache_cleanup();
  hooks_cleanup();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(placeholder_func);

Datum placeholder_func(PG_FUNCTION_ARGS)
{
  return PG_GETARG_DATUM(0);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(internal_qual_wrapper);

Datum internal_qual_wrapper(PG_FUNCTION_ARGS)
{
  return PG_GETARG_DATUM(0);
}
