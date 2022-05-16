#include "postgres.h"

#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "parser/parse_func.h"
#include "utils/lsyscache.h"

#include "pg_diffix/oid_cache.h"
#include "pg_diffix/utils.h"

static Oid lookup_function(char *namespace, char *name, int num_args, Oid *arg_types);

Oids g_oid_cache;

static bool g_loaded = false;

void oid_cache_init(void)
{
  if (g_loaded)
    return;

  g_oid_cache.count_star = lookup_function(NULL, "count", 0, (Oid[]){});
  g_oid_cache.count_value = lookup_function(NULL, "count", 1, (Oid[]){ANYOID});

  g_oid_cache.low_count = lookup_function("diffix", "low_count", -1, (Oid[]){});
  g_oid_cache.anon_count_distinct = lookup_function("diffix", "anon_count_distinct", -1, (Oid[]){});
  g_oid_cache.anon_count_star = lookup_function("diffix", "anon_count_star", -1, (Oid[]){});
  g_oid_cache.anon_count_value = lookup_function("diffix", "anon_count_value", -1, (Oid[]){});

  g_oid_cache.anon_agg_state = get_func_rettype(g_oid_cache.anon_count_star);

  g_oid_cache.is_suppress_bin = lookup_function("diffix", "is_suppress_bin", 0, (Oid[]){});

  g_oid_cache.round_by_nn = lookup_function("diffix", "round_by", 2, (Oid[]){NUMERICOID, NUMERICOID});
  g_oid_cache.round_by_dd = lookup_function("diffix", "round_by", 2, (Oid[]){FLOAT8OID, FLOAT8OID});
  g_oid_cache.ceil_by_nn = lookup_function("diffix", "ceil_by", 2, (Oid[]){NUMERICOID, NUMERICOID});
  g_oid_cache.ceil_by_dd = lookup_function("diffix", "ceil_by", 2, (Oid[]){FLOAT8OID, FLOAT8OID});
  g_oid_cache.floor_by_nn = lookup_function("diffix", "floor_by", 2, (Oid[]){NUMERICOID, NUMERICOID});
  g_oid_cache.floor_by_dd = lookup_function("diffix", "floor_by", 2, (Oid[]){FLOAT8OID, FLOAT8OID});

  g_oid_cache.internal_qual_wrapper = lookup_function("diffix", "internal_qual_wrapper", 1, (Oid[]){BOOLOID});

  DEBUG_LOG("OidCache %s", oids_to_string(&g_oid_cache));

  g_loaded = true;
}

static Oid lookup_function(char *namespace, char *name, int num_args, Oid *arg_types)
{
  Oid oid;

  List *func_name = (namespace != NULL) ? list_make1(makeString(namespace)) : NIL;
  func_name = lappend(func_name, makeString(name));

  oid = LookupFuncName(func_name, num_args, arg_types, false);

  list_free_deep(func_name);

  return oid;
}

void oid_cache_cleanup()
{
  g_loaded = false;
  /* If we'd have any dynamic allocation here would be the place to free it. */
}

char *oids_to_string(Oids *oids)
{
  StringInfoData string;

  initStringInfo(&string);

  appendStringInfo(&string, "{OID_CACHE");

  appendStringInfo(&string, " :count_star %u", oids->count_star);
  appendStringInfo(&string, " :count_value %u", oids->count_value);

  appendStringInfo(&string, " :low_count %u", oids->low_count);
  appendStringInfo(&string, " :anon_count_distinct %u", oids->anon_count_distinct);
  appendStringInfo(&string, " :anon_count_star %u", oids->anon_count_star);
  appendStringInfo(&string, " :anon_count_value %u", oids->anon_count_value);

  appendStringInfo(&string, " :anon_agg_state %u", oids->anon_agg_state);

  appendStringInfo(&string, " :is_suppress_bin %u", oids->is_suppress_bin);

  appendStringInfo(&string, " :round_by (nn %u) (dd %u)", oids->round_by_nn, oids->round_by_dd);
  appendStringInfo(&string, " :ceil_by (nn %u) (dd %u)", oids->ceil_by_nn, oids->ceil_by_dd);
  appendStringInfo(&string, " :floor_by (nn %u) (dd %u)", oids->floor_by_nn, oids->floor_by_dd);

  appendStringInfo(&string, " :internal_qual_wrapper (nn %u) (dd %u)", oids->floor_by_nn, oids->floor_by_dd);

  appendStringInfo(&string, "}");

  return string.data;
}
