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
  g_oid_cache.sum_int2 = lookup_function(NULL, "sum", 1, (Oid[]){INT2OID});
  g_oid_cache.sum_int4 = lookup_function(NULL, "sum", 1, (Oid[]){INT4OID});
  g_oid_cache.sum_int8 = lookup_function(NULL, "sum", 1, (Oid[]){INT8OID});
  g_oid_cache.sum_numeric = lookup_function(NULL, "sum", 1, (Oid[]){NUMERICOID});
  g_oid_cache.sum_float4 = lookup_function(NULL, "sum", 1, (Oid[]){FLOAT4OID});
  g_oid_cache.sum_float8 = lookup_function(NULL, "sum", 1, (Oid[]){FLOAT8OID});
  g_oid_cache.avg_int2 = lookup_function(NULL, "avg", 1, (Oid[]){INT2OID});
  g_oid_cache.avg_int4 = lookup_function(NULL, "avg", 1, (Oid[]){INT4OID});
  g_oid_cache.avg_int8 = lookup_function(NULL, "avg", 1, (Oid[]){INT8OID});
  g_oid_cache.avg_numeric = lookup_function(NULL, "avg", 1, (Oid[]){NUMERICOID});
  g_oid_cache.avg_float4 = lookup_function(NULL, "avg", 1, (Oid[]){FLOAT4OID});
  g_oid_cache.avg_float8 = lookup_function(NULL, "avg", 1, (Oid[]){FLOAT8OID});
  g_oid_cache.count_histogram = lookup_function("diffix", "count_histogram", 1, (Oid[]){ANYOID});
  g_oid_cache.count_histogram_int8 = lookup_function("diffix", "count_histogram", 2, (Oid[]){ANYOID, INT8OID});

  g_oid_cache.count_star_noise = lookup_function("diffix", "count_noise", 0, (Oid[]){});
  g_oid_cache.count_value_noise = lookup_function("diffix", "count_noise", 1, (Oid[]){ANYOID});
  g_oid_cache.sum_noise = lookup_function("diffix", "sum_noise", 1, (Oid[]){ANYOID});
  g_oid_cache.avg_noise = lookup_function("diffix", "avg_noise", 1, (Oid[]){ANYOID});

  g_oid_cache.low_count = lookup_function("diffix", "low_count", -1, (Oid[]){});
  g_oid_cache.anon_count_distinct = lookup_function("diffix", "anon_count_distinct", -1, (Oid[]){});
  g_oid_cache.anon_count_star = lookup_function("diffix", "anon_count_star", -1, (Oid[]){});
  g_oid_cache.anon_count_value = lookup_function("diffix", "anon_count_value", -1, (Oid[]){});
  g_oid_cache.anon_sum = lookup_function("diffix", "anon_sum", -1, (Oid[]){});
  g_oid_cache.anon_count_histogram = lookup_function("diffix", "anon_count_histogram", -1, (Oid[]){});

  g_oid_cache.anon_count_distinct_noise = lookup_function("diffix", "anon_count_distinct_noise", -1, (Oid[]){});
  g_oid_cache.anon_count_star_noise = lookup_function("diffix", "anon_count_star_noise", -1, (Oid[]){});
  g_oid_cache.anon_count_value_noise = lookup_function("diffix", "anon_count_value_noise", -1, (Oid[]){});
  g_oid_cache.anon_sum_noise = lookup_function("diffix", "anon_sum_noise", -1, (Oid[]){});

  g_oid_cache.anon_agg_state = get_func_rettype(g_oid_cache.anon_count_star);

  g_oid_cache.is_suppress_bin = lookup_function("diffix", "is_suppress_bin", 0, (Oid[]){});

  g_oid_cache.round_by_nn = lookup_function("diffix", "round_by", 2, (Oid[]){NUMERICOID, NUMERICOID});
  g_oid_cache.round_by_dd = lookup_function("diffix", "round_by", 2, (Oid[]){FLOAT8OID, FLOAT8OID});
  g_oid_cache.ceil_by_nn = lookup_function("diffix", "ceil_by", 2, (Oid[]){NUMERICOID, NUMERICOID});
  g_oid_cache.ceil_by_dd = lookup_function("diffix", "ceil_by", 2, (Oid[]){FLOAT8OID, FLOAT8OID});
  g_oid_cache.floor_by_nn = lookup_function("diffix", "floor_by", 2, (Oid[]){NUMERICOID, NUMERICOID});
  g_oid_cache.floor_by_dd = lookup_function("diffix", "floor_by", 2, (Oid[]){FLOAT8OID, FLOAT8OID});

  g_oid_cache.internal_qual_wrapper = lookup_function("diffix", "internal_qual_wrapper", 1, (Oid[]){BOOLOID});

  g_loaded = true;
}

bool is_sum_oid(Oid aggoid)
{
  return aggoid == g_oid_cache.sum_int2 || aggoid == g_oid_cache.sum_int4 || aggoid == g_oid_cache.sum_int8 ||
         aggoid == g_oid_cache.sum_numeric || aggoid == g_oid_cache.sum_float4 || aggoid == g_oid_cache.sum_float8;
}

bool is_avg_oid(Oid aggoid)
{
  return aggoid == g_oid_cache.avg_int2 || aggoid == g_oid_cache.avg_int4 || aggoid == g_oid_cache.avg_int8 ||
         aggoid == g_oid_cache.avg_numeric || aggoid == g_oid_cache.avg_float4 || aggoid == g_oid_cache.avg_float8;
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
