#include "postgres.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "parser/parse_func.h"

#include "pg_diffix/utils.h"
#include "pg_diffix/query/oid_cache.h"

static Oid lookup_function(char *namespace, char *name, int num_args, Oid *arg_types);

Oids g_oid_cache;

void load_oid_cache(void)
{
  g_oid_cache.count = lookup_function(NULL, "count", 0, (Oid[]){});
  g_oid_cache.count_any = lookup_function(NULL, "count", 1, (Oid[]){ANYOID});

  g_oid_cache.diffix_lcf = lookup_function("diffix", "diffix_lcf", 1, (Oid[]){ANYELEMENTOID});
  g_oid_cache.diffix_count_distinct = lookup_function("diffix", "diffix_count_distinct", 1, (Oid[]){ANYELEMENTOID});
  g_oid_cache.diffix_count = lookup_function("diffix", "diffix_count", 1, (Oid[]){ANYELEMENTOID});
  g_oid_cache.diffix_count_any = lookup_function("diffix", "diffix_count", 2, (Oid[]){ANYELEMENTOID, ANYOID});

  g_oid_cache.generate_series = lookup_function(NULL, "generate_series", 2, (Oid[]){INT8OID, INT8OID});

  g_oid_cache.loaded = true;
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

void free_oid_cache()
{
  g_oid_cache.loaded = false;
  /* If we'd have any dynamic allocation here would be the place to free it. */
}

char *oids_to_string(Oids *oids)
{
  StringInfoData string;

  initStringInfo(&string);

  appendStringInfo(&string, "{OID_CACHE");

  appendStringInfo(&string, " :count %u", oids->count);
  appendStringInfo(&string, " :count_any %u", oids->count_any);
  appendStringInfo(&string, " :diffix_lcf %u", oids->diffix_lcf);
  appendStringInfo(&string, " :diffix_count_distinct %u", oids->diffix_count_distinct);
  appendStringInfo(&string, " :diffix_count %u", oids->diffix_count);
  appendStringInfo(&string, " :diffix_count_any %u", oids->diffix_count_any);

  appendStringInfo(&string, "}");

  return string.data;
}
