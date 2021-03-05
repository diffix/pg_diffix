#include "postgres.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "parser/parse_func.h"

#include "pg_diffix/utils.h"
#include "pg_diffix/query/oid_cache.h"

static Oid lookup_function(char *name, int num_args, Oid *arg_types);

Oids OidCache;

void load_oid_cache(void)
{
  OidCache.count = lookup_function("count", 0, (Oid[]){});
  OidCache.count_any = lookup_function("count", 1, (Oid[]){ANYOID});
  OidCache.diffix_lcf = lookup_function("diffix_lcf", 1, (Oid[]){ANYELEMENTOID});
  OidCache.diffix_count_distinct = lookup_function("diffix_count_distinct", 1, (Oid[]){ANYELEMENTOID});
  OidCache.diffix_count = lookup_function("diffix_count", 1, (Oid[]){ANYELEMENTOID});
  OidCache.diffix_count_any = lookup_function("diffix_count", 2, (Oid[]){ANYELEMENTOID, ANYOID});
  OidCache.generate_series = lookup_function("generate_series", 2, (Oid[]){INT8OID, INT8OID});

  OidCache.loaded = true;
}

static Oid lookup_function(char *name, int num_args, Oid *arg_types)
{
  Oid oid;
  Value *name_value = makeString(name);
  List *func_name = list_make1(name_value);

  oid = LookupFuncName(func_name, num_args, arg_types, false);

  list_free(func_name);
  pfree(name_value);

  return oid;
}

void free_oid_cache()
{
  OidCache.loaded = false;
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
