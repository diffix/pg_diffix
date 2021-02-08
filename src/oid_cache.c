#include "postgres.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "parser/parse_func.h"

#include "pg_diffix/oid_cache.h"
#include "pg_diffix/utils.h"

static Oid lookup_aggregate(char *name, int num_args, Oid *arg_types);
static void append_default_oids(StringInfo string, DefaultAggregateOids *oids);
static void append_diffix_oids(StringInfo string, DiffixAggregateOids *oids);

AggregateOids OidCache;

void load_oid_cache(void)
{
  if (OidCache.loaded)
  {
    return;
  }

  /* Postgres */
  OidCache.postgres.count = lookup_aggregate("count", 0, (Oid[]){});
  OidCache.postgres.count_any = lookup_aggregate("count", 1, (Oid[]){ANYOID});

  /* AID: int4 */
  OidCache.aid_int4.diffix_count = lookup_aggregate("diffix_count", 1, (Oid[]){INT4OID});
  OidCache.aid_int4.diffix_count_any = lookup_aggregate("diffix_count", 2, (Oid[]){INT4OID, ANYELEMENTOID});
  OidCache.aid_int4.diffix_lcf = lookup_aggregate("diffix_lcf", 1, (Oid[]){INT4OID});

  /* AID: text */
  OidCache.aid_text.diffix_count = lookup_aggregate("diffix_count", 1, (Oid[]){TEXTOID});
  OidCache.aid_text.diffix_count_any = lookup_aggregate("diffix_count", 2, (Oid[]){TEXTOID, ANYELEMENTOID});
  OidCache.aid_text.diffix_lcf = lookup_aggregate("diffix_lcf", 1, (Oid[]){TEXTOID});

  OidCache.loaded = true;
}

static Oid lookup_aggregate(char *name, int num_args, Oid *arg_types)
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

char *oids_to_string(AggregateOids *oids)
{
  StringInfoData string;

  initStringInfo(&string);

  /* begin oids */
  appendStringInfo(&string, "{OID_CACHE");

  appendStringInfo(&string, " :postgres ");
  append_default_oids(&string, &oids->postgres);

  appendStringInfo(&string, " :aid_int4 ");
  append_diffix_oids(&string, &oids->aid_int4);

  appendStringInfo(&string, " :aid_text ");
  append_diffix_oids(&string, &oids->aid_text);

  appendStringInfo(&string, "}");
  /* end oids */

  return string.data;
}

static void append_default_oids(StringInfo string, DefaultAggregateOids *oids)
{
  appendStringInfo(string, "{DEFAULT_AGGREGATE_OIDS");
  appendStringInfo(string, " :count %u", oids->count);
  appendStringInfo(string, " :count_any %u", oids->count_any);
  appendStringInfo(string, "}");
}

static void append_diffix_oids(StringInfo string, DiffixAggregateOids *oids)
{
  appendStringInfo(string, "{DIFFIX_AGGREGATE_OIDS");
  appendStringInfo(string, " :diffix_count %u", oids->diffix_count);
  appendStringInfo(string, " :diffix_count_any %u", oids->diffix_count_any);
  appendStringInfo(string, " :diffix_lcf %u", oids->diffix_lcf);
  appendStringInfo(string, "}");
}
