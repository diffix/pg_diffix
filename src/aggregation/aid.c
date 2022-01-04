#include "postgres.h"
#include "catalog/pg_type.h"
#include "common/hashfn.h"
#include "utils/builtins.h"
#include "utils/elog.h"

#include "pg_diffix/aggregation/aid.h"

static aid_hash_t hash_int4_aid(Datum datum)
{
  aid_hash_t aid_hash = DatumGetUInt32(datum);
#ifndef DEBUG
  aid_hash = HASH_AID_64(aid_hash); /* We keep integer values untouched on DEBUG builds. */
#endif
  return aid_hash;
}

static aid_hash_t hash_int8_aid(Datum datum)
{
  aid_hash_t aid_hash = DatumGetUInt64(datum);
#ifndef DEBUG
  aid_hash = HASH_AID_64(aid_hash); /* We keep integer values untouched on DEBUG builds. */
#endif
  return aid_hash;
}

static aid_hash_t hash_text_aid(Datum datum)
{
  char *str = TextDatumGetCString(datum);
  return hash_bytes((unsigned char *)str, strlen(str));
}

AidDescriptor get_aid_descriptor(Oid aid_type)
{
  AidDescriptor descriptor = {0};

  switch (aid_type)
  {
  case INT4OID:
    descriptor.hash_aid = hash_int4_aid;
    break;
  case INT8OID:
    descriptor.hash_aid = hash_int8_aid;
    break;
  case TEXTOID:
  case VARCHAROID:
    descriptor.hash_aid = hash_text_aid;
    break;
  default:
    ereport(ERROR, (errmsg("Unsupported AID type (OID %u)", aid_type)));
    break;
  }

  return descriptor;
}
