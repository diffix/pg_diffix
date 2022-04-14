#include "postgres.h"

#include "catalog/pg_type.h"
#include "utils/builtins.h"

#include "pg_diffix/aggregation/aid.h"

static aid_t make_int4_aid(Datum datum)
{
  /* Cast to `uint64` for consistent hashing. */
  uint64 aid = DatumGetUInt32(datum);
  return hash_bytes(&aid, sizeof(aid));
}

static aid_t make_int8_aid(Datum datum)
{
  uint64 aid = DatumGetUInt64(datum);
  return hash_bytes(&aid, sizeof(aid));
}

static aid_t make_text_aid(Datum datum)
{
  char *str = TextDatumGetCString(datum);
  return hash_bytes((unsigned char *)str, strlen(str));
}

MapAidFunc get_aid_mapper(Oid aid_type)
{
  switch (aid_type)
  {
  case INT4OID:
    return make_int4_aid;
  case INT8OID:
    return make_int8_aid;
  case TEXTOID:
  case VARCHAROID:
    return make_text_aid;
  default:
    ereport(ERROR, (errmsg("Unsupported AID type (OID %u)", aid_type)));
    return NULL;
  }
}
