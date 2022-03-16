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

AidDescriptor get_aid_descriptor(Oid aid_type)
{
  AidDescriptor descriptor = {0};

  switch (aid_type)
  {
  case INT4OID:
    descriptor.make_aid = make_int4_aid;
    break;
  case INT8OID:
    descriptor.make_aid = make_int8_aid;
    break;
  case TEXTOID:
  case VARCHAROID:
    descriptor.make_aid = make_text_aid;
    break;
  default:
    ereport(ERROR, (errmsg("Unsupported AID type (OID %u)", aid_type)));
    break;
  }

  return descriptor;
}
