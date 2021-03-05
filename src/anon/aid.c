#include "postgres.h"
#include "catalog/pg_type.h"
#include "common/hashfn.h"
#include "utils/builtins.h"
#include "utils/elog.h"

#include "pg_diffix/anon/aid.h"

static aid_t make_int4_aid(Datum datum)
{
  return DatumGetUInt32(datum);
}

static aid_t make_int8_aid(Datum datum)
{
  return DatumGetUInt64(datum);
}

static aid_t make_text_aid(Datum datum)
{
  char *str = text_to_cstring(DatumGetTextPP(datum));
  return hash_bytes((unsigned char *)str, strlen(str));
}

AidDescriptor get_aid_descriptor(Oid aid_type)
{
  AidDescriptor descriptor = {
      .make_aid = NULL,
      .is_hash = false,
  };

  switch (aid_type)
  {
  case INT4OID:
    descriptor.make_aid = make_int4_aid;
    break;
  case INT8OID:
    descriptor.make_aid = make_int8_aid;
    break;
  case TEXTOID:
    descriptor.make_aid = make_text_aid;
    descriptor.is_hash = true;
    break;
  default:
    ereport(ERROR, (errmsg("Unsupported AID type (OID %u)", aid_type)));
    break;
  }

  return descriptor;
}
