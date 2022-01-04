#ifndef PG_DIFFIX_AID_H
#define PG_DIFFIX_AID_H

#include "c.h"
#include "common/hashfn.h"

#include <inttypes.h>

#define AID_FMT PRIu64

typedef uint64 aid_hash_t;

typedef aid_hash_t (*HashAidValueFunc)(Datum datum);

typedef struct AidDescriptor
{
  HashAidValueFunc hash_aid; /* Function which gets an AID from a Datum */
} AidDescriptor;

extern AidDescriptor get_aid_descriptor(Oid aid_type);

#define HASH_AID_32(aidv) hash_bytes((unsigned char *)&aidv, sizeof(aid_hash_t))
#define HASH_AID_64(aidv) hash_bytes_extended((unsigned char *)&aidv, sizeof(aid_hash_t), 0)

#endif /* PG_DIFFIX_AID_H */
