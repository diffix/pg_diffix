#ifndef PG_DIFFIX_AID_H
#define PG_DIFFIX_AID_H

#include "common/hashfn.h"

#include <inttypes.h>

#define AID_FMT PRIu64

typedef uint64 aid_t;

typedef aid_t (*MakeAidFunc)(Datum datum);

typedef struct AidDescriptor
{
  MakeAidFunc make_aid; /* Function which gets an AID from a Datum */
} AidDescriptor;

extern AidDescriptor get_aid_descriptor(Oid aid_type);

#define HASH_AID_32(aid) hash_bytes((unsigned char *)&aid, sizeof(aid_t))
#define HASH_AID_64(aid) hash_bytes_extended((unsigned char *)&aid, sizeof(aid_t), 0)

#endif /* PG_DIFFIX_AID_H */
