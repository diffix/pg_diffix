#ifndef PG_DIFFIX_AID_H
#define PG_DIFFIX_AID_H

#include "postgres.h"
#include "common/hashfn.h"

typedef uint64 aid_t;

typedef aid_t (*MakeAidFunc)(Datum datum);

typedef struct AidSetup
{
  MakeAidFunc make_aid; /* Function which gets an AID from a Datum */
  bool aid_is_hash;     /* Whether the created AIDs are already hashed.
                         * This is set to true for TEXT AIDs,
                         * meaning we don't need to hash again. */
} AidSetup;

extern AidSetup setup_aid(Oid aid_type);

#define HASH_AID_32(aid) hash_bytes((unsigned char *)&aid, sizeof(aid_t))
#define HASH_AID_64(aid) hash_bytes_extended((unsigned char *)&aid, sizeof(aid_t), 0)

#endif /* PG_DIFFIX_AID_H */
