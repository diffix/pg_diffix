#ifndef PG_DIFFIX_AID_H
#define PG_DIFFIX_AID_H

#include "pg_diffix/utils.h"

typedef hash_t aid_t;

typedef aid_t (*MakeAidFunc)(Datum datum);

typedef struct AidDescriptor
{
  MakeAidFunc make_aid; /* Maps Datum to an AID value */
} AidDescriptor;

extern AidDescriptor get_aid_descriptor(Oid aid_type);

#endif /* PG_DIFFIX_AID_H */
