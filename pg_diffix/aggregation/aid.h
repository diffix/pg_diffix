#ifndef PG_DIFFIX_AID_H
#define PG_DIFFIX_AID_H

#include "pg_diffix/utils.h"

typedef hash_t aid_t;

typedef aid_t (*MakeAidFunc)(Datum datum);

extern MakeAidFunc get_aid_maker(Oid aid_type);

#endif /* PG_DIFFIX_AID_H */
