#ifndef PG_DIFFIX_AID_TRACKER_H
#define PG_DIFFIX_AID_TRACKER_H

#include "pg_diffix/aggregation/aid.h"
#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/aggregation/noise.h"

typedef struct AidTrackerHashEntry
{
  aid_t aid;   /* Entry key */
  char status; /* Required for hash table */
} AidTrackerHashEntry;

/*
 * Declarations for HashTable<aid_t, AidTrackerHashEntry>
 */
#define SH_PREFIX AidTracker
#define SH_ELEMENT_TYPE AidTrackerHashEntry
#define SH_KEY_TYPE aid_t
#define SH_SCOPE extern
#define SH_DECLARE
#include "lib/simplehash.h"

typedef struct AidTrackerState
{
  MapAidFunc aid_mapper;    /* Mapper of AIDs from Datums */
  AidTracker_hash *aid_set; /* Hash set of all AIDs */
  seed_t aid_seed;          /* Current AID seed */
} AidTrackerState;

/*
 * Updates state with an AID.
 */
extern void aid_tracker_update(AidTrackerState *state, aid_t aid);

/*
 * Creates a new state for tracking AID values.
 */
extern AidTrackerState *aid_tracker_new(MapAidFunc aid_mapper);

#endif /* PG_DIFFIX_AID_TRACKER_H */
