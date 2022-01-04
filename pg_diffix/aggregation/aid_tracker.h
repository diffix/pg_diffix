#ifndef PG_DIFFIX_AID_TRACKER_H
#define PG_DIFFIX_AID_TRACKER_H

#include "c.h"
#include "fmgr.h"
#include "nodes/pg_list.h"

#include "pg_diffix/aggregation/aid.h"

typedef struct AidTrackerHashEntry
{
  aid_hash_t aid_hash;   /* Entry key */
  char status; /* Required for hash table */
} AidTrackerHashEntry;

/*
 * Declarations for HashTable<aid_hash_t, AidTrackerHashEntry>
 */
#define SH_PREFIX AidTracker
#define SH_ELEMENT_TYPE AidTrackerHashEntry
#define SH_KEY_TYPE aid_hash_t
#define SH_SCOPE extern
#define SH_DECLARE
#include "lib/simplehash.h"

typedef struct AidTrackerState
{
  AidDescriptor aid_descriptor; /* Behavior for AIDs */
  AidTracker_hash *aid_set;     /* Hash set of all AIDs */
  uint64 aid_seed;              /* Current AID seed */
} AidTrackerState;

/*
 * Updates state with an AID.
 */
extern void aid_tracker_update(AidTrackerState *state, aid_hash_t aid_hash);

/*
 * Gets or creates the multi-AID aggregation state from the function arguments.
 */
extern List *get_aggregate_aid_trackers(PG_FUNCTION_ARGS, int aids_offset);

#endif /* PG_DIFFIX_AID_TRACKER_H */
