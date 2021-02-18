#ifndef PG_DIFFIX_AID_TRACKER_H
#define PG_DIFFIX_AID_TRACKER_H

#include "postgres.h"
#include "fmgr.h"

#include "pg_diffix/aid.h"

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
  AidFunctions aid_functions; /* Behavior for AIDs */
  AidTracker_hash *aid_set;   /* Hash set of all AIDs */
  uint64 aid_seed;            /* Current AID seed */
} AidTrackerState;

/*
 * Creates a new empty state.
 */
extern AidTrackerState *aid_tracker_new(
    MemoryContext context,
    AidFunctions aid_functions,
    uint64 initial_seed);

/*
 * Updates state with an AID.
 */
extern void aid_tracker_update(AidTrackerState *state, aid_t aid);

/*
 * Gets or creates the aggregation state from the function arguments.
 */
extern AidTrackerState *get_aggregate_aid_tracker(PG_FUNCTION_ARGS);

#endif /* PG_DIFFIX_AID_TRACKER_H */
