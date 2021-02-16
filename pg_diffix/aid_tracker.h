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
  MakeAidFunc make_aid;     /* Function to get the AID from a Datum */
  AidTracker_hash *aid_set; /* Hash set of all AIDs */
  uint64 aid_seed;          /* Current AID seed */
  bool hash_aids;           /* Should AIDs be hashed when combined in the seed? */
} AidTrackerState;

/*
 * Creates a new empty state.
 */
extern AidTrackerState *aid_tracker_new(
    MemoryContext context,
    MakeAidFunc make_aid,
    bool hash_aids,
    uint64 initial_seed);

/*
 * Updates state with a new AID.
 */
extern void aid_tracker_update(AidTrackerState *state, Datum aid_datum);

/*
 * Gets or creates the aggregation state from the argument at the given index.
 */
extern AidTrackerState *get_aggregate_aid_tracker(PG_FUNCTION_ARGS, int index);

#endif /* PG_DIFFIX_AID_TRACKER_H */
