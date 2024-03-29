#include "postgres.h"

#include "pg_diffix/aggregation/aid_tracker.h"
#include "pg_diffix/utils.h"

/*
 * Definitions for HashTable<aid_t, AidTrackerHashEntry>
 */
#define SH_PREFIX AidTracker
#define SH_ELEMENT_TYPE AidTrackerHashEntry
#define SH_KEY aid
#define SH_KEY_TYPE aid_t
#define SH_EQUAL(tb, a, b) (a == b)
#define SH_HASH_KEY(tb, key) (uint32) key /* `key` is already a hash */
#define SH_SCOPE inline
#define SH_DEFINE
#include "lib/simplehash.h"

void aid_tracker_init(AidTrackerState *state, MapAidFunc aid_mapper)
{
  state->aid_mapper = aid_mapper;
  state->aid_set = AidTracker_create(CurrentMemoryContext, 4, NULL);
  state->aid_seed = 0;
}

void aid_tracker_update(AidTrackerState *state, aid_t aid)
{
  bool found;
  AidTracker_insert(state->aid_set, aid, &found);
  if (!found)
    state->aid_seed ^= aid;
}

void aid_tracker_merge(AidTrackerState *dst_tracker, const AidTrackerState *src_tracker)
{
  AidTrackerHashEntry *entry;
  foreach_entry(entry, src_tracker->aid_set, AidTracker)
  {
    aid_tracker_update(dst_tracker, entry->aid);
  }
}
