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
#define SH_EQUAL(tb, a, b) a == b
#define SH_HASH_KEY(tb, key) (uint32) key /* `key` is already a hash */
#define SH_SCOPE inline
#define SH_DEFINE
#include "lib/simplehash.h"

static AidTrackerState *aid_tracker_new(AidDescriptor aid_descriptor)
{
  AidTrackerState *state = palloc0(sizeof(AidTrackerState));

  state->aid_descriptor = aid_descriptor;
  state->aid_set = AidTracker_create(CurrentMemoryContext, 128, NULL);
  state->aid_seed = 0;

  return state;
}

void aid_tracker_update(AidTrackerState *state, aid_t aid)
{
  bool found;
  AidTracker_insert(state->aid_set, aid, &found);
  if (!found)
  {
    state->aid_seed ^= aid;
  }
}

List *create_aid_trackers(ArgsDescriptor *args_desc, int aids_offset)
{
  Assert(args_desc->num_args > aids_offset);

  List *trackers = NIL;
  for (int arg_index = aids_offset; arg_index < args_desc->num_args; arg_index++)
  {
    Oid aid_type = args_desc->args[arg_index].type_oid;
    AidTrackerState *tracker = aid_tracker_new(get_aid_descriptor(aid_type));
    trackers = lappend(trackers, tracker);
  }

  return trackers;
}
