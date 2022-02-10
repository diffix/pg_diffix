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
  AidTrackerState *state = (AidTrackerState *)palloc0(sizeof(AidTrackerState));

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

static const int STATE_INDEX = 0;

List *get_aggregate_aid_trackers(PG_FUNCTION_ARGS, int aids_offset)
{
  if (!PG_ARGISNULL(STATE_INDEX))
    return (List *)PG_GETARG_POINTER(STATE_INDEX);

  Assert(PG_NARGS() > aids_offset);

  /* We want all memory allocations to be done per aggregation node. */
  MemoryContext old_context = switch_to_aggregation_context(fcinfo);

  List *trackers = NIL;
  for (int arg_index = aids_offset; arg_index < PG_NARGS(); arg_index++)
  {
    Oid aid_type = get_fn_expr_argtype(fcinfo->flinfo, arg_index);
    AidTrackerState *tracker = aid_tracker_new(get_aid_descriptor(aid_type));
    trackers = lappend(trackers, tracker);
  }

  MemoryContextSwitchTo(old_context);
  return trackers;
}
