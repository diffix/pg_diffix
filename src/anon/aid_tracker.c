#include "postgres.h"
#include "common/hashfn.h"
#include "utils/elog.h"

#include "pg_diffix/anon/aid_tracker.h"

/*
 * Definitions for HashTable<aid_t, AidTrackerHashEntry>
 */
#define SH_PREFIX AidTracker
#define SH_ELEMENT_TYPE AidTrackerHashEntry
#define SH_KEY aid
#define SH_KEY_TYPE aid_t
#define SH_EQUAL(tb, a, b) a == b
#define SH_HASH_KEY(tb, key) HASH_AID_32(key)
#define SH_SCOPE inline
#define SH_DEFINE
#include "lib/simplehash.h"

AidTrackerState *aid_tracker_new(
    MemoryContext context,
    AidDescriptor aid_descriptor,
    uint64 initial_seed)
{
  AidTrackerState *state = (AidTrackerState *)
      MemoryContextAlloc(context, sizeof(AidTrackerState));

  state->aid_descriptor = aid_descriptor;
  state->aid_set = AidTracker_create(context, 128, NULL);
  state->aid_seed = initial_seed;

  return state;
}

void aid_tracker_update(AidTrackerState *state, aid_t aid)
{
  bool found;
  AidTracker_insert(state->aid_set, aid, &found);
  if (!found)
  {
    state->aid_seed ^= state->aid_descriptor.is_hash ? aid : HASH_AID_64(aid);
  }
}

#define STATE_INDEX 0
#define AID_INDEX 1

AidTrackerState *get_aggregate_aid_tracker(PG_FUNCTION_ARGS)
{
  if (!PG_ARGISNULL(STATE_INDEX))
  {
    return (AidTrackerState *)PG_GETARG_POINTER(STATE_INDEX);
  }

  MemoryContext agg_context;
  if (AggCheckCallContext(fcinfo, &agg_context) != AGG_CONTEXT_AGGREGATE)
  {
    ereport(ERROR, (errmsg("Aggregate called in non-aggregate context")));
  }

  Oid aid_type = get_fn_expr_argtype(fcinfo->flinfo, AID_INDEX);
  return aid_tracker_new(agg_context, get_aid_descriptor(aid_type), 0);
}
