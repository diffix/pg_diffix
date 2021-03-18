#include "postgres.h"
#include "common/hashfn.h"
#include "utils/elog.h"

#include "pg_diffix/config.h"
#include "pg_diffix/aggregation/contribution_tracker.h"

/* ----------------------------------------------------------------
 * Top contributors management
 * ----------------------------------------------------------------
 */

static inline uint32 find_aid_index(
    ContributionTrackerState *state,
    uint32 top_length,
    aid_t aid,
    contribution_t old_contribution)
{
  for (uint32 i = state->contribution_descriptor.contribution_greater(
                      old_contribution,
                      state->top_contributors[top_length / 2].contribution)
                      ? 0
                      : (top_length / 2);
       i < top_length;
       i++)
  {
    if (aid == state->top_contributors[i].aid)
    {
      return i;
    }
  }

  return top_length;
}

static inline uint32 find_insertion_index(
    ContributionTrackerState *state,
    uint32 top_length,
    contribution_t contribution)
{
  ContributionGreaterFunc greater = state->contribution_descriptor.contribution_greater;

  /*
   * Do a single comparison in the middle to halve lookup steps.
   * No. elements won't be large enough to bother with a full binary search.
   */
  for (uint32 i = greater(contribution, state->top_contributors[top_length / 2].contribution)
                      ? 0
                      : (top_length / 2 + 1);
       i < top_length;
       i++)
  {
    if (greater(contribution, state->top_contributors[i].contribution))
    {
      return i;
    }
  }

  return top_length;
}

static void add_top_contributor(
    ContributionTrackerState *state,
    uint32 top_length,
    aid_t aid,
    contribution_t contribution)
{
  /*
  * Entry is not a top contributor if capacity is exhausted and
  * contribution is not greater than the lowest top contribution.
  */
  uint32 capacity = state->top_contributors_length;
  ContributionDescriptor *descriptor = &state->contribution_descriptor;
  if (top_length == capacity &&
      !descriptor->contribution_greater(contribution, state->top_contributors[top_length - 1].contribution))
    return;

  uint32 insertion_index = find_insertion_index(state, top_length, contribution);
  Assert(insertion_index < capacity); /* sanity check */

  /* Slide items to the right before inserting new item. */
  size_t elements = (top_length < capacity ? top_length + 1 : capacity) - insertion_index - 1;
  memmove(
      &state->top_contributors[insertion_index + 1],
      &state->top_contributors[insertion_index],
      elements * sizeof(TopContributor));

  state->top_contributors[insertion_index].aid = aid;
  state->top_contributors[insertion_index].contribution = contribution;
}

static void bump_or_add_top_contributor(
    ContributionTrackerState *state,
    uint32 top_length,
    aid_t aid,
    contribution_t old_contribution,
    contribution_t new_contribution)
{
  uint32 aid_index = find_aid_index(state, top_length, aid, old_contribution);
  if (aid_index == top_length)
  {
    /* Not an existing top contributor, try to add it as a new entry and return. */
    add_top_contributor(state, top_length, aid, new_contribution);
    return;
  }

  uint32 insertion_index = find_insertion_index(state, top_length, new_contribution);
  Assert(insertion_index <= aid_index); /* sanity check */

  size_t elements = aid_index - insertion_index;
  memmove(
      &state->top_contributors[insertion_index + 1],
      &state->top_contributors[insertion_index],
      elements * sizeof(TopContributor));

  state->top_contributors[insertion_index].aid = aid;
  state->top_contributors[insertion_index].contribution = new_contribution;
}

/* ----------------------------------------------------------------
 * Public functions
 * ----------------------------------------------------------------
 */

/*
 * Definitions for HashTable<aid_t, ContributionTrackerHashEntry>
 */
#define SH_PREFIX ContributionTracker
#define SH_ELEMENT_TYPE ContributionTrackerHashEntry
#define SH_KEY aid
#define SH_KEY_TYPE aid_t
#define SH_EQUAL(tb, a, b) a == b
#define SH_HASH_KEY(tb, key) HASH_AID_32(key)
#define SH_SCOPE inline
#define SH_DEFINE
#include "lib/simplehash.h"

ContributionTrackerState *contribution_tracker_new(
    MemoryContext context,
    AidDescriptor aid_descriptor,
    ContributionDescriptor contribution_descriptor,
    uint64 initial_seed,
    uint32 top_contributors_length)
{
  ContributionTrackerState *state = (ContributionTrackerState *)MemoryContextAlloc(
      context,
      sizeof(ContributionTrackerState) + top_contributors_length * sizeof(TopContributor));

  state->aid_descriptor = aid_descriptor;
  state->contribution_descriptor = contribution_descriptor;
  state->contribution_table = ContributionTracker_create(context, 128, NULL);
  state->contributions_count = 0;
  state->distinct_contributors = 0;
  state->overall_contribution = contribution_descriptor.contribution_initial;
  state->aid_seed = initial_seed;
  state->top_contributors_length = top_contributors_length;

  return state;
}

void contribution_tracker_update_aid(ContributionTrackerState *state, aid_t aid)
{
  bool found;
  ContributionTrackerHashEntry *entry = ContributionTracker_insert(state->contribution_table, aid, &found);
  if (!found)
  {
    state->aid_seed ^= state->aid_descriptor.is_hash ? aid : HASH_AID_64(aid);
    entry->has_contribution = false;
  }
}

void contribution_tracker_update_contribution(
    ContributionTrackerState *state,
    aid_t aid,
    contribution_t contribution)
{
  ContributionDescriptor *descriptor = &state->contribution_descriptor;
  uint32 top_length = Min(state->distinct_contributors, state->top_contributors_length);

  state->contributions_count++;
  state->overall_contribution = descriptor->contribution_combine(state->overall_contribution, contribution);

  bool found;
  ContributionTrackerHashEntry *entry = ContributionTracker_insert(state->contribution_table, aid, &found);
  if (!found)
  {
    /* AID does not exist in table. */
    entry->has_contribution = true;
    entry->contribution = contribution;
    state->distinct_contributors++;
    state->aid_seed ^= state->aid_descriptor.is_hash ? aid : HASH_AID_64(aid);

    add_top_contributor(state, top_length, aid, contribution);
    return;
  }
  else if (!entry->has_contribution)
  {
    /* AID exists but hasn't contributed yet. */
    entry->has_contribution = true;
    entry->contribution = contribution;
    state->distinct_contributors++;

    add_top_contributor(state, top_length, aid, contribution);
    return;
  }

  /* Save old contribution and set new value. */
  contribution_t contribution_old = entry->contribution;
  entry->contribution = descriptor->contribution_combine(contribution_old, contribution);

  /* We have to check for existence first. If it exists we bump, otherwise we try to insert. */
  bump_or_add_top_contributor(state, top_length, entry->aid, contribution_old, entry->contribution);
}

#define STATE_INDEX 0
#define AID_INDEX 1

ContributionTrackerState *get_aggregate_contribution_tracker(
    PG_FUNCTION_ARGS,
    const ContributionDescriptor *descriptor)
{
  if (!PG_ARGISNULL(STATE_INDEX))
  {
    return (ContributionTrackerState *)PG_GETARG_POINTER(STATE_INDEX);
  }

  MemoryContext agg_context;
  if (AggCheckCallContext(fcinfo, &agg_context) != AGG_CONTEXT_AGGREGATE)
  {
    ereport(ERROR, (errmsg("Aggregate called in non-aggregate context")));
  }

  Oid aid_type = get_fn_expr_argtype(fcinfo->flinfo, AID_INDEX);
  return contribution_tracker_new(
      agg_context,
      get_aid_descriptor(aid_type),
      *descriptor,
      0,
      g_config.outlier_count_max + g_config.top_count_max);
}
