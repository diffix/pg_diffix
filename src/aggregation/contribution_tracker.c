#include "postgres.h"
#include "common/hashfn.h"
#include "utils/elog.h"

#include "pg_diffix/config.h"
#include "pg_diffix/utils.h"
#include "pg_diffix/aggregation/contribution_tracker.h"

/* ----------------------------------------------------------------
 * Top contributors management
 * ----------------------------------------------------------------
 */

static inline uint32 find_aid_index(
    const TopContributor *top_contributors,
    uint32 top_length,
    aid_t aid)
{
  for (uint32 i = 0; i < top_length; i++)
  {
    if (aid == top_contributors[i].aid)
      return i;
  }

  return top_length;
}

static inline uint32 find_insertion_index(
    const ContributionDescriptor *descriptor,
    const TopContributor *top_contributors,
    uint32 top_length,
    contribution_t contribution)
{
  ContributionGreaterFunc greater = descriptor->contribution_greater;

  /*
   * Do a single comparison in the middle to halve lookup steps.
   * No. elements won't be large enough to bother with a full binary search.
   */
  contribution_t middle_contribution = top_contributors[top_length / 2].contribution;
  uint32 start_index = greater(contribution, middle_contribution) ? 0 : (top_length / 2 + 1);
  for (uint32 i = start_index; i < top_length; i++)
  {
    if (greater(contribution, top_contributors[i].contribution))
      return i;
  }

  return top_length;
}

void add_top_contributor(
    const ContributionDescriptor *descriptor,
    TopContributor *top_contributors,
    uint32 capacity,
    uint32 top_length,
    aid_t aid,
    contribution_t contribution)
{
  Assert(capacity >= top_length);

  /*
   * Entry is not a top contributor if capacity is exhausted and
   * contribution is not greater than the lowest top contribution.
   */
  if (top_length == capacity &&
      !descriptor->contribution_greater(contribution, top_contributors[top_length - 1].contribution))
    return;

  uint32 insertion_index = find_insertion_index(descriptor, top_contributors, top_length, contribution);
  Assert(insertion_index < capacity); /* sanity check */

  /* Slide items to the right before inserting new item. */
  size_t elements = (top_length < capacity ? top_length + 1 : capacity) - insertion_index - 1;
  memmove(&top_contributors[insertion_index + 1],
          &top_contributors[insertion_index],
          elements * sizeof(TopContributor));

  top_contributors[insertion_index].aid = aid;
  top_contributors[insertion_index].contribution = contribution;
}

void update_or_add_top_contributor(
    const ContributionDescriptor *descriptor,
    TopContributor *top_contributors,
    uint32 capacity,
    uint32 top_length,
    aid_t aid,
    contribution_t contribution)
{
  Assert(capacity >= top_length);

  uint32 aid_index = find_aid_index(top_contributors, top_length, aid);
  if (aid_index == top_length)
  {
    /* Not an existing top contributor, try to add it as a new entry and return. */
    add_top_contributor(descriptor, top_contributors, capacity, top_length, aid, contribution);
    return;
  }

  uint32 insertion_index = find_insertion_index(descriptor, top_contributors, top_length, contribution);
  Assert(insertion_index <= aid_index); /* sanity check */

  size_t elements = aid_index - insertion_index;
  memmove(&top_contributors[insertion_index + 1],
          &top_contributors[insertion_index],
          elements * sizeof(TopContributor));

  top_contributors[insertion_index].aid = aid;
  top_contributors[insertion_index].contribution = contribution;
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

static ContributionTrackerState *contribution_tracker_new(
    AidDescriptor aid_descriptor,
    const ContributionDescriptor *contribution_descriptor,
    uint32 top_contributors_length)
{
  ContributionTrackerState *state = (ContributionTrackerState *)palloc0(
      sizeof(ContributionTrackerState) + top_contributors_length * sizeof(TopContributor));

  state->aid_descriptor = aid_descriptor;
  state->contribution_descriptor = *contribution_descriptor;
  state->contribution_table = ContributionTracker_create(CurrentMemoryContext, 128, NULL);
  state->distinct_contributors = 0;
  state->overall_contribution = contribution_descriptor->contribution_initial;
  state->aid_seed = 0;
  state->top_contributors_length = top_contributors_length;

  return state;
}

void contribution_tracker_update_aid(ContributionTrackerState *state, aid_t aid)
{
  bool found;
  ContributionTrackerHashEntry *entry = ContributionTracker_insert(state->contribution_table, aid, &found);
  if (!found)
  {
    state->aid_seed ^= aid;
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

  state->overall_contribution = descriptor->contribution_combine(state->overall_contribution, contribution);

  bool found;
  ContributionTrackerHashEntry *entry = ContributionTracker_insert(state->contribution_table, aid, &found);
  if (!found)
  {
    /* AID does not exist in table. */
    entry->has_contribution = true;
    entry->contribution = contribution;
    state->distinct_contributors++;
    state->aid_seed ^= aid;

    add_top_contributor(&state->contribution_descriptor,
                        state->top_contributors, state->top_contributors_length, top_length,
                        aid, contribution);
    return;
  }
  else if (!entry->has_contribution)
  {
    /* AID exists but hasn't contributed yet. */
    entry->has_contribution = true;
    entry->contribution = contribution;
    state->distinct_contributors++;

    add_top_contributor(
        &state->contribution_descriptor,
        state->top_contributors, state->top_contributors_length, top_length,
        aid, contribution);
    return;
  }

  /* Aggregate new contribution. */
  entry->contribution = descriptor->contribution_combine(entry->contribution, contribution);

  /* We have to check for existence first. If it exists we bump, otherwise we try to insert. */
  update_or_add_top_contributor(
      &state->contribution_descriptor,
      state->top_contributors, state->top_contributors_length, top_length,
      entry->aid, entry->contribution);
}

static const int STATE_INDEX = 0;

List *get_aggregate_contribution_trackers(
    PG_FUNCTION_ARGS,
    int aids_offset,
    const ContributionDescriptor *descriptor)
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
    ContributionTrackerState *tracker = contribution_tracker_new(
        get_aid_descriptor(aid_type),
        descriptor,
        g_config.outlier_count_max + g_config.top_count_max);
    trackers = lappend(trackers, tracker);
  }

  MemoryContextSwitchTo(old_context);
  return trackers;
}
