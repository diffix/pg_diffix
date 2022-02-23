#include "postgres.h"

#include "pg_diffix/aggregation/contribution_tracker.h"
#include "pg_diffix/config.h"
#include "pg_diffix/utils.h"

/* ----------------------------------------------------------------
 * Top contributors management
 * ----------------------------------------------------------------
 */

static inline uint32 find_aid_index(const Contributors *top_contributors, aid_t aid)
{
  for (uint32 i = 0; i < top_contributors->length; i++)
  {
    if (aid == top_contributors->members[i].aid)
      return i;
  }

  return top_contributors->length;
}

static inline uint32 find_insertion_index(
    const ContributionDescriptor *descriptor,
    const Contributors *top_contributors,
    contribution_t contribution)
{
  ContributionGreaterFunc greater = descriptor->contribution_greater;

  /*
   * Do a single comparison in the middle to halve lookup steps.
   * No. elements won't be large enough to bother with a full binary search.
   */
  contribution_t middle_contribution = top_contributors->members[top_contributors->length / 2].contribution;
  uint32 start_index = greater(contribution, middle_contribution) ? 0 : (top_contributors->length / 2 + 1);
  for (uint32 i = start_index; i < top_contributors->length; i++)
  {
    if (greater(contribution, top_contributors->members[i].contribution))
      return i;
  }

  return top_contributors->length;
}

void add_top_contributor(
    const ContributionDescriptor *descriptor,
    Contributors *top_contributors,
    Contributor contributor)
{
  uint32 length = top_contributors->length, capacity = top_contributors->capacity;
  Assert(capacity >= length);

  /*
   * Entry is not a top contributor if capacity is exhausted and
   * contribution is not greater than the lowest top contribution.
   */
  if (length == capacity)
  {
    Contributor lowest_contributor = top_contributors->members[length - 1];
    if (!descriptor->contribution_greater(contributor.contribution, lowest_contributor.contribution))
      return;
  }

  uint32 insertion_index = find_insertion_index(descriptor, top_contributors, contributor.contribution);
  Assert(insertion_index < top_contributors->capacity); /* sanity check */

  /* Slide items to the right before inserting new item. */
  size_t elements = (length < capacity ? length + 1 : capacity) - insertion_index - 1;
  memmove(&top_contributors->members[insertion_index + 1],
          &top_contributors->members[insertion_index],
          elements * sizeof(Contributor));

  top_contributors->members[insertion_index] = contributor;
  top_contributors->length = Min(length + 1, capacity);
}

void update_or_add_top_contributor(
    const ContributionDescriptor *descriptor,
    Contributors *top_contributors,
    Contributor contributor)
{
  Assert(top_contributors->capacity >= top_contributors->length);

  uint32 aid_index = find_aid_index(top_contributors, contributor.aid);
  if (aid_index == top_contributors->length)
  {
    /* Not an existing top contributor, try to add it as a new entry and return. */
    add_top_contributor(descriptor, top_contributors, contributor);
    return;
  }

  uint32 insertion_index = find_insertion_index(descriptor, top_contributors, contributor.contribution);
  Assert(insertion_index <= aid_index); /* sanity check */

  size_t elements = aid_index - insertion_index;
  memmove(&top_contributors->members[insertion_index + 1],
          &top_contributors->members[insertion_index],
          elements * sizeof(Contributor));

  top_contributors->members[insertion_index] = contributor;
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
#define SH_KEY contributor.aid
#define SH_KEY_TYPE aid_t
#define SH_EQUAL(tb, a, b) a == b
#define SH_HASH_KEY(tb, key) (uint32) key /* `key` is already a hash */
#define SH_SCOPE inline
#define SH_DEFINE
#include "lib/simplehash.h"

static ContributionTrackerState *contribution_tracker_new(
    AidDescriptor aid_descriptor,
    const ContributionDescriptor *contribution_descriptor)
{
  uint32 top_capacity = g_config.outlier_count_max + g_config.top_count_max;
  ContributionTrackerState *state = (ContributionTrackerState *)palloc0(
      sizeof(ContributionTrackerState) + top_capacity * sizeof(Contributor));

  state->aid_descriptor = aid_descriptor;
  state->contribution_descriptor = *contribution_descriptor;
  state->contribution_table = ContributionTracker_create(CurrentMemoryContext, 128, NULL);
  state->aid_seed = 0;
  state->distinct_contributors = 0;
  state->unaccounted_for = 0;
  state->overall_contribution = contribution_descriptor->contribution_initial;
  state->top_contributors.length = 0;
  state->top_contributors.capacity = top_capacity;

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
    state->distinct_contributors++;
  }
}

void contribution_tracker_update_contribution(
    ContributionTrackerState *state,
    aid_t aid,
    contribution_t contribution)
{
  ContributionDescriptor *descriptor = &state->contribution_descriptor;

  state->overall_contribution = descriptor->contribution_combine(state->overall_contribution, contribution);

  bool found;
  ContributionTrackerHashEntry *entry = ContributionTracker_insert(state->contribution_table, aid, &found);
  if (!found)
  {
    /* AID does not exist in table. */
    entry->has_contribution = true;
    entry->contributor.contribution = contribution;
    state->distinct_contributors++;
    state->aid_seed ^= aid;

    add_top_contributor(&state->contribution_descriptor,
                        &state->top_contributors,
                        entry->contributor);
    return;
  }
  else if (!entry->has_contribution)
  {
    /* AID exists but hasn't contributed yet. */
    entry->has_contribution = true;
    entry->contributor.contribution = contribution;

    add_top_contributor(
        &state->contribution_descriptor,
        &state->top_contributors,
        entry->contributor);
    return;
  }

  /* Aggregate new contribution. */
  entry->contributor.contribution = descriptor->contribution_combine(entry->contributor.contribution, contribution);

  /* We have to check for existence first. If it exists we bump, otherwise we try to insert. */
  update_or_add_top_contributor(
      &state->contribution_descriptor,
      &state->top_contributors,
      entry->contributor);
}

List *create_contribution_trackers(
    PG_FUNCTION_ARGS,
    int aids_offset,
    const ContributionDescriptor *descriptor)
{
  Assert(PG_NARGS() > aids_offset);

  List *trackers = NIL;
  for (int arg_index = aids_offset; arg_index < PG_NARGS(); arg_index++)
  {
    Oid aid_type = get_fn_expr_argtype(fcinfo->flinfo, arg_index);
    ContributionTrackerState *tracker = contribution_tracker_new(get_aid_descriptor(aid_type), descriptor);
    trackers = lappend(trackers, tracker);
  }

  return trackers;
}
