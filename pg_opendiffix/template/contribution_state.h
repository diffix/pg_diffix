/* Largely based on Postgres' "simplehash.h" templating. */

/*
 * The following are expected as "input" macros.
 *
 * --- Base macros ---
 * CS_PREFIX
 * CS_AID_TYPE
 *
 * CS_TRACK_CONTRIBUTION             - whether to track contribution
 * CS_CONTRIBUTION_TYPE              - if CS_TRACK_CONTRIBUTION
 * CS_OVERALL_CONTRIBUTION_CALCULATE - if CS_TRACK_CONTRIBUTION
 *
 * --- Required for definition ---
 * CS_AID_EQUAL(a, b)
 * CS_AID_HASH(aid)
 * CS_CONTRIBUTION_GREATER(a, b)   - if CS_TRACK_CONTRIBUTION
 * CS_CONTRIBUTION_EQUAL(a, b)     - if CS_TRACK_CONTRIBUTION
 * CS_CONTRIBUTION_COMBINE(a, b)   - if CS_TRACK_CONTRIBUTION
 * CS_OVERALL_CONTRIBUTION_INITIAL - if CS_TRACK_CONTRIBUTION
 *
 * --- Optional ---
 * CS_INIT_AID_HASH(state, aid, aid_hash) - override initial AID hash calculation
 * CS_INIT_ENTRY(state, entry)            - callback to initialize hash table entries
 *
 * --- What to yield? ---
 * CS_SCOPE
 * CS_DECLARE
 * CS_DEFINE
 */

#ifndef PG_OPENDIFFIX_CONTRIBUTION_STATE_H
#define PG_OPENDIFFIX_CONTRIBUTION_STATE_H

/* Non-template stuff should go here. */

#include "postgres.h"
#include "fmgr.h"

#define CS_HASH_COMBINE(a, b) (a ^ b)
#define CS_SEED_INITIAL 0

#endif /* PG_OPENDIFFIX_CONTRIBUTION_STATE_H */

/* ----------------------------------------------------------------
 * Internal macros
 * ----------------------------------------------------------------
 */

/* Utils (taken from "simplehash.h") */
#define CS_MAKE_PREFIX(a) CppConcat(a, _)
#define CS_MAKE_NAME(name) CS_MAKE_NAME_(CS_MAKE_PREFIX(CS_PREFIX), name)
#define CS_MAKE_NAME_(a, b) CppConcat(a, b)

/* Types for contributors */
#define CS_TOP_CONTRIBUTOR CS_MAKE_NAME(Contributor)
#define CS_CONTRIBUTION_STATE CS_MAKE_NAME(ContributionState)

/* Types for table */
#define CS_TABLE CS_MAKE_NAME(Table)
#define CS_TABLE_HASH CS_MAKE_NAME(Table_hash)
#define CS_TABLE_ENTRY CS_MAKE_NAME(Table_Entry)

/* API Functions */
#define CS_STATE_NEW CS_MAKE_NAME(state_new)
#define CS_STATE_GETARG0 CS_MAKE_NAME(state_getarg0)
#define CS_STATE_UPDATE_AID CS_MAKE_NAME(state_update_aid)
#define CS_STATE_UPDATE_CONTRIBUTION CS_MAKE_NAME(state_update_contribution)

/* ----------------------------------------------------------------
 * Declarations
 * ----------------------------------------------------------------
 */

#ifdef CS_DECLARE

typedef struct CS_TABLE_ENTRY
{
  uint32 hash;
  CS_AID_TYPE aid;
#ifdef CS_TRACK_CONTRIBUTION
  CS_CONTRIBUTION_TYPE contribution;
#endif
  bool has_contribution;
  char status;
} CS_TABLE_ENTRY;

/* Hash table setup */

#define SH_PREFIX CS_TABLE
#define SH_ELEMENT_TYPE CS_TABLE_ENTRY
#define SH_KEY aid
#define SH_KEY_TYPE CS_AID_TYPE
#define SH_EQUAL(tb, a, b) CS_AID_EQUAL(a, b)
#define SH_HASH_KEY(tb, key) CS_AID_HASH(key)
#define SH_STORE_HASH
#define SH_GET_HASH(tb, a) a->hash
#define SH_SCOPE static inline
#define SH_DECLARE
#include "lib/simplehash.h"

#ifdef CS_TRACK_CONTRIBUTION

typedef struct CS_TOP_CONTRIBUTOR
{
  CS_AID_TYPE aid;
  CS_CONTRIBUTION_TYPE contribution;
} CS_TOP_CONTRIBUTOR;

#endif

typedef struct CS_CONTRIBUTION_STATE
{
  uint64 distinct_aids;            /* Number of distinct AIDs being tracked */
  uint32 aid_seed;                 /* Seed derived from unique AIDs */
  MemoryContext context;           /* Where the hash table lives */
  CS_TABLE_HASH *all_contributors; /* All contributors (map of AID -> TableEntry) */

#ifdef CS_TRACK_CONTRIBUTION

#ifdef CS_OVERALL_CONTRIBUTION_CALCULATE
  CS_CONTRIBUTION_TYPE overall_contribution; /* Total contribution from all contributors */
#endif
  int top_contributors_length;                                /* Length of top_contributors array */
  CS_TOP_CONTRIBUTOR top_contributors[FLEXIBLE_ARRAY_MEMBER]; /* Stores top_contributors_length number of top contributors */

#endif /* CS_TRACK_CONTRIBUTION */
} CS_CONTRIBUTION_STATE;

/* API Functions */

CS_SCOPE CS_CONTRIBUTION_STATE *CS_STATE_NEW(
    MemoryContext context
#ifdef CS_TRACK_CONTRIBUTION
    ,
    int top_contributors_length
#endif
);

CS_SCOPE CS_CONTRIBUTION_STATE *CS_STATE_GETARG0(PG_FUNCTION_ARGS);

CS_SCOPE void CS_STATE_UPDATE_AID(
    CS_CONTRIBUTION_STATE *state,
    CS_AID_TYPE aid);

#ifdef CS_TRACK_CONTRIBUTION
CS_SCOPE void CS_STATE_UPDATE_CONTRIBUTION(
    CS_CONTRIBUTION_STATE *state,
    CS_AID_TYPE aid,
    CS_CONTRIBUTION_TYPE contribution);
#endif

#endif /* CS_DECLARE */

/* ----------------------------------------------------------------
 * Definitions
 * ----------------------------------------------------------------
 */

#ifdef CS_DEFINE

#include "utils/elog.h"

/* Functions for table */
#define CS_TABLE_CREATE CS_MAKE_NAME(Table_create)
#define CS_TABLE_INSERT_HASH CS_MAKE_NAME(Table_insert_hash)

/* Internal functions */
#define CS_INSERTION_INDEX CS_MAKE_NAME(internal_insertion_index)
#define CS_AID_INDEX CS_MAKE_NAME(internal_aid_index)
#define CS_INSERT_CONTRIBUTOR CS_MAKE_NAME(internal_insert_contributor)
#define CS_BUMP_OR_INSERT_CONTRIBUTOR CS_MAKE_NAME(internal_bump_or_insert_contributor)
/*
 * CS_BUMP_OR_INSERT_CONTRIBUTOR and CS_BUMP_CONTRIBUTOR are the same,
 * but CS_BUMP_CONTRIBUTOR can be optimized in the future by utilizing the
 * knowledge that the AID already exists in the top contributors list.
 */
#define CS_BUMP_CONTRIBUTOR CS_BUMP_OR_INSERT_CONTRIBUTOR

/* Hash table setup */

#define SH_PREFIX CS_TABLE
#define SH_ELEMENT_TYPE CS_TABLE_ENTRY
#define SH_KEY aid
#define SH_KEY_TYPE CS_AID_TYPE
#define SH_EQUAL(tb, a, b) CS_AID_EQUAL(a, b)
#define SH_HASH_KEY(tb, key) CS_AID_HASH(key)
#define SH_STORE_HASH
#define SH_GET_HASH(tb, a) a->hash
#define SH_SCOPE static inline
#define SH_DEFINE
#include "lib/simplehash.h"

#ifdef CS_TRACK_CONTRIBUTION

static inline int CS_INSERTION_INDEX(
    CS_CONTRIBUTION_STATE *state,
    int top_length,
    CS_CONTRIBUTION_TYPE contribution)
{
  /*
   * Do a single comparison in the middle to halve lookup steps.
   * No. elements won't be large enough to bother with a full binary search.
   */
  for (int i = CS_CONTRIBUTION_GREATER(contribution, state->top_contributors[top_length / 2].contribution)
                   ? 0
                   : (top_length / 2 + 1);
       i < top_length;
       i++)
  {
    if (CS_CONTRIBUTION_GREATER(contribution, state->top_contributors[i].contribution))
    {
      return i;
    }
  }

  return top_length;
}

static inline int CS_AID_INDEX(
    CS_CONTRIBUTION_STATE *state,
    int top_length,
    CS_AID_TYPE aid,
    CS_CONTRIBUTION_TYPE old_contribution)
{
  for (int i = CS_CONTRIBUTION_GREATER(old_contribution, state->top_contributors[top_length / 2].contribution)
                   ? 0
                   : (top_length / 2 + 1);
       i < top_length;
       i++)
  {
    if (CS_AID_EQUAL(aid, state->top_contributors[i].aid))
    {
      return i;
    }
  }

  return top_length;
}

static inline void CS_INSERT_CONTRIBUTOR(
    CS_CONTRIBUTION_STATE *state,
    int top_length,
    CS_AID_TYPE aid,
    CS_CONTRIBUTION_TYPE contribution)
{
  int insertion_index = CS_INSERTION_INDEX(state, top_length, contribution);
  int capacity = state->top_contributors_length;
  size_t elements;

  if (insertion_index == capacity)
  {
    /* Do nothing if out of capacity. */
    return;
  }

  /* Slide items to the right before inserting new item. */
  elements = (top_length < capacity ? top_length + 1 : capacity) - insertion_index - 1;
  if (elements)
  {
    memmove(
        &state->top_contributors[insertion_index + 1],
        &state->top_contributors[insertion_index],
        elements * sizeof(CS_TOP_CONTRIBUTOR));
  }

  state->top_contributors[insertion_index].aid = aid;
  state->top_contributors[insertion_index].contribution = contribution;
}

static inline void CS_BUMP_OR_INSERT_CONTRIBUTOR(
    CS_CONTRIBUTION_STATE *state,
    int top_length,
    CS_AID_TYPE aid,
    CS_CONTRIBUTION_TYPE old_contribution,
    CS_CONTRIBUTION_TYPE new_contribution)
{
  int aid_index = CS_AID_INDEX(state, top_length, aid, old_contribution);
  int insertion_index;
  size_t elements;

  if (aid_index == top_length)
  {
    CS_INSERT_CONTRIBUTOR(state, top_length, aid, new_contribution);
    return;
  }

  insertion_index = CS_INSERTION_INDEX(state, top_length, new_contribution);

  Assert(aid_index > insertion_index); /* sanity check */
  elements = aid_index - insertion_index;
  if (elements)
  {
    memmove(
        &state->top_contributors[insertion_index + 1],
        &state->top_contributors[insertion_index],
        elements * sizeof(CS_TOP_CONTRIBUTOR));
  }

  state->top_contributors[insertion_index].aid = aid;
  state->top_contributors[insertion_index].contribution = new_contribution;
}

#endif /* CS_TRACK_CONTRIBUTION */

/* API Functions */

CS_SCOPE CS_CONTRIBUTION_STATE *CS_STATE_NEW(
    MemoryContext context
#ifdef CS_TRACK_CONTRIBUTION
    ,
    int top_contributors_length
#endif
)
{
  CS_CONTRIBUTION_STATE *state = (CS_CONTRIBUTION_STATE *)MemoryContextAlloc(
      context, sizeof(CS_CONTRIBUTION_STATE)
#ifdef CS_TRACK_CONTRIBUTION
                   + top_contributors_length * sizeof(CS_TOP_CONTRIBUTOR)
#endif
  );

  state->distinct_aids = 0;
  state->aid_seed = CS_SEED_INITIAL;
  state->context = context;
  state->all_contributors = CS_TABLE_CREATE(context, 128, NULL);

#ifdef CS_TRACK_CONTRIBUTION
  state->top_contributors_length = top_contributors_length;

#ifdef CS_OVERALL_CONTRIBUTION_CALCULATE
  state->overall_contribution = CS_OVERALL_CONTRIBUTION_INITIAL;
#endif
#endif /* CS_TRACK_CONTRIBUTION */

  return state;
}

CS_SCOPE CS_CONTRIBUTION_STATE *CS_STATE_GETARG0(PG_FUNCTION_ARGS)
{
  MemoryContext agg_context;

  if (!PG_ARGISNULL(0))
  {
    return (CS_CONTRIBUTION_STATE *)PG_GETARG_POINTER(0);
  }

  if (AggCheckCallContext(fcinfo, &agg_context) != AGG_CONTEXT_AGGREGATE)
  {
    ereport(ERROR, (errmsg("Aggregate called in non-aggregate context")));
  }

#ifdef CS_TRACK_CONTRIBUTION
  return CS_STATE_NEW(agg_context, 10);
#else
  return CS_STATE_NEW(agg_context);
#endif
}

CS_SCOPE void CS_STATE_UPDATE_AID(
    CS_CONTRIBUTION_STATE *state,
    CS_AID_TYPE aid)
{
  bool found;
  uint32 aid_hash;
  CS_TABLE_ENTRY *entry;

#ifdef CS_INIT_AID_HASH
  CS_INIT_AID_HASH(state, aid, aid_hash);
#else
  aid_hash = CS_AID_HASH(aid);
#endif

  entry = CS_TABLE_INSERT_HASH(state->all_contributors, aid, aid_hash, &found);
  if (!found)
  {
    state->distinct_aids++;
    state->aid_seed = CS_HASH_COMBINE(state->aid_seed, aid_hash);
    entry->has_contribution = false;
#ifdef CS_INIT_ENTRY
    CS_INIT_ENTRY(state, entry);
#endif
  }
}

#ifdef CS_TRACK_CONTRIBUTION

CS_SCOPE void CS_STATE_UPDATE_CONTRIBUTION(
    CS_CONTRIBUTION_STATE *state,
    CS_AID_TYPE aid,
    CS_CONTRIBUTION_TYPE contribution)
{
  bool found;
  uint32 aid_hash;
  int top_length;
  CS_TABLE_ENTRY *entry;
  CS_CONTRIBUTION_TYPE contribution_old;
  CS_CONTRIBUTION_TYPE min_top_contribution;

#ifdef CS_INIT_AID_HASH
  CS_INIT_AID_HASH(state, aid, aid_hash);
#else
  aid_hash = CS_AID_HASH(aid);
#endif

  entry = CS_TABLE_INSERT_HASH(state->all_contributors, aid, aid_hash, &found);
  /*
   * Careful!
   * If aid is a reference type, then entry->aid may be different from aid.
   * From this point entry->aid will be used because it's bound to the state context.
   */

  top_length = Min(state->distinct_aids, state->top_contributors_length);

#ifdef CS_OVERALL_CONTRIBUTION_CALCULATE
  state->overall_contribution = CS_CONTRIBUTION_COMBINE(state->overall_contribution, contribution);
#endif

  if (!found)
  {
    /* AID does not exist in table. */
    state->distinct_aids++;
    state->aid_seed = CS_HASH_COMBINE(state->aid_seed, aid_hash);
    entry->has_contribution = true;
    entry->contribution = contribution;
#ifdef CS_INIT_ENTRY
    CS_INIT_ENTRY(state, entry);
#endif

    /* We can insert to top contributors if either: */
    if (
        /* - top_contributors is not full */
        top_length != state->top_contributors_length ||
        /* - contribution is greater than the lowest top contribution */
        CS_CONTRIBUTION_GREATER(contribution, state->top_contributors[top_length - 1].contribution))
    {
      CS_INSERT_CONTRIBUTOR(state, top_length, entry->aid, contribution);
    }

    return;
  }
  else if (!entry->has_contribution)
  {
    /* AID exists but hasn't contributed yet. */
    entry->has_contribution = true;
    entry->contribution = contribution;
    if (top_length != state->top_contributors_length ||
        CS_CONTRIBUTION_GREATER(contribution, state->top_contributors[top_length - 1].contribution))
    {
      CS_INSERT_CONTRIBUTOR(state, top_length, entry->aid, contribution);
    }

    return;
  }

  contribution_old = entry->contribution;
  entry->contribution = CS_CONTRIBUTION_COMBINE(contribution_old, contribution);

  if (CS_CONTRIBUTION_EQUAL(entry->contribution, contribution_old))
  {
    /* Nothing changed. */
    return;
  }

  if (top_length != state->top_contributors_length)
  {
    /* We know AID is already a top contributor because top_contributors is not full. */
    CS_BUMP_CONTRIBUTOR(state, top_length, entry->aid, contribution_old, entry->contribution);
    return;
  }

  min_top_contribution = state->top_contributors[top_length - 1].contribution;

  if (CS_CONTRIBUTION_GREATER(contribution_old, min_top_contribution))
  {
    /* We know AID is already a top contributor because old contribution is greater than the lowest top contribution. */
    CS_BUMP_CONTRIBUTOR(state, top_length, entry->aid, contribution_old, entry->contribution);
    return;
  }

  if (CS_CONTRIBUTION_GREATER(min_top_contribution, entry->contribution))
  {
    /* Lowest top contribution is greater than new contribution. Nothing to do here. */
    return;
  }

  /*
   * We don't know whether AID is a top contributor or not because of possible equality.
   * We have to check for existence first. If it exists we bump, otherwise we insert.
   */
  CS_BUMP_OR_INSERT_CONTRIBUTOR(state, top_length, entry->aid, contribution_old, entry->contribution);
}

#endif /* CS_TRACK_CONTRIBUTION */

/* Functions for table */
#undef CS_TABLE_CREATE
#undef CS_TABLE_INSERT_HASH

/* Internal functions */
#undef CS_INSERTION_INDEX
#undef CS_AID_INDEX
#undef CS_INSERT_CONTRIBUTOR
#undef CS_BUMP_OR_INSERT_CONTRIBUTOR
#undef CS_BUMP_CONTRIBUTOR

#endif /* CS_DEFINE */

/* ----------------------------------------------------------------
 * Cleanup
 * ----------------------------------------------------------------
 */

/* Input macros */
#undef CS_PREFIX
#undef CS_AID_TYPE
#undef CS_TRACK_CONTRIBUTION
#undef CS_CONTRIBUTION_TYPE
#undef CS_OVERALL_CONTRIBUTION_CALCULATE
#undef CS_AID_EQUAL
#undef CS_AID_HASH
#undef CS_CONTRIBUTION_GREATER
#undef CS_CONTRIBUTION_EQUAL
#undef CS_CONTRIBUTION_COMBINE
#undef CS_OVERALL_CONTRIBUTION_INITIAL
#undef CS_INIT_AID_HASH
#undef CS_INIT_ENTRY
#undef CS_SCOPE
#undef CS_DECLARE
#undef CS_DEFINE

/* Helpers */
#undef CS_MAKE_PREFIX
#undef CS_MAKE_NAME
#undef CS_MAKE_NAME_

/* Types for contributors */
#undef CS_TOP_CONTRIBUTOR
#undef CS_CONTRIBUTION_STATE

/* Types for table */
#undef CS_TABLE
#undef CS_TABLE_HASH
#undef CS_TABLE_ENTRY

/* API Functions */
#undef CS_STATE_NEW
#undef CS_STATE_GETARG0
#undef CS_STATE_UPDATE_AID
#undef CS_STATE_UPDATE_CONTRIBUTION
