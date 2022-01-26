#ifndef PG_DIFFIX_CONTRIBUTION_TRACKER_H
#define PG_DIFFIX_CONTRIBUTION_TRACKER_H

#include "fmgr.h"
#include "nodes/pg_list.h"

#include <inttypes.h>

#include "pg_diffix/aggregation/aid.h"
#include "pg_diffix/aggregation/noise.h"

#define CONTRIBUTION_INT_FMT PRIi64
#define CONTRIBUTION_REAL_FMT "f"

typedef union contribution_t
{
  int64 integer;
  float8 real;
} contribution_t;

/* Returns whether x is "more" contribution than y. */
typedef bool (*ContributionGreaterFunc)(contribution_t x, contribution_t y);

/* Returns whether x and y are equal contributions. */
typedef bool (*ContributionEqualFunc)(contribution_t x, contribution_t y);

/* Combines x and y and to a single contribution. */
typedef contribution_t (*ContributionCombineFunc)(contribution_t x, contribution_t y);

typedef struct ContributionDescriptor
{
  ContributionGreaterFunc contribution_greater;
  ContributionEqualFunc contribution_equal;
  ContributionCombineFunc contribution_combine;
  contribution_t contribution_initial; /* Initial or "zero" value for a contribution */
} ContributionDescriptor;

typedef struct Contributor
{
  aid_t aid;
  contribution_t contribution;
} Contributor;

typedef struct Contributors
{
  uint32 length;
  uint32 capacity;
  Contributor members[FLEXIBLE_ARRAY_MEMBER];
} Contributors;

typedef struct ContributionTrackerHashEntry
{
  Contributor contributor; /* Contributor info */
  bool has_contribution;   /* Whether the AID has contributed yet */
  char status;             /* Required for hash table */
} ContributionTrackerHashEntry;

/*
 * Declarations for HashTable<aid_t, ContributionTrackerHashEntry>
 */
#define SH_PREFIX ContributionTracker
#define SH_ELEMENT_TYPE ContributionTrackerHashEntry
#define SH_KEY_TYPE aid_t
#define SH_SCOPE extern
#define SH_DECLARE
#include "lib/simplehash.h"

typedef struct ContributionTrackerState
{
  AidDescriptor aid_descriptor;                   /* Behavior for AIDs */
  ContributionDescriptor contribution_descriptor; /* Behavior for contributions */
  ContributionTracker_hash *contribution_table;   /* Hash set of all AIDs */
  seed_t aid_seed;                                /* Current AID seed */
  uint64 distinct_contributors;                   /* Count of distinct non-NULL contributors */
  contribution_t overall_contribution;            /* Combined contribution from all contributors */
  uint64 unaccounted_for;                         /* Count of NULL contributions unaccounted for */
  /* NOTE: Variable size field, has to be last in the list! */
  Contributors top_contributors; /* AIDs with largest contributions */
} ContributionTrackerState;

/*
 * Updates state with an AID without contribution.
 */
extern void contribution_tracker_update_aid(ContributionTrackerState *state, aid_t aid);

/*
 * Updates state with a contribution from an AID.
 */
extern void contribution_tracker_update_contribution(
    ContributionTrackerState *state,
    aid_t aid,
    contribution_t contribution);

/*
 * Gets or creates the multi-AID aggregation state from the function arguments.
 */
extern List *get_aggregate_contribution_trackers(
    PG_FUNCTION_ARGS,
    int aids_offset,
    const ContributionDescriptor *descriptor);

extern void add_top_contributor(
    const ContributionDescriptor *descriptor,
    Contributors *top_contributors,
    Contributor contributor);

extern void update_or_add_top_contributor(
    const ContributionDescriptor *descriptor,
    Contributors *top_contributors,
    Contributor contributor);

#endif /* PG_DIFFIX_CONTRIBUTION_TRACKER_H */
