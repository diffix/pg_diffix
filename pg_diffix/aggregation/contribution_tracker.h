#ifndef PG_DIFFIX_CONTRIBUTION_TRACKER_H
#define PG_DIFFIX_CONTRIBUTION_TRACKER_H

#include "c.h"
#include "fmgr.h"
#include "nodes/pg_list.h"

#include <inttypes.h>

#include "pg_diffix/aggregation/aid.h"

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

typedef struct ContributionTrackerHashEntry
{
  contribution_t contribution; /* Contribution from AID */
  aid_t aid;                   /* Entry key */
  bool has_contribution;       /* Whether the AID has contributed yet */
  char status;                 /* Required for hash table */
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

typedef struct TopContributor
{
  aid_t aid;
  contribution_t contribution;
} TopContributor;

typedef struct ContributionTrackerState
{
  AidDescriptor aid_descriptor;                           /* Behavior for AIDs */
  ContributionDescriptor contribution_descriptor;         /* Behavior for contributions */
  ContributionTracker_hash *contribution_table;           /* Hash set of all AIDs */
  uint64 contributions_count;                             /* Total count of non-NULL contributions */
  uint32 distinct_contributors;                           /* Count of distinct non-NULL contributors */
  contribution_t overall_contribution;                    /* Combined contribution from all contributors */
  uint64 aid_seed;                                        /* Current AID seed */
  uint32 top_contributors_length;                         /* Length of top_contributors array */
  TopContributor top_contributors[FLEXIBLE_ARRAY_MEMBER]; /* Stores top_contributors_length number of top contributors */
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

#endif /* PG_DIFFIX_CONTRIBUTION_TRACKER_H */
