#ifndef PG_DIFFIX_CONTRIBUTION_TRACKER_H
#define PG_DIFFIX_CONTRIBUTION_TRACKER_H

#include "postgres.h"
#include "fmgr.h"

#include "pg_diffix/aid.h"

typedef union contribution_t
{
  int64 integer;
  double real;
} contribution_t;

/* Gets contribution from a datum. */
typedef contribution_t (*ContributionMakeFunc)(Datum datum);

/* Returns whether x is "more" contribution than y. */
typedef bool (*ContributionGreaterFunc)(contribution_t x, contribution_t y);

/* Returns whether x and y are equal contributions. */
typedef bool (*ContributionEqualFunc)(contribution_t x, contribution_t y);

/* Combines x and y and to a single contribution. */
typedef contribution_t (*ContributionCombineFunc)(contribution_t x, contribution_t y);

typedef struct ContributionFunctions
{
  ContributionMakeFunc contribution_make;
  ContributionGreaterFunc contribution_greater;
  ContributionEqualFunc contribution_equal;
  ContributionCombineFunc contribution_combine;
  contribution_t contribution_initial; /* Initial or "zero" value for a contribution */
} ContributionFunctions;

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
  AidFunctions aid_functions;                             /* Behavior for AIDs */
  ContributionFunctions contribution_functions;           /* Behavior for contributions */
  ContributionTracker_hash *contribution_table;           /* Hash set of all AIDs */
  uint64 total_contributions;                             /* Total count of non-NULL contributions */
  uint32 distinct_contributors;                           /* Count of distinct non-NULL contriburs*/
  contribution_t overall_contribution;                    /* Combined contribution from all contributors */
  uint64 aid_seed;                                        /* Current AID seed */
  uint32 top_contributors_length;                         /* Length of top_contributors array */
  TopContributor top_contributors[FLEXIBLE_ARRAY_MEMBER]; /* Stores top_contributors_length number of top contributors */
} ContributionTrackerState;

/*
 * Creates a new empty state.
 */
extern ContributionTrackerState *contribution_tracker_new(
    MemoryContext context,
    AidFunctions aid_functions,
    ContributionFunctions contribution_functions,
    uint64 initial_seed,
    uint32 top_contributors_length);

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
 * Gets or creates the aggregation state from the function arguments.
 */
extern ContributionTrackerState *get_aggregate_contribution_tracker(
    PG_FUNCTION_ARGS,
    ContributionFunctions *functions);

#endif /* PG_DIFFIX_CONTRIBUTION_TRACKER_H */
