#ifndef PG_DIFFIX_COMMON_H
#define PG_DIFFIX_COMMON_H

#include "fmgr.h"

/*-------------------------------------------------------------------------
 * Common aggregation interface
 *-------------------------------------------------------------------------
 */

typedef struct AnonAggFuncs AnonAggFuncs;
typedef struct AnonAggState AnonAggState;
typedef struct Bucket Bucket;

/* Known anonymizing aggregators. */
extern const AnonAggFuncs g_count_funcs;
extern const AnonAggFuncs g_count_any_funcs;
extern const AnonAggFuncs g_count_distinct_funcs;
extern const AnonAggFuncs g_lcf_funcs;

struct AnonAggFuncs
{
  /* Get type of final value. */
  Oid (*final_type)(void);

  /*
   * Create an empty state in the given memory context. The implementation is
   * responsible for switching to this memory context when allocating.
   * PG_FUNCTION_ARGS should be used only for inspecting parameter types.
   */
  AnonAggState *(*create_state)(MemoryContext memory_context, PG_FUNCTION_ARGS);

  /* Transitions the aggregator state for an input tuple. */
  void (*transition)(AnonAggState *state, PG_FUNCTION_ARGS);

  /* Derive final value from aggregation state and bucket data. */
  Datum (*finalize)(const AnonAggState *state, const Bucket *bucket, bool *is_null);

  /* Merge source aggregation state to destination state. */
  void (*merge)(AnonAggState *dst_state, const AnonAggState *src_state);

  /*
   * Returns a string representation of the aggregator state.
   * The string should be allocated in the current (not state's) memory context.
   */
  const char *(*explain)(const AnonAggState *state);
};

/*
 * Base data for an anonymizing aggregator state.
 * Must be the first member in derived structs.
 */
struct AnonAggState
{
  const AnonAggFuncs *agg_funcs; /* Aggregator implementation. */
  MemoryContext memory_context;  /* Where this state lives. */
};

struct Bucket
{
  int row_count;                                        /* Number of rows in the bucket. */
  int grouping_labels_length;                           /* Number of grouping labels. */
  NullableDatum grouping_labels[FLEXIBLE_ARRAY_MEMBER]; /* Values of grouping labels. */
};

#endif /* PG_DIFFIX_COMMON_H */
