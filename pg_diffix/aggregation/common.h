#ifndef PG_DIFFIX_COMMON_H
#define PG_DIFFIX_COMMON_H

#include "fmgr.h"

/*-------------------------------------------------------------------------
 *
 * AnonAggFuncs is the unified interface for all anonymizing aggregators.
 *
 * The `create_state` function must:
 *   - Switch to the provided memory context to ensure adequate lifetime of the state.
 *   - Return a derived struct with its first member of type AnonAggState (not pointer!) and populate it.
 *   - Inspect PG_FUNCTION_ARGS for type info but not values, since it may also be called during finalfunc.
 *     Argument at index 0 should be ignored because it is managed by the wrapper function.
 *   - Initialize aggregator data such as hash tables (in the provided memory context).
 *
 * The `transition` function must:
 *   - Advance the aggregator state for the given input tuple (read PG_FUNCTION_ARGS).
 *     Argument at index 0 should be ignored because it is managed by the wrapper function.
 *   - Switch to the state's memory context when attaching data to the state.
 *     If moving any input Datums to the state, they must be copied first with `datumCopy`.
 *
 * The `finalize` function derives the final value (of type `final_type`) of the aggregator.
 * Temporary and return data should not be allocated in the state's memory context but in
 * the current memory context which is shorter lived. See below for information about memory.
 *
 * The `explain` function returns a human-readable representation of the aggregator state.
 * As with `finalize`, the current memory context should be used for temporary and return values.
 *
 * The `merge` function merges source state into destination state.
 * Merging 2 separately aggregated sequences must be equivalent to a single aggregation
 * of those 2 sequences concatenated, meaning:
 *
 *     agg2(args2) -> merge_to -> agg1(args1) == agg(args1 ++ args2)
 *
 * Both states are known to live in the same memory context. Temporary data should be allocated
 * in the current memory context (not state's).
 *
 * Memory contexts:
 *
 * During aggregation, we have the following memory contexts (longest lifespan to shortest):
 *   - bucket_context (for all buckets)
 *   - outer_context  (aka aggregation_context, for current bucket)
 *   - inner_context  (aka tuple_context, for current input tuple)
 *
 * Once a memory context is reset or destroyed, any memory attached to it will be freed.
 * It is safe to use the current context for returning values from functions
 * because it will not be reset until the owning executor has been called again for the next tuple.
 * By that time we can assume the Datum has been emitted or copied by the parent node.
 *
 * The following pseudocode illustrates how often each context is reset:
 *
 *   bucket_context = new context or NULL if no BucketScan parent
 *   outer_context  = new context
 *   inner_context  = new context
 *
 *   for bucket in buckets:
 *     reset outer_context
 *     state <- create_state(bucket_context || outer_context)
 *
 *     for input_tuple in bucket:
 *       reset inner_context
 *       current_context <- inner_context
 *       state <- transition(state, input_tuple)
 *
 *   destroy inner_context, outer_context
 *
 *   process, finalize, and emit buckets in BucketScan (if it exists)
 *   destroy bucket_context
 *
 *-------------------------------------------------------------------------
 */

typedef struct AnonAggFuncs AnonAggFuncs;
typedef struct AnonAggState AnonAggState;

/* Known anonymizing aggregators. */
extern const AnonAggFuncs g_count_star_funcs;
extern const AnonAggFuncs g_count_value_funcs;
extern const AnonAggFuncs g_count_distinct_funcs;
extern const AnonAggFuncs g_low_count_funcs;

typedef enum BucketAttributeTag
{
  BUCKET_LABEL = 0,
  BUCKET_REGULAR_AGG,
  BUCKET_ANON_AGG
} BucketAttributeTag;

/* Describes a bucket label or aggregate. */
typedef struct BucketAttribute
{
  const AnonAggFuncs *agg_funcs; /* Agg funcs if tag=BUCKET_ANON_AGG */
  BucketAttributeTag tag;        /* Label or aggregate? */
  int typ_len;                   /* Data type length */
  bool typ_byval;                /* Data type is by value? */
  char *resname;                 /* Name of source TargetEntry */
  Oid final_type;                /* Final type OID */
  int final_typmod;              /* Final type modifier */
  Oid final_collid;              /* Final type collation ID */
} BucketAttribute;

typedef struct BucketDescriptor
{
  MemoryContext bucket_context;                 /* Memory context where buckets live */
  int num_labels;                               /* Number of label attributes */
  int num_aggs;                                 /* Number of aggregate attributes */
  BucketAttribute attrs[FLEXIBLE_ARRAY_MEMBER]; /* Descriptors of grouping labels followed by aggregates */
} BucketDescriptor;

static inline int bucket_num_atts(BucketDescriptor *bucket_desc)
{
  return bucket_desc->num_labels + bucket_desc->num_aggs;
}

/*
 * A bucket is an output row from an aggregation node.
 */
typedef struct Bucket
{
  Datum *values;  /* Array of label values followed by aggregates */
  bool *is_null;  /* Attribute at index is null? */
  bool low_count; /* Has low count AIDs? */
  bool merged;    /* Was merged to some other bucket? */
} Bucket;

struct AnonAggFuncs
{
  /* Get type information of final value. */
  void (*final_type)(Oid *type, int32 *typmod, Oid *collid);

  /*
   * Create an empty state in the given memory context. The implementation is
   * responsible for switching to this memory context when allocating.
   * PG_FUNCTION_ARGS should be used only for inspecting parameter types.
   */
  AnonAggState *(*create_state)(MemoryContext memory_context, PG_FUNCTION_ARGS);

  /* Transitions the aggregator state for an input tuple. */
  void (*transition)(AnonAggState *state, PG_FUNCTION_ARGS);

  /* Derive final value from aggregation state and bucket data. */
  Datum (*finalize)(AnonAggState *state, Bucket *bucket, BucketDescriptor *bucket_desc, bool *is_null);

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

/*
 * Finds aggregator spec for given OID.
 * Returns NULL if the given OID is not an anonymizing aggregator.
 */
extern const AnonAggFuncs *find_agg_funcs(Oid oid);

/*
 * Merges all anonymizing aggregates from source bucket to destination bucket.
 */
extern void merge_bucket(Bucket *destination, Bucket *source, BucketDescriptor *bucket_desc);

#endif /* PG_DIFFIX_COMMON_H */
