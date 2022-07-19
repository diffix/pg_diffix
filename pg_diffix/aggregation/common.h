#ifndef PG_DIFFIX_COMMON_H
#define PG_DIFFIX_COMMON_H

#include "access/attnum.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"

#include "pg_diffix/aggregation/noise.h"

/*-------------------------------------------------------------------------
 *
 * AnonAggFuncs is the unified interface for all anonymizing aggregators.
 *
 * The `create_state` function must:
 *   - Switch to the provided memory context to ensure adequate lifetime of the state.
 *   - Return a derived struct with its first member of type AnonAggState (not pointer!).
 *   - Inspect ArgsDescriptor for type info. Argument at index 0 should be ignored
 *     because it is managed by the wrapper function. ArgsDescriptor is short lived
 *     and must not be cached.
 *   - Initialize aggregator data such as hash tables (in the provided memory context).
 *
 * The `transition` function must:
 *   - Advance the aggregator state for the given input tuple.
 *     Argument at index 0 should be ignored because it is managed by the wrapper function.
 *   - Switch to the state's memory context when attaching data to the state.
 *     If moving any input Datums to the state, they must be copied first with `datumCopy`.
 *
 * The `finalize` function derives the final value (of type `final_type`) of the aggregator.
 * Temporary and return data should not be allocated in the state's memory context but in
 * the current memory context which is shorter lived. See below for information about memory.
 * Because state might be borrowed from another aggregator, `finalize` must be idempotent,
 * meaning multiple executions against the same state have to return the same result.
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

#define SHARED_AGG_STATE NULL

/* Describes a single function call argument. */
typedef struct ArgDescriptor
{
  Expr *expr;    /* Expression of argument or NULL if unknown */
  Oid type_oid;  /* Type OID of argument */
  int16 typlen;  /* Length of argument type */
  bool typbyval; /* Whether argument type is by val */
} ArgDescriptor;

/* Describes the list of function call arguments. */
typedef struct ArgsDescriptor
{
  int num_args;                              /* Number of arguments in function call */
  ArgDescriptor args[FLEXIBLE_ARRAY_MEMBER]; /* Descriptors of individual arguments */
} ArgsDescriptor;

/*
 * Describes the transfn arguments of an anonymizing aggregator.
 */
extern ArgsDescriptor *build_args_desc(Aggref *aggref);

typedef struct AnonAggFuncs AnonAggFuncs;
typedef struct AnonAggState AnonAggState;

/* Known anonymizing aggregators. */
extern const AnonAggFuncs g_count_star_funcs;
extern const AnonAggFuncs g_count_value_funcs;
extern const AnonAggFuncs g_sum_funcs;
extern const AnonAggFuncs g_count_distinct_noise_funcs;
extern const AnonAggFuncs g_count_star_noise_funcs;
extern const AnonAggFuncs g_count_value_noise_funcs;
extern const AnonAggFuncs g_sum_noise_funcs;
extern const AnonAggFuncs g_count_distinct_funcs;
extern const AnonAggFuncs g_low_count_funcs;
extern const AnonAggFuncs g_count_histogram_funcs;

typedef enum BucketAttributeTag
{
  BUCKET_LABEL = 0,
  BUCKET_REGULAR_AGG,
  BUCKET_ANON_AGG
} BucketAttributeTag;

/* Describes a bucket label or aggregate. */
typedef struct BucketAttribute
{
  BucketAttributeTag tag; /* Label or aggregate? */
  struct
  {
    Aggref *aggref;            /* Expr of aggregate */
    ArgsDescriptor *args_desc; /* Agg arguments descriptor */
    const AnonAggFuncs *funcs; /* Agg funcs if tag=BUCKET_ANON_AGG */
    int redirect_to;           /* If shared, points to attribute that owns the state */
  } agg;                       /* Populated if tag!=BUCKET_LABEL */
  int typ_len;                 /* Data type length */
  bool typ_byval;              /* Data type is by value? */
  char *resname;               /* Name of source TargetEntry */
  Oid final_type;              /* Final type OID */
  int final_typmod;            /* Final type modifier */
  Oid final_collid;            /* Final type collation ID */
} BucketAttribute;

typedef struct AnonymizationContext
{
  seed_t sql_seed;            /* Static part of bucket seed */
  List *base_labels_hash_set; /* Hashed labels that apply to all buckets (from filters) */
  AttrNumber *grouping_cols;  /* Array of indices into the target list for the grouping columns */
  int grouping_cols_count;    /* Count of grouping columns */
  bool expand_buckets;        /* True if buckets have to be expanded for this query */
} AnonymizationContext;

typedef struct BucketDescriptor
{
  MemoryContext bucket_context;                 /* Memory context where buckets live */
  AnonymizationContext *anon_context;           /* Corresponding query anonymization parameters */
  int low_count_index;                          /* Index of low count agg, or -1 if none */
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
  void (*final_type)(const ArgsDescriptor *args_desc, Oid *type, int32 *typmod, Oid *collid);

  /*
   * Create an empty state in the given memory context. The implementation is
   * responsible for switching to this memory context when allocating.
   */
  AnonAggState *(*create_state)(MemoryContext memory_context, ArgsDescriptor *args_desc);

  /* Transitions the aggregator state for an input tuple. */
  void (*transition)(AnonAggState *state, int num_args, NullableDatum *args);

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
 * Creates an empty AnonAggState state for given AnonAggFuncs.
 */
static inline AnonAggState *create_anon_agg_state(const AnonAggFuncs *agg_funcs,
                                                  MemoryContext bucket_context,
                                                  ArgsDescriptor *args_desc)
{
  AnonAggState *state = agg_funcs->create_state(bucket_context, args_desc);
  state->agg_funcs = agg_funcs;
  state->memory_context = bucket_context;
  return state;
}

/*
 * Finds aggregator spec for given OID.
 * Returns NULL if the given OID is not an anonymizing aggregator.
 */
extern const AnonAggFuncs *find_agg_funcs(Oid oid);

/*
 * Returns true if the given OID represents an anonymizing aggregator.
 */
static inline bool is_anonymizing_agg(Oid oid)
{
  return find_agg_funcs(oid) != NULL;
}

/*
 * Determines whether the given bucket is low count.
 */
extern bool eval_low_count(Bucket *bucket, BucketDescriptor *bucket_desc);

/*
 * Merges all anonymizing aggregator states from source bucket to destination bucket.
 */
extern void merge_bucket(Bucket *destination, Bucket *source, BucketDescriptor *bucket_desc);

/*
 * Returns true if all AID instances in the given range are NULL.
 */
extern bool all_aids_null(NullableDatum *args, int aids_offset, int aids_count);

/*
 * Rounds the noise std. dev. to obtain a reported noise value.
 */
extern double round_reported_noise_sd(double noise_sd);

#endif /* PG_DIFFIX_COMMON_H */
