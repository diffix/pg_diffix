#include "postgres.h"

#include <math.h>

#include "fmgr.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "utils/lsyscache.h"

#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/oid_cache.h"
#include "pg_diffix/utils.h"

/* See AnonAggState definition in SQL. */
#define PG_GET_AGG_STATE(index) ((AnonAggState *)PG_GETARG_POINTER(index))
#define PG_RETURN_AGG_STATE(state) PG_RETURN_POINTER(state)

/* Memory context of currently executing BucketScan node (if any). */
extern MemoryContext g_current_bucket_context;

PG_FUNCTION_INFO_V1(anon_agg_state_input);
PG_FUNCTION_INFO_V1(anon_agg_state_output);
PG_FUNCTION_INFO_V1(anon_agg_state_transfn);
PG_FUNCTION_INFO_V1(anon_agg_state_finalfn);

ArgsDescriptor *build_args_desc(Aggref *aggref)
{
  List *args = aggref->args;

  int num_args = 1 + list_length(args); /* First item is AnonAggState. */
  ArgsDescriptor *args_desc = palloc0(sizeof(ArgsDescriptor) + num_args * sizeof(ArgDescriptor));
  args_desc->num_args = num_args;

  args_desc->args[0].expr = NULL; /* Agg state has no expression. */
  args_desc->args[0].type_oid = g_oid_cache.anon_agg_state;
  args_desc->args[0].typlen = sizeof(Datum);
  args_desc->args[0].typbyval = true;

  for (int i = 1; i < num_args; i++)
  {
    TargetEntry *arg_tle = list_nth_node(TargetEntry, args, i - 1);
    ArgDescriptor *arg_desc = &args_desc->args[i];
    arg_desc->expr = arg_tle->expr;
    arg_desc->type_oid = exprType((Node *)arg_tle->expr);
    get_typlenbyval(arg_desc->type_oid, &arg_desc->typlen, &arg_desc->typbyval);
  }

  return args_desc;
}

const AnonAggFuncs *find_agg_funcs(Oid oid)
{
  if (!OidIsValid(oid))
    return NULL;
  else if (oid == g_oid_cache.anon_count_star)
    return &g_count_star_funcs;
  else if (oid == g_oid_cache.anon_count_value)
    return &g_count_value_funcs;
  else if (oid == g_oid_cache.anon_count_distinct)
    return &g_count_distinct_funcs;
  else if (oid == g_oid_cache.anon_sum)
    return &g_sum_funcs;
  else if (oid == g_oid_cache.anon_count_histogram)
    return &g_count_histogram_funcs;
  else if (oid == g_oid_cache.anon_count_star_noise)
    return &g_count_star_noise_funcs;
  else if (oid == g_oid_cache.anon_count_value_noise)
    return &g_count_value_noise_funcs;
  else if (oid == g_oid_cache.anon_count_distinct_noise)
    return &g_count_distinct_noise_funcs;
  else if (oid == g_oid_cache.anon_sum_noise)
    return &g_sum_noise_funcs;
  else if (oid == g_oid_cache.low_count)
    return &g_low_count_funcs;

  return NULL;
}

bool eval_low_count(Bucket *bucket, BucketDescriptor *bucket_desc)
{
  int low_count_index = bucket_desc->low_count_index;
  Assert(low_count_index >= bucket_desc->num_labels && low_count_index < bucket_num_atts(bucket_desc));
  AnonAggState *agg_state = (AnonAggState *)DatumGetPointer(bucket->values[low_count_index]);
  Assert(agg_state != NULL);
  Assert(agg_state->agg_funcs == &g_low_count_funcs);
  bool is_null = false;
  Datum is_low_count = g_low_count_funcs.finalize(agg_state, bucket, bucket_desc, &is_null);
  Assert(!is_null);
  return DatumGetBool(is_low_count);
}

void merge_bucket(Bucket *destination, Bucket *source, BucketDescriptor *bucket_desc)
{
  int num_atts = bucket_num_atts(bucket_desc);
  for (int i = bucket_desc->num_labels; i < num_atts; i++)
  {
    BucketAttribute *att = &bucket_desc->attrs[i];
    if (att->tag == BUCKET_ANON_AGG)
    {
      Assert(!source->is_null[i]);
      Assert(!destination->is_null[i]);
      AnonAggState *dst_state = (AnonAggState *)destination->values[i];
      AnonAggState *src_state = (AnonAggState *)source->values[i];
      /* Shared states need to be merged only once. */
      if (dst_state != SHARED_AGG_STATE)
      {
        Assert(src_state != SHARED_AGG_STATE);
        att->agg.funcs->merge(dst_state, src_state);
      }
    }
  }
}

static AnonAggState *get_agg_state(PG_FUNCTION_ARGS)
{
  if (!PG_ARGISNULL(0))
    return PG_GET_AGG_STATE(0);

  MemoryContext bucket_context;
  if (AggCheckCallContext(fcinfo, &bucket_context) != AGG_CONTEXT_AGGREGATE)
    FAILWITH("Aggregate called in non-aggregate context");

  Aggref *aggref = AggGetAggref(fcinfo);

  if (g_current_bucket_context != NULL)
  {
    if (aggref_shares_state(aggref))
      return SHARED_AGG_STATE;

    bucket_context = g_current_bucket_context;
  }

  const AnonAggFuncs *agg_funcs = find_agg_funcs(aggref->aggfnoid);

  if (unlikely(agg_funcs == NULL))
    FAILWITH("Unsupported anonymizing aggregator (OID %u)", aggref->aggfnoid);

  return create_anon_agg_state(agg_funcs, bucket_context, build_args_desc(aggref));
}

Datum anon_agg_state_input(PG_FUNCTION_ARGS)
{
  FAILWITH("Cannot create aggregator state from string.");
  PG_RETURN_NULL();
}

Datum anon_agg_state_output(PG_FUNCTION_ARGS)
{
  AnonAggState *state = PG_GET_AGG_STATE(0);
  Assert(state != SHARED_AGG_STATE); /* Won't happen outside of a BucketScan context. */
  const char *str = state->agg_funcs->explain(state);
  PG_RETURN_CSTRING(str);
}

Datum anon_agg_state_transfn(PG_FUNCTION_ARGS)
{
  AnonAggState *state = get_agg_state(fcinfo);
  /* A SHARED_AGG_STATE means the owning aggregator will handle transitions. */
  if (state != SHARED_AGG_STATE)
    state->agg_funcs->transition(state, PG_NARGS(), fcinfo->args);
  PG_RETURN_AGG_STATE(state);
}

/*
 * This finalfunc is a dummy version which does nothing.
 * It only ensures that state is initialized for empty buckets.
 */
Datum anon_agg_state_finalfn(PG_FUNCTION_ARGS)
{
  AnonAggState *state = get_agg_state(fcinfo);
  PG_RETURN_AGG_STATE(state);
}

bool all_aids_null(NullableDatum *args, int aids_offset, int aids_count)
{
  for (int aid_index = aids_offset; aid_index < aids_offset + aids_count; aid_index++)
  {
    if (!args[aid_index].isnull)
      return false;
  }
  return true;
}

double round_reported_noise_sd(double noise_sd)
{
  if (noise_sd == 0.0)
  {
    return 0.0;
  }
  else
  {
    const double rounding_resolution = money_round(0.05 * noise_sd);
    return rounding_resolution * ceil(noise_sd / rounding_resolution);
  }
}
