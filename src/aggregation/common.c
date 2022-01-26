#include "postgres.h"
#include "nodes/primnodes.h"

#include "pg_diffix/oid_cache.h"
#include "pg_diffix/utils.h"
#include "pg_diffix/aggregation/common.h"

/* See AggState definition in SQL. */
#define PG_GET_AGG_STATE(index) ((AnonAggState *)PG_GETARG_INT64(index))
#define PG_RETURN_AGG_STATE(state) PG_RETURN_INT64(state)

/* Memory context of currently executing BucketScan node (if any). */
/* extern MemoryContext g_current_bucket_context; */

/* TODO: Remove once bucket scan is implemented. */
MemoryContext g_current_bucket_context = NULL;

PG_FUNCTION_INFO_V1(agg_state_input);
PG_FUNCTION_INFO_V1(agg_state_output);
PG_FUNCTION_INFO_V1(agg_state_transfn);
PG_FUNCTION_INFO_V1(agg_state_finalfn);

Datum agg_state_input(PG_FUNCTION_ARGS)
{
  FAILWITH("Cannot create aggregator state from string.");
  PG_RETURN_NULL();
}

Datum agg_state_output(PG_FUNCTION_ARGS)
{
  AnonAggState *state = PG_GET_AGG_STATE(0);
  char *str = state->agg_funcs->explain(state);
  PG_RETURN_CSTRING(str);
}

static const AnonAggFuncs *find_agg_funcs(Oid oid)
{
  Assert(OidIsValid(oid));

  if (oid == g_oid_cache.anon_count)
    return &g_count_funcs;
  else if (oid == g_oid_cache.anon_count_any)
    return &g_count_any_funcs;
  else if (oid == g_oid_cache.anon_count_distinct)
    return &g_count_distinct_funcs;
  else if (oid == g_oid_cache.lcf)
    return &g_lcf_funcs;

  FAILWITH("Unsupported anonymizing aggregator (OID %u)", oid);
  return NULL;
}

static AnonAggState *get_agg_state(PG_FUNCTION_ARGS)
{
  if (!PG_ARGISNULL(0))
    return PG_GET_AGG_STATE(0);

  MemoryContext bucket_context;
  if (AggCheckCallContext(fcinfo, &bucket_context) != AGG_CONTEXT_AGGREGATE)
    FAILWITH("Aggregate called in non-aggregate context");

  if (g_current_bucket_context != NULL)
    bucket_context = g_current_bucket_context;

  Aggref *aggref = AggGetAggref(fcinfo);
  const AnonAggFuncs *agg_funcs = find_agg_funcs(aggref->aggfnoid);

  AnonAggState *state = agg_funcs->create_state(bucket_context, fcinfo);
  Assert(state->agg_funcs == agg_funcs);
  Assert(state->memory_context == bucket_context);

  return state;
}

Datum agg_state_transfn(PG_FUNCTION_ARGS)
{
  AnonAggState *state = get_agg_state(fcinfo);
  state->agg_funcs->transition(state, fcinfo);
  PG_RETURN_AGG_STATE(state);
}

/*
 * This finalfunc is a dummy version which does nothing.
 * It only ensures that state is not null for empty buckets.
 */
Datum agg_state_finalfn(PG_FUNCTION_ARGS)
{
  AnonAggState *state = get_agg_state(fcinfo);
  PG_RETURN_AGG_STATE(state);
}
