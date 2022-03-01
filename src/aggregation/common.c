#include "postgres.h"

#include "nodes/primnodes.h"

#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/oid_cache.h"
#include "pg_diffix/utils.h"

/* See AggState definition in SQL. */
#define PG_GET_AGG_STATE(index) ((AnonAggState *)PG_GETARG_INT64(index))
#define PG_RETURN_AGG_STATE(state) PG_RETURN_INT64(state)

/* Memory context of currently executing BucketScan node (if any). */
extern MemoryContext g_current_bucket_context;

PG_FUNCTION_INFO_V1(anon_agg_state_input);
PG_FUNCTION_INFO_V1(anon_agg_state_output);
PG_FUNCTION_INFO_V1(anon_agg_state_transfn);
PG_FUNCTION_INFO_V1(anon_agg_state_finalfn);

Datum anon_agg_state_input(PG_FUNCTION_ARGS)
{
  FAILWITH("Cannot create aggregator state from string.");
  PG_RETURN_NULL();
}

Datum anon_agg_state_output(PG_FUNCTION_ARGS)
{
  AnonAggState *state = PG_GET_AGG_STATE(0);
  const char *str = state->agg_funcs->explain(state);
  PG_RETURN_CSTRING(str);
}

const AnonAggFuncs *find_agg_funcs(Oid oid)
{
  Assert(OidIsValid(oid));

  if (oid == g_oid_cache.anon_count_star)
    return &g_count_star_funcs;
  else if (oid == g_oid_cache.anon_count_value)
    return &g_count_value_funcs;
  else if (oid == g_oid_cache.anon_count_distinct)
    return &g_count_distinct_funcs;
  else if (oid == g_oid_cache.low_count)
    return &g_low_count_funcs;

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

  if (unlikely(agg_funcs == NULL))
    FAILWITH("Unsupported anonymizing aggregator (OID %u)", aggref->aggfnoid);

  AnonAggState *state = agg_funcs->create_state(bucket_context, fcinfo);
  state->agg_funcs = agg_funcs;
  state->memory_context = bucket_context;

  return state;
}

Datum anon_agg_state_transfn(PG_FUNCTION_ARGS)
{
  AnonAggState *state = get_agg_state(fcinfo);
  state->agg_funcs->transition(state, fcinfo);
  PG_RETURN_AGG_STATE(state);
}

/*
 * This finalfunc is a dummy version which does nothing.
 * It only ensures that state is not null for empty buckets.
 */
Datum anon_agg_state_finalfn(PG_FUNCTION_ARGS)
{
  AnonAggState *state = get_agg_state(fcinfo);
  PG_RETURN_AGG_STATE(state);
}
