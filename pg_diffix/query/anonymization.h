#ifndef PG_DIFFIX_ANONYMIZATION_H
#define PG_DIFFIX_ANONYMIZATION_H

#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"

#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/aggregation/noise.h"

/*
 * Opaque struct containing references to anonymizing (sub)queries.
 */
typedef struct AnonQueryLinks AnonQueryLinks;

/*
 * Transforms a standard query into an anonymizing query.
 * Returned data is used during plan rewrite.
 */
extern AnonQueryLinks *compile_anonymizing_query(Query *query, List *sensitive_relations);

/*
 * Wraps anonymizing Agg nodes with BucketScan nodes. Does nothing if links is NULL.
 * Frees memory associated with links after rewriting.
 */
extern Plan *rewrite_plan(Plan *plan, AnonQueryLinks *links);

/*
 * Ensures that the plan rows don't leak true counts, e.g. from `EXPLAIN`.
 */
extern bool censor_plan_rows(Plan *plan, bool *is_anonymizing);

/*
 * Ensures that the instrumentation doesn't leak true counts, e.g. from `EXPLAIN ANALYZE`.
 */
extern bool censor_instrumentation(PlanState *plan_state, bool *is_anonymizing);

/*
 * Returns the noise layer seed for the current bucket.
 */
seed_t compute_bucket_seed(const Bucket *bucket, const BucketDescriptor *bucket_desc);

#endif /* PG_DIFFIX_ANONYMIZATION_H */
