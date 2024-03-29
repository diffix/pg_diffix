#ifndef PG_DIFFIX_ANONYMIZATION_H
#define PG_DIFFIX_ANONYMIZATION_H

#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"

#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/aggregation/noise.h"
#include "pg_diffix/node_funcs.h"

/*
 * Opaque struct containing references to anonymizing (sub)queries.
 */
typedef struct AnonQueryLinks AnonQueryLinks;

/*
 * Transforms subqueries accessing personal relations into anonymizing subqueries.
 * Returned data is used during plan rewrite.
 */
extern AnonQueryLinks *compile_query(Query *query, List *personal_relations, ParamListInfo bound_params);

/*
 * Calls `rewrite_plan` for each item in a list of Plan nodes.
 */
extern void rewrite_plan_list(List *plans, AnonQueryLinks *links);

/*
 * Wraps anonymizing Agg nodes with BucketScan nodes. Does nothing if links is NULL.
 */
extern Plan *rewrite_plan(Plan *plan, AnonQueryLinks *links);

/*
 * Returns the noise layer seed for the current bucket.
 */
seed_t compute_bucket_seed(const Bucket *bucket, const BucketDescriptor *bucket_desc);

#endif /* PG_DIFFIX_ANONYMIZATION_H */
