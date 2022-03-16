#ifndef PG_DIFFIX_ANONYMIZATION_H
#define PG_DIFFIX_ANONYMIZATION_H

#include "nodes/parsenodes.h"

#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/aggregation/noise.h"

/*
 * Transforms a standard query into an anonymizing query.
 */
extern void compile_anonymizing_query(Query *query, List *sensitive_relations);

/*
 * Returns the noise layer seed for the current bucket.
 */
seed_t compute_bucket_seed(const Bucket *bucket, const BucketDescriptor *bucket_desc);

#endif /* PG_DIFFIX_ANONYMIZATION_H */
