#ifndef PG_DIFFIX_ANONYMIZATION_H
#define PG_DIFFIX_ANONYMIZATION_H

#include "nodes/parsenodes.h"

#include "pg_diffix/aggregation/noise.h"

/*
 * Rewrites a regular query to an anonymizing query.
 */
extern void rewrite_query(Query *query, List *sensitive_relations);

/*
 * Returns the noise layer seed for the current bucket.
 */
extern seed_t compute_bucket_seed(void);

#endif /* PG_DIFFIX_ANONYMIZATION_H */
