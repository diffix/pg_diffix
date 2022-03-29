#ifndef PG_DIFFIX_BUCKET_SCAN_H
#define PG_DIFFIX_BUCKET_SCAN_H

#include "nodes/plannodes.h"

#include "pg_diffix/aggregation/common.h"

/*
 * Registers custom nodes needed by BucketScan.
 */
extern void register_bucket_scan_nodes(void);

/*
 * Wraps and rewrites an Agg node with a BucketScan.
 * See bucket_scan.c for details.
 */
extern Plan *make_bucket_scan(Plan *left_tree, AnonymizationContext *anon_context);

#endif /* PG_DIFFIX_BUCKET_SCAN_H */
