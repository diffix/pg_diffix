#ifndef PG_DIFFIX_BUCKET_SCAN_H
#define PG_DIFFIX_BUCKET_SCAN_H

#include "nodes/plannodes.h"

#include "pg_diffix/aggregation/common.h"

extern Plan *make_bucket_scan(Plan *left_tree, AnonymizationContext *anon_context);

#endif /* PG_DIFFIX_BUCKET_SCAN_H */
