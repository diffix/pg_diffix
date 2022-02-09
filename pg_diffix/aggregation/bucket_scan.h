#ifndef PG_DIFFIX_BUCKET_SCAN_H
#define PG_DIFFIX_BUCKET_SCAN_H

#include "nodes/plannodes.h"

extern Plan *make_bucket_scan(Plan *left_tree);

#endif /* PG_DIFFIX_BUCKET_SCAN_H */
