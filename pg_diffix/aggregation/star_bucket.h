#ifndef PG_DIFFIX_STAR_BUCKET_H
#define PG_DIFFIX_STAR_BUCKET_H

#include "nodes/pg_list.h"

#include "pg_diffix/aggregation/common.h"

extern Bucket *star_bucket_hook(List *buckets, BucketDescriptor *bucket_desc);

#endif /* PG_DIFFIX_STAR_BUCKET_H */
