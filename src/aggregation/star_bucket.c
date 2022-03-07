#include "postgres.h"

#include "utils/memutils.h"

#include "pg_diffix/aggregation/star_bucket.h"

Bucket *star_bucket_hook(List *buckets, BucketDescriptor *bucket_desc)
{
  MemoryContext bucket_context = bucket_desc->bucket_context;
  MemoryContext temp_context = AllocSetContextCreate(bucket_context, "star_bucket_hook temporary context", ALLOCSET_DEFAULT_SIZES);

  MemoryContext old_context = MemoryContextSwitchTo(temp_context);

  int num_atts = bucket_num_atts(bucket_desc);

  Bucket *star_bucket = (Bucket *)MemoryContextAllocZero(bucket_context, sizeof(Bucket));
  star_bucket->values = (Datum *)MemoryContextAllocZero(bucket_context, num_atts * sizeof(Datum));
  star_bucket->is_null = (bool *)MemoryContextAllocZero(bucket_context, num_atts * sizeof(bool));

  for (int i = 0; i < num_atts; i++)
  {
    BucketAttribute *att = &bucket_desc->attrs[i];
    if (att->tag == BUCKET_ANON_AGG)
    {
      /* Create an empty anon agg state and merge buckets into it. */
      star_bucket->values[i] = PointerGetDatum(att->agg_funcs->create_state(bucket_context, att->agg_args_desc));
    }
    else
    {
      /*
       * Everything else is NULL.
       * Todo: test for text type and put * instead.
       */
      star_bucket->is_null[i] = true;
    }
  }

  int buckets_merged = 0;
  ListCell *cell;
  foreach (cell, buckets)
  {
    Bucket *bucket = (Bucket *)lfirst(cell);
    if (bucket->low_count && !bucket->merged)
    {
      buckets_merged++;
      merge_bucket(star_bucket, bucket, bucket_desc);
      MemoryContextReset(temp_context);
    }
  }

  star_bucket->low_count = eval_low_count(star_bucket, bucket_desc);

  MemoryContextSwitchTo(old_context);

  MemoryContextDelete(temp_context);

  if (star_bucket->low_count || buckets_merged < 2)
    return NULL;
  else
    return star_bucket;
}
