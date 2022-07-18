#include "postgres.h"

#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/aggregation/star_bucket.h"
#include "pg_diffix/config.h"
#include "pg_diffix/oid_cache.h"

static void set_text_label(Bucket *star_bucket, int att_idx, Oid type, MemoryContext context)
{
  switch (type)
  {
  case CSTRINGOID:
    star_bucket->values[att_idx] = CStringGetDatum(g_config.text_label_for_suppress_bin);
    break;
  /* Postgres codebase indicates these are handled the same way. */
  case TEXTOID:
  case VARCHAROID:
  case BPCHAROID:;
    MemoryContext old_context = MemoryContextSwitchTo(context);
    star_bucket->values[att_idx] = PointerGetDatum(cstring_to_text(g_config.text_label_for_suppress_bin));
    MemoryContextSwitchTo(old_context);
    break;
  default:
    star_bucket->is_null[att_idx] = true;
    break;
  }
}

Bucket *star_bucket_hook(List *buckets, BucketDescriptor *bucket_desc)
{
  MemoryContext bucket_context = bucket_desc->bucket_context;
  MemoryContext temp_context = AllocSetContextCreate(bucket_context, "star_bucket_hook temporary context", ALLOCSET_DEFAULT_SIZES);

  MemoryContext old_context = MemoryContextSwitchTo(temp_context);

  int num_atts = bucket_num_atts(bucket_desc);

  Bucket *star_bucket = MemoryContextAllocZero(bucket_context, sizeof(Bucket));
  star_bucket->values = MemoryContextAllocZero(bucket_context, num_atts * sizeof(Datum));
  star_bucket->is_null = MemoryContextAllocZero(bucket_context, num_atts * sizeof(bool));

  for (int i = 0; i < num_atts; i++)
  {
    BucketAttribute *att = &bucket_desc->attrs[i];
    if (att->tag == BUCKET_ANON_AGG)
      /* Create an empty anon agg state and merge buckets into it. */
      star_bucket->values[i] = PointerGetDatum(i != att->agg.redirect_to
                                                   ? SHARED_AGG_STATE
                                                   : create_anon_agg_state(att->agg.funcs, bucket_context, att->agg.args_desc));
    else if (att->tag == BUCKET_LABEL)
      set_text_label(star_bucket, i, att->final_type, bucket_context);
    else if (att->agg.fn_oid == g_oid_cache.is_suppress_bin)
      star_bucket->values[i] = BoolGetDatum(true);
    else
      star_bucket->is_null[i] = true;
  }

  int buckets_merged = 0;

  int num_buckets = list_length(buckets);
  for (int i = 1; i < num_buckets; i++)
  {
    Bucket *bucket = (Bucket *)list_nth(buckets, i);
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
