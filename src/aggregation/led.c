#include "postgres.h"

#include <math.h>

#include "nodes/pg_list.h"
#include "utils/datum.h"

#include "pg_diffix/aggregation/led.h"
#include "pg_diffix/utils.h"

typedef Bucket *BucketRef; /* Treat a bucket pointer as opaque in this file. */

#define MAX_SIBLINGS 3

typedef struct BucketSiblings
{
  BucketRef values[MAX_SIBLINGS]; /* (up to) First 3 buckets in subset */
} BucketSiblings;

typedef struct SiblingsTrackerData
{
  BucketDescriptor *bucket_desc; /* Reference to bucket descriptor */
  int skipped_column;            /* Skipped column for subsets */
} SiblingsTrackerData;

typedef struct SiblingsTrackerEntry
{
  BucketRef key;            /* First bucket in subset */
  BucketSiblings *siblings; /* Pointer to siblings in subset */
  uint32 hash;              /* Memorized hash */
  char status;              /* Required for hash table */
} SiblingsTrackerEntry;

static inline bool subset_equals(SiblingsTrackerData *data, BucketRef a, BucketRef b)
{
  BucketDescriptor *bucket_desc = data->bucket_desc;
  int skipped_column = data->skipped_column;
  int num_labels = bucket_desc->num_labels;

  for (int i = 0; i < num_labels; i++)
  {
    if (i == skipped_column)
      continue;

    if (a->is_null[i] != b->is_null[i])
      return false; /* NULL vs non-NULL. */

    if (a->is_null[i])
      continue; /* Both NULL. */

    BucketAttribute *att = &bucket_desc->attrs[i];
    if (!datumIsEqual(a->values[i], b->values[i], att->typ_byval, att->typ_len))
      return false;
  }

  return true;
}

static inline uint32 subset_hash(SiblingsTrackerData *data, BucketRef bucket)
{
  BucketDescriptor *bucket_desc = data->bucket_desc;
  int skipped_column = data->skipped_column;
  int num_labels = bucket_desc->num_labels;

  uint32 hash = 0;
  for (int i = 0; i < num_labels; i++)
  {
    if (i == skipped_column)
      continue;

    uint32 label_hash = bucket->is_null[i]
                            ? 0
                            : (uint32)hash_datum(bucket->values[i],
                                                 bucket_desc->attrs[i].typ_byval,
                                                 bucket_desc->attrs[i].typ_len);
    hash ^= label_hash;
  }

  return hash;
}

/*
 * Declarations for HashTable<Bucket, SiblingsTrackerEntry>
 */
#define SH_PREFIX SiblingsTracker
#define SH_ELEMENT_TYPE SiblingsTrackerEntry
#define SH_KEY key
#define SH_KEY_TYPE BucketRef
#define DATA(tb) ((SiblingsTrackerData *)tb->private_data)
#define SH_EQUAL(tb, a, b) subset_equals(DATA(tb), a, b)
#define SH_HASH_KEY(tb, key) subset_hash(DATA(tb), key)
#define SH_STORE_HASH
#define SH_GET_HASH(tb, entry) entry->hash
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

static inline void add_sibling(BucketSiblings *siblings, BucketRef bucket)
{
  for (int i = 0; i < MAX_SIBLINGS; i++)
  {
    if (siblings->values[i] == NULL)
    {
      siblings->values[i] = bucket;
      return;
    }
  }

  /* Array is full, do nothing. */
}

static inline int siblings_length(BucketSiblings *siblings)
{
  int len = 0;
  for (int i = 0; i < MAX_SIBLINGS; i++)
  {
    if (siblings->values[i] == NULL)
      break;
    else
      len++;
  }

  return len;
}

void led_hook(List *buckets, BucketDescriptor *bucket_desc)
{
  int num_buckets = list_length(buckets);
  int num_labels = bucket_desc->num_labels;

  /*
   * LED requires at least 2 columns - an isolating column and an unknown column.
   * With exactly 2 columns attacks are not useful because
   * they would have to isolate victims against the whole dataset.
   */
  if (num_labels <= 2)
    return;

  MemoryContext led_context = AllocSetContextCreate(bucket_desc->bucket_context, "led_hook context", ALLOCSET_DEFAULT_SIZES);
  MemoryContext temp_context = AllocSetContextCreate(led_context, "led_hook temporary context", ALLOCSET_DEFAULT_SIZES);

  MemoryContext old_context = MemoryContextSwitchTo(temp_context);

  /*
   * For each column, we build a cache where we group buckets by their labels EXCLUDING that column.
   * This means that every cache will associate siblings where the respective column is different.
   */
  SiblingsTracker_hash **trackers_per_column = palloc(num_labels * sizeof(SiblingsTracker_hash *));
  SiblingsTrackerData *data = palloc(num_labels * sizeof(SiblingsTrackerData));

  for (int i = 0; i < num_labels; i++)
  {
    data[i].bucket_desc = bucket_desc;
    data[i].skipped_column = i;
    trackers_per_column[i] = SiblingsTracker_create(temp_context, num_buckets, data + i);
  }

  /*
   * Per bucket, per column pointers to siblings. For example,
   * if we have 3 columns (c), then for each bucket (b) we have:
   * [ b0c0, b0c1, b0c2, b1c0, b1c1, b1c2, b2c0 ... ]
   */
  BucketSiblings **bucket_siblings = MemoryContextAllocZero(
      led_context,
      num_buckets * num_labels * sizeof(BucketSiblings *));

  /* Fill hash table & associate siblings. Skip star bucket (bucket #0). */
  for (int bucket_idx = 1; bucket_idx < num_buckets; bucket_idx++)
  {
    BucketRef bucket = (BucketRef)list_nth(buckets, bucket_idx);
    for (int column_idx = 0; column_idx < num_labels; column_idx++)
    {
      bool found;
      SiblingsTrackerEntry *entry = SiblingsTracker_insert(trackers_per_column[column_idx], bucket, &found);
      if (!found)
        entry->siblings = MemoryContextAllocZero(led_context, sizeof(BucketSiblings));

      add_sibling(entry->siblings, bucket);
      bucket_siblings[bucket_idx * num_labels + column_idx] = entry->siblings;
    }
  }

  /* Free hash table. */
  MemoryContextReset(temp_context);

  /* Temp storage to stage buckets for merging. */
  BucketRef *merge_targets = MemoryContextAlloc(led_context, num_labels * sizeof(BucketRef));
  int buckets_merged = 0;
  int total_merges = 0;

  /* LED bucket loop */
  for (int bucket_idx = 1; bucket_idx < num_buckets; bucket_idx++)
  {
    BucketRef bucket = (BucketRef)list_nth(buckets, bucket_idx);

    if (!bucket->low_count)
      continue;

    bool has_unknown_column = false;
    int isolating_columns = 0;

    for (int column_idx = 0; column_idx < num_labels; column_idx++)
    {
      BucketSiblings *siblings = bucket_siblings[bucket_idx * num_labels + column_idx];
      int num_siblings = siblings_length(siblings);
      if (num_siblings == 1)
      {
        /*
         * No siblings for this column (num_siblings=1 means itself).
         * A column without siblings is an unknown column.
         */
        Assert(siblings->values[0] == bucket);
        has_unknown_column = true;
      }
      else if (num_siblings == 2)
      {
        /*
         * Single sibling (self+other). Find it and check if it's high count.
         * A column with a single high-count sibling is an isolating column.
         */
        BucketRef other_bucket = (siblings->values[0] == bucket)
                                     ? (siblings->values[1])
                                     : (siblings->values[0]);
        if (!other_bucket->low_count)
          merge_targets[isolating_columns++] = other_bucket;
      }
      else
      {
        /* num_siblings=3 means there are multiple siblings and has no special meaning. */
      }
    }

    /* We need both for merge conditions. */
    if (!has_unknown_column || isolating_columns == 0)
      continue;

    for (int i = 0; i < isolating_columns; i++)
      merge_bucket(merge_targets[i], bucket, bucket_desc);

    buckets_merged++;
    total_merges += isolating_columns;
    bucket->merged = true;

    /* Free any garbage from merging. */
    MemoryContextReset(temp_context);
  }

  DEBUG_LOG("[LED] Buckets merged: %i; Total merges: %i", buckets_merged, total_merges);

  MemoryContextSwitchTo(old_context);

  MemoryContextDelete(led_context); /* Also removes temp_context. */
}
