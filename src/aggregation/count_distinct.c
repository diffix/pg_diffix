#include "postgres.h"

#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/typcache.h"

#include "pg_diffix/aggregation/count.h"
#include "pg_diffix/aggregation/summable.h"
#include "pg_diffix/config.h"
#include "pg_diffix/query/anonymization.h"

/*
 * For each unique value we encounter, we keep a set of AID values for each AID instance available.
 * We limit the size of an AID value set to the `maximum threshold for low-count buckets` + 1.
 */
typedef struct DistinctTrackerHashEntry
{
  Datum value;           /* Unique value */
  List *aid_values_sets; /* List of AID sets, one for each AID instance */
  char status;           /* Required for hash table */
} DistinctTrackerHashEntry;

/* Metadata needed for hashing and equality checks on the unique values. */
typedef struct DistinctTrackerData
{
  int16 typlen;  /* Cached `typlen` attribute for value type */
  bool typbyval; /* Cached `typbyval` attribute for value type */
} DistinctTrackerData;

/*
 * Declarations for HashTable<Datum, DistinctTrackerHashEntry>
 */
#define SH_PREFIX DistinctTracker
#define SH_ELEMENT_TYPE DistinctTrackerHashEntry
#define SH_KEY value
#define SH_KEY_TYPE Datum
#define DATA(tb) ((DistinctTrackerData *)tb->private_data)
#define SH_EQUAL(tb, a, b) datumIsEqual(a, b, DATA(tb)->typbyval, DATA(tb)->typlen)
#define SH_HASH_KEY(tb, key) (uint32) hash_datum(key, DATA(tb)->typbyval, DATA(tb)->typlen)
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

static const int VALUE_INDEX = 1;
static const int AIDS_OFFSET = 2;

static DistinctTrackerHashEntry *
get_distinct_tracker_entry(DistinctTracker_hash *tracker, Datum value, int aids_count)
{
  bool found;
  DistinctTrackerHashEntry *entry = DistinctTracker_insert(tracker, value, &found);
  if (!found)
  {
    entry->aid_values_sets = NIL;
    entry->value = datumCopy(value, DATA(tracker)->typbyval, DATA(tracker)->typlen);
    for (int i = 0; i < aids_count; i++)
    {
      entry->aid_values_sets = lappend(entry->aid_values_sets, NIL);
    }
  }
  return entry;
}

/*
 * We need additional meta-data to compare values, but we can't pass a comparison context to
 * the sorting function, so we make it a global instead.
 */
FmgrInfo *g_compare_values_func;
TypeCacheEntry *g_compare_values_typentry;

static int compare_datums(const Datum value_a, const Datum value_b)
{
  Datum c = FunctionCall2Coll(g_compare_values_func, g_compare_values_typentry->typcollation, value_a, value_b);
  return DatumGetInt32(c);
}

static void set_value_sorting_globals(Oid element_type)
{
  g_compare_values_typentry = lookup_type_cache(element_type, TYPECACHE_CMP_PROC_FINFO);
  g_compare_values_func = &g_compare_values_typentry->cmp_proc_finfo;
}

static bool aid_set_is_high_count(const List *aid_values_set)
{
  if (list_length(aid_values_set) < g_config.low_count_min_threshold)
    return false; /* Fewer AID values than minimum threshold, value is low-count. */

  seed_t aid_seed = hash_set_to_seed(aid_values_set);
  double threshold = generate_lcf_threshold(aid_seed);

  return list_length(aid_values_set) >= threshold;
}

static bool aid_sets_are_high_count(const List *aid_values_sets)
{
  ListCell *cell;
  foreach (cell, aid_values_sets)
  {
    const List *aid_values_set = (const List *)lfirst(cell);
    if (!aid_set_is_high_count(aid_values_set))
      return false;
  }
  return true;
}

/* Returns a list with the tracker entries that are low count. */
static List *filter_lc_entries(DistinctTracker_hash *tracker)
{
  List *lc_entries = NIL;

  DistinctTrackerHashEntry *entry;
  foreach_entry(entry, tracker, DistinctTracker)
  {
    if (!aid_sets_are_high_count(entry->aid_values_sets))
      lc_entries = lappend(lc_entries, entry);
  }

  return lc_entries;
}

static int compare_tracker_entries_by_value(const ListCell *a, const ListCell *b)
{
  Datum value_a = ((const DistinctTrackerHashEntry *)lfirst(a))->value;
  Datum value_b = ((const DistinctTrackerHashEntry *)lfirst(b))->value;

  return compare_datums(value_a, value_b);
}

static Contributors *create_contributors(uint32 capacity)
{
  Contributors *contributors = palloc(sizeof(Contributors) + capacity * sizeof(Contributor));
  contributors->length = 0;
  contributors->capacity = capacity;

  return contributors;
}

/* Holds the low-count values contributed by an AID value. */
typedef struct PerAidValuesEntry
{
  aid_t aid;
  List *values;
  uint32 contributions;
} PerAidValuesEntry;

static List *associate_value_with_aid(List *per_aid_values, aid_t aid, Datum value)
{
  /*
   * Do a binary search for an existing entry or for the insertion location of a new entry.
   * Since Postgres lists are actually arrays, cells are stored in memory sequentially,
   * so index lookups are O(1).
   */
  int start = 0, end = list_length(per_aid_values) - 1;
  while (start <= end)
  {
    int middle = start + (end - start) / 2;
    PerAidValuesEntry *entry = (PerAidValuesEntry *)list_nth(per_aid_values, middle);
    if (entry->aid < aid)
    {
      start = middle + 1;
    }
    else if (entry->aid > aid)
    {
      end = middle - 1;
    }
    else /* We found an already existing entry. */
    {
      entry->values = lappend(entry->values, (void *)value);
      return per_aid_values;
    }
  }

  /* No entry found, we insert a new one in the correct position to keep the list ordered. */
  PerAidValuesEntry *entry = palloc0(sizeof(PerAidValuesEntry));
  entry->aid = aid;
  entry->values = list_make1((void *)value);
  return list_insert_nth(per_aid_values, start, entry);
}

/* Maps values per-AID given the list of low-count tracker entries and an AID values set index. */
static List *transpose_lc_values_per_aid(List *lc_entries, int aid_index, uint32 *lc_values_true_count)
{
  List *per_aid_values = NIL;
  *lc_values_true_count = 0;

  ListCell *lc_entry_cell;
  foreach (lc_entry_cell, lc_entries)
  {
    const DistinctTrackerHashEntry *entry = (const DistinctTrackerHashEntry *)lfirst(lc_entry_cell);
    const List *aid_values_set = (const List *)list_nth(entry->aid_values_sets, aid_index);

    if (aid_values_set != NIL) /* Count unique value only if it has at least one associated AID value. */
      (*lc_values_true_count)++;

    ListCell *aidv_cell;
    foreach (aidv_cell, aid_values_set)
    {
      aid_t aid = (aid_t)lfirst(aidv_cell);
      per_aid_values = associate_value_with_aid(per_aid_values, aid, entry->value);
    }
  }

  return per_aid_values;
}

static int compare_per_aid_values_entries(const ListCell *a, const ListCell *b)
{
  const PerAidValuesEntry *entry_a = (const PerAidValuesEntry *)lfirst(a);
  const PerAidValuesEntry *entry_b = (const PerAidValuesEntry *)lfirst(b);
  if (list_length(entry_a->values) != list_length(entry_b->values))
  {
    /* Order entries by increasing count of values. */
    return list_length(entry_a->values) - list_length(entry_b->values);
  }
  else
  {
    /* To ensure determinism, order entries with identical counts by AID value. */
    if (entry_a->aid > entry_b->aid)
      return 1;
    else if (entry_a->aid < entry_b->aid)
      return -1;
    else
      return 0;
  }
}

static void *list_pop_back(List **list)
{
  if (*list == NIL)
    return NULL;

  void *value = llast(*list);
  *list = list_delete_last(*list);
  return value;
}

/*
 * Used to leverage `simplehash.h` as a simple HashSet<Datum>.
 */
typedef struct DatumSetEntry
{
  Datum value; /* Unique value */
  char status; /* Required for hash table */
} DatumSetEntry;

/*
 * Declarations for HashTable<Datum, DatumSetEntry>>
 * Since values held here are unique at this point, we can use simple pointer equality even for reference types.
 */
#define SH_PREFIX DatumSet
#define SH_ELEMENT_TYPE DatumSetEntry
#define SH_KEY value
#define SH_KEY_TYPE Datum
#define SH_EQUAL(tb, a, b) (a == b)
#define SH_HASH_KEY(tb, key) (uint32) hash_bytes(&key, sizeof(Datum))
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

static inline bool is_marked_as_used(DatumSet_hash *used_values, Datum value)
{
  return DatumSet_lookup(used_values, value) != NULL;
}

static inline void mark_as_used(DatumSet_hash *used_values, Datum value)
{
  bool found;
  DatumSet_insert(used_values, value, &found);
}

/*
 * Builds the top contributors array from the list of per-AID low-count values.
 * From each AID value in turn, in increasing order of contributions amount, a unique value
 * is counted and marked as "used", until all distinct values are exhausted.
 */
static void distribute_lc_values(List *per_aid_values, uint32 values_count)
{
  DatumSet_hash *used_values = DatumSet_create(CurrentMemoryContext, values_count, NULL);
  while (values_count > 0)
  {
    ListCell *cell;
    foreach (cell, per_aid_values)
    {
      PerAidValuesEntry *entry = (PerAidValuesEntry *)lfirst(cell);
      if (entry->values != NIL)
      {
        Datum value = (Datum)list_pop_back(&entry->values);
        while (entry->values != NIL && is_marked_as_used(used_values, value))
          value = (Datum)list_pop_back(&entry->values);
        if (!is_marked_as_used(used_values, value))
        {
          values_count--;
          mark_as_used(used_values, value);
          entry->contributions++;
        }
      }
    }
  }
  DatumSet_destroy(used_values);
}

/* Computes the aid seed, total count of contributors and fills the top contributors array. */
static void process_lc_values_contributions(List *per_aid_values,
                                            seed_t *aid_seed,
                                            uint64 *contributors_count,
                                            Contributors *top_contributors)
{
  *contributors_count = 0;
  *aid_seed = 0;

  ListCell *cell;
  foreach (cell, per_aid_values)
  {
    PerAidValuesEntry *entry = (PerAidValuesEntry *)lfirst(cell);
    if (entry->contributions > 0)
    {
      *aid_seed ^= entry->aid;
      Contributor contributor = {.aid = entry->aid, .contribution = {.integer = entry->contributions}};
      add_top_contributor(&integer_descriptor, top_contributors, contributor);
      (*contributors_count)++;
    }
  }
}

typedef struct CountDistinctState
{
  AnonAggState base;
  ArgsDescriptor *args_desc;
  DistinctTracker_hash *tracker;
} CountDistinctState;

typedef struct CountDistinctResult
{
  int64 hc_values_count;
  int64 lc_values_count;
  int64 noisy_count;
  double noise_sd;
  bool not_enough_aid_values;
} CountDistinctResult;

/*
 * The number of high count values is safe to be shown directly, without any extra noise.
 * The number of low count values has to be anonymized.
 */
static CountDistinctResult count_distinct_calculate_final(AnonAggState *base_state, Bucket *bucket, BucketDescriptor *bucket_desc)
{
  CountDistinctState *state = (CountDistinctState *)base_state;

  seed_t bucket_seed = compute_bucket_seed(bucket, bucket_desc);

  int aids_count = state->args_desc->num_args - AIDS_OFFSET;
  set_value_sorting_globals(state->args_desc->args[VALUE_INDEX].type_oid);

  DistinctTracker_hash *tracker = state->tracker;

  List *lc_entries = filter_lc_entries(tracker);
  list_sort(lc_entries, &compare_tracker_entries_by_value); /* Needed to ensure determinism. */

  CountDistinctResult result = {0};
  result.lc_values_count = list_length(lc_entries);
  result.hc_values_count = tracker->members - result.lc_values_count;
  result.noisy_count = result.hc_values_count;

  uint32 top_contributors_capacity = g_config.outlier_count_max + g_config.top_count_max;

  SummableResultAccumulator lc_result_accumulator = {0};

  for (int aid_index = 0; aid_index < aids_count; aid_index++)
  {
    Contributors *top_contributors = create_contributors(top_contributors_capacity);

    uint32 lc_values_true_count = 0;
    List *per_aid_values = transpose_lc_values_per_aid(lc_entries, aid_index, &lc_values_true_count);

    list_sort(per_aid_values, &compare_per_aid_values_entries);
    distribute_lc_values(per_aid_values, lc_values_true_count);

    seed_t aid_seed = 0;
    uint64 contributors_count = 0;
    process_lc_values_contributions(
        per_aid_values,
        &aid_seed, &contributors_count,
        top_contributors);

    contribution_t unaccounted_for = {.integer = 0};
    contribution_t true_count = {.integer = (int64)lc_values_true_count};
    SummableResult inner_count_result = aggregate_contributions(
        bucket_seed, aid_seed, true_count,
        contributors_count, unaccounted_for, integer_descriptor.contribution_to_double, top_contributors);

    list_free_deep(per_aid_values);
    pfree(top_contributors);

    accumulate_result(&lc_result_accumulator, &inner_count_result);
    if (lc_result_accumulator.not_enough_aid_values)
      break;
  }

  if (!lc_result_accumulator.not_enough_aid_values)
  {
    result.noisy_count += finalize_count_result(&lc_result_accumulator);
    result.noise_sd = finalize_noise_result(&lc_result_accumulator);
  }

  result.not_enough_aid_values = lc_result_accumulator.not_enough_aid_values && result.hc_values_count == 0;
  return result;
}

static ArgsDescriptor *copy_args_desc(const ArgsDescriptor *source)
{
  size_t args_desc_size = sizeof(ArgsDescriptor) + source->num_args * sizeof(ArgDescriptor);
  ArgsDescriptor *dest = palloc(args_desc_size);
  memcpy(dest, source, args_desc_size);
  return dest;
}

/*-------------------------------------------------------------------------
 * Aggregation callbacks
 *-------------------------------------------------------------------------
 */

static void count_distinct_final_type(const ArgsDescriptor *args_desc, Oid *type, int32 *typmod, Oid *collid)
{
  *type = INT8OID;
  *typmod = -1;
  *collid = 0;
}

static AnonAggState *count_distinct_create_state(MemoryContext memory_context, ArgsDescriptor *args_desc)
{
  MemoryContext old_context = MemoryContextSwitchTo(memory_context);

  CountDistinctState *state = palloc0(sizeof(CountDistinctState));

  DistinctTrackerData *data = palloc0(sizeof(DistinctTrackerData));
  data->typlen = args_desc->args[VALUE_INDEX].typlen;
  data->typbyval = args_desc->args[VALUE_INDEX].typbyval;

  state->tracker = DistinctTracker_create(memory_context, 4, data);
  state->args_desc = copy_args_desc(args_desc);

  MemoryContextSwitchTo(old_context);
  return &state->base;
}

static Datum count_distinct_finalize(AnonAggState *base_state, Bucket *bucket, BucketDescriptor *bucket_desc, bool *is_null)
{
  bool is_global = bucket_desc->num_labels == 0;
  int64 min_count = is_global ? 0 : g_config.low_count_min_threshold;
  CountDistinctResult result = count_distinct_calculate_final(base_state, bucket, bucket_desc);
  return Int64GetDatum(Max(result.noisy_count, min_count));
}

static void count_distinct_merge(AnonAggState *dst_base_state, const AnonAggState *src_base_state)
{
  CountDistinctState *dst_state = (CountDistinctState *)dst_base_state;
  const CountDistinctState *src_state = (const CountDistinctState *)src_base_state;

  Assert(dst_state->args_desc->num_args == src_state->args_desc->num_args);
  Assert(0 == memcmp(dst_state->args_desc,
                     src_state->args_desc,
                     sizeof(ArgsDescriptor) + dst_state->args_desc->num_args * sizeof(ArgDescriptor)));
  Assert(DATA(dst_state->tracker)->typbyval == DATA(src_state->tracker)->typbyval);
  Assert(DATA(dst_state->tracker)->typlen == DATA(src_state->tracker)->typlen);

  int aids_count = dst_state->args_desc->num_args - AIDS_OFFSET;
  MemoryContext old_context = MemoryContextSwitchTo(dst_base_state->memory_context);

  DistinctTrackerHashEntry *src_entry;
  foreach_entry(src_entry, src_state->tracker, DistinctTracker)
  {
    DistinctTrackerHashEntry *dst_entry =
        get_distinct_tracker_entry(dst_state->tracker, src_entry->value, aids_count);

    ListCell *dst_cell = NULL;
    const ListCell *src_cell = NULL;
    forboth(dst_cell, dst_entry->aid_values_sets, src_cell, src_entry->aid_values_sets)
    {
      List **dst_aid_values_set = (List **)&lfirst(dst_cell);
      const List **src_aid_values_set = (const List **)&lfirst(src_cell);
      *dst_aid_values_set = hash_set_union(*dst_aid_values_set, *src_aid_values_set);
    }
  }

  MemoryContextSwitchTo(old_context);
}

static const char *count_distinct_explain(const AnonAggState *base_state)
{
  return "diffix.anon_count_distinct";
}

static List *add_aid_value_to_set(List *aid_values_set, NullableDatum aid_arg, Oid aid_type)
{
  if (!aid_arg.isnull)
  {
    aid_t aid_value = get_aid_mapper(aid_type)(aid_arg.value);
    aid_values_set = hash_set_add(aid_values_set, aid_value);
  }
  return aid_values_set;
}

static void count_distinct_transition(AnonAggState *base_state, int num_args, NullableDatum *args)
{
  CountDistinctState *state = (CountDistinctState *)base_state;

  Assert(num_args > AIDS_OFFSET);
  int aids_count = num_args - AIDS_OFFSET;
  MemoryContext old_context = MemoryContextSwitchTo(base_state->memory_context);

  if (!args[VALUE_INDEX].isnull)
  {
    Datum value = args[VALUE_INDEX].value;
    DistinctTrackerHashEntry *entry = get_distinct_tracker_entry(state->tracker, value, aids_count);

    ListCell *cell;
    foreach (cell, entry->aid_values_sets)
    {
      int aid_index = foreach_current_index(cell) + AIDS_OFFSET;
      Oid aid_type = state->args_desc->args[aid_index].type_oid;
      List **aid_values_set = (List **)&lfirst(cell);
      *aid_values_set = add_aid_value_to_set(*aid_values_set, args[aid_index], aid_type);
    }
  }

  MemoryContextSwitchTo(old_context);
}

const AnonAggFuncs g_count_distinct_funcs = {
    .final_type = count_distinct_final_type,
    .create_state = count_distinct_create_state,
    .transition = count_distinct_transition,
    .finalize = count_distinct_finalize,
    .merge = count_distinct_merge,
    .explain = count_distinct_explain,
};

static void count_distinct_noise_final_type(const ArgsDescriptor *args_desc, Oid *type, int32 *typmod, Oid *collid)
{
  *type = FLOAT8OID;
  *typmod = -1;
  *collid = 0;
}

static Datum count_distinct_noise_finalize(AnonAggState *base_state, Bucket *bucket, BucketDescriptor *bucket_desc, bool *is_null)
{
  CountDistinctResult result = count_distinct_calculate_final(base_state, bucket, bucket_desc);
  if (result.not_enough_aid_values)
  {
    *is_null = true;
    return Float8GetDatum(0.0);
  }
  else
  {
    return Float8GetDatum(result.noise_sd);
  }
}

static const char *count_distinct_noise_explain(const AnonAggState *base_state)
{
  return "diffix.anon_count_distinct_noise";
}

const AnonAggFuncs g_count_distinct_noise_funcs = {
    .final_type = count_distinct_noise_final_type,
    .create_state = count_distinct_create_state,
    .transition = count_distinct_transition,
    .finalize = count_distinct_noise_finalize,
    .merge = count_distinct_merge,
    .explain = count_distinct_noise_explain,
};
