#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#include <math.h>
#include <inttypes.h>

#include "pg_diffix/config.h"
#include "pg_diffix/utils.h"
#include "pg_diffix/aggregation/aid.h"
#include "pg_diffix/aggregation/random.h"
#include "pg_diffix/aggregation/count.h"

static const bool TYPE_BY_REF = false;

static uint32 hash_datum(Datum value, bool typbyval, int16 typlen)
{
  const void *data = NULL;
  size_t data_size = 0;
  if (typbyval)
  {
    data = &value;
    data_size = sizeof(Datum);
  }
  else
  {
    data = DatumGetPointer(value);
    data_size = datumGetSize(value, TYPE_BY_REF, typlen);
  }
  return hash_bytes(data, data_size);
}

/*
 * For each unique value we encounter, we keep a set of AID values for each AID instance available.
 * We limit the size of an AID value set to the `maximum threshold for low-count buckets` + 1.
 */
typedef struct DistinctTrackerHashEntry
{
  Datum value; /* Unique value */
  List *aidvs; /* List of (hashes of) AID value lists, one for each AID instance */
  char status; /* Required for hash table */
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
#define SH_HASH_KEY(tb, key) hash_datum(key, DATA(tb)->typbyval, DATA(tb)->typlen)
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

static const int STATE_INDEX = 0;
static const int VALUE_INDEX = 1;
static const int AIDS_OFFSET = 2;

static DistinctTracker_hash *get_distinct_tracker(PG_FUNCTION_ARGS)
{
  if (!PG_ARGISNULL(STATE_INDEX))
    return (DistinctTracker_hash *)PG_GETARG_POINTER(STATE_INDEX);

  DistinctTrackerData *data = (DistinctTrackerData *)palloc0(sizeof(DistinctTrackerData));
  Oid value_oid = get_fn_expr_argtype(fcinfo->flinfo, VALUE_INDEX);
  get_typlenbyval(value_oid, &data->typlen, &data->typbyval);

  return DistinctTracker_create(CurrentMemoryContext, 128, data);
}

static DistinctTrackerHashEntry *
get_distinct_tracker_entry(DistinctTracker_hash *tracker, Datum value, int aids_count)
{
  bool found = false;
  DistinctTrackerHashEntry *entry = DistinctTracker_insert(tracker, value, &found);
  if (!found)
  {
    entry->aidvs = NIL;
    entry->value = datumCopy(value, DATA(tracker)->typbyval, DATA(tracker)->typlen);
    for (int i = 0; i < aids_count; i++)
    {
      entry->aidvs = lappend(entry->aidvs, NIL);
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

static void set_value_sorting_globals(PG_FUNCTION_ARGS)
{
  Oid element_type = get_fn_expr_argtype(fcinfo->flinfo, VALUE_INDEX);
  g_compare_values_typentry = lookup_type_cache(element_type, TYPECACHE_CMP_PROC_FINFO);
  g_compare_values_func = &g_compare_values_typentry->cmp_proc_finfo;
}

PG_FUNCTION_INFO_V1(anon_count_distinct_transfn);
PG_FUNCTION_INFO_V1(anon_count_distinct_finalfn);
PG_FUNCTION_INFO_V1(anon_count_distinct_explain_finalfn);

Datum anon_count_distinct_transfn(PG_FUNCTION_ARGS)
{
  /* We want all memory allocations to be done per aggregation node. */
  MemoryContext old_context = switch_to_aggregation_context(fcinfo);

  DistinctTracker_hash *tracker = get_distinct_tracker(fcinfo);

  if (!PG_ARGISNULL(VALUE_INDEX))
  {
    Assert(PG_NARGS() > AIDS_OFFSET);

    Datum value = PG_GETARG_DATUM(VALUE_INDEX);
    int aids_count = PG_NARGS() - AIDS_OFFSET;
    DistinctTrackerHashEntry *entry = get_distinct_tracker_entry(tracker, value, aids_count);

    ListCell *cell;
    foreach (cell, entry->aidvs)
    {
      int aid_index = foreach_current_index(cell) + AIDS_OFFSET;
      if (!PG_ARGISNULL(aid_index))
      {
        Oid aid_type = get_fn_expr_argtype(fcinfo->flinfo, aid_index);
        aid_t aid = get_aid_descriptor(aid_type).make_aid(PG_GETARG_DATUM(aid_index));
        List **aidv = (List **)&lfirst(cell);               // pointer to the set of AID values
        *aidv = list_append_unique_ptr(*aidv, (void *)aid); // add current AID value to the set
      }
    }
  }

  MemoryContextSwitchTo(old_context);

  PG_RETURN_POINTER(tracker);
}

typedef struct CountDistinctResult
{
  int64 hc_values_count;
  int64 lc_values_count;
  int64 noisy_count;
} CountDistinctResult;

static CountDistinctResult count_distinct_calculate_final(DistinctTracker_hash *state, int aids_count);

Datum anon_count_distinct_finalfn(PG_FUNCTION_ARGS)
{
  /* We want all memory allocations to be done per aggregation node. */
  MemoryContext old_context = switch_to_aggregation_context(fcinfo);

  set_value_sorting_globals(fcinfo);

  DistinctTracker_hash *tracker = get_distinct_tracker(fcinfo);
  CountDistinctResult result = count_distinct_calculate_final(tracker, PG_NARGS() - AIDS_OFFSET);

  MemoryContextSwitchTo(old_context);

  if (result.noisy_count == 0)
    PG_RETURN_NULL();
  else
    PG_RETURN_INT64(result.noisy_count);
}

Datum anon_count_distinct_explain_finalfn(PG_FUNCTION_ARGS)
{
  /* We want all memory allocations to be done per aggregation node. */
  MemoryContext old_context = switch_to_aggregation_context(fcinfo);

  set_value_sorting_globals(fcinfo);

  DistinctTracker_hash *tracker = get_distinct_tracker(fcinfo);
  CountDistinctResult result = count_distinct_calculate_final(tracker, PG_NARGS() - AIDS_OFFSET);

  MemoryContextSwitchTo(old_context);

  StringInfoData string;
  initStringInfo(&string);

  appendStringInfo(&string, "hc_values=%" PRIi64 ", lc_values=%" PRIi64 ", noisy_count=%" PRIi64,
                   result.hc_values_count, result.lc_values_count, result.noisy_count);

  PG_RETURN_TEXT_P(cstring_to_text(string.data));
}

static uint64 seed_from_aidv(const List *aidvs)
{
  uint64 seed = 0;
  ListCell *cell;
  foreach (cell, aidvs)
  {
    aid_t aid = (aid_t)lfirst(cell);
    seed ^= aid;
  }
  return make_seed(seed);
}

static bool aid_set_is_high_count(const List *aidvs)
{
  if (list_length(aidvs) < g_config.low_count_min_threshold)
    return false; /* Less AID values than minimum threshold, value is low-count. */
  uint64 seed = seed_from_aidv(aidvs);
  int threshold = generate_lcf_threshold(&seed);
  return list_length(aidvs) >= threshold;
}

static bool aid_sets_are_high_count(const List *aidvs)
{
  ListCell *cell;
  foreach (cell, aidvs)
  {
    const List *aidv = (const List *)lfirst(cell);
    if (!aid_set_is_high_count(aidv))
      return false;
  }
  return true;
}

/* Returns a list with the tracker entries that are low count. */
static List *filter_lc_entries(DistinctTracker_hash *tracker)
{
  List *lc_entries = NIL;

  DistinctTracker_iterator it;
  DistinctTracker_start_iterate(tracker, &it);
  DistinctTrackerHashEntry *entry = NULL;
  while ((entry = DistinctTracker_iterate(tracker, &it)) != NULL)
  {
    if (!aid_sets_are_high_count(entry->aidvs))
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
    PerAidValuesEntry *entry = (PerAidValuesEntry *)lfirst(list_nth_cell(per_aid_values, middle));
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
    const List *aidvs = (const List *)list_nth(entry->aidvs, aid_index);

    if (aidvs != NIL) /* Count unique value only if it has at least one associated AID value. */
      (*lc_values_true_count)++;

    ListCell *aidv_cell;
    foreach (aidv_cell, aidvs)
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

static void delete_value(List *per_aid_values, Datum value)
{
  ListCell *cell;
  foreach (cell, per_aid_values)
  {
    PerAidValuesEntry *entry = (PerAidValuesEntry *)lfirst(cell);
    /* Since values are unique at this point, we can use simple pointer equality even for reference types. */
    entry->values = list_delete_ptr(entry->values, (void *)value);
  }
}

/*
 * Builds the top contributors array from the list of per-AID low-count values.
 * From each AID value in turn, in increasing order of contributions amount, a unique value
 * is counted and removed from all other entries, until all distinct values are exhausted.
 */
static void distribute_lc_values(List *per_aid_values, uint32 values_count)
{
  while (values_count > 0)
  {
    ListCell *cell;
    foreach (cell, per_aid_values)
    {
      PerAidValuesEntry *entry = (PerAidValuesEntry *)lfirst(cell);
      if (entry->values != NIL)
      {
        values_count--;
        delete_value(per_aid_values, (Datum)lfirst(list_tail(entry->values)));
        entry->contributions++;
      }
    }
  }
}

/* Computes the aggregation seed, total count of contributors and fills the top contributors array. */
static void process_lc_values_contributions(
    List *per_aid_values, uint64 *seed, uint64 *contributors_count,
    Contributors *top_contributors)
{
  *contributors_count = 0;
  *seed = 0;

  ListCell *cell;
  foreach (cell, per_aid_values)
  {
    PerAidValuesEntry *entry = (PerAidValuesEntry *)lfirst(cell);
    *seed ^= entry->aid;
    if (entry->contributions > 0)
    {
      Contributor contributor = {.aid = entry->aid, .contribution = {.integer = entry->contributions}};
      add_top_contributor(&count_descriptor, top_contributors, contributor);
      (*contributors_count)++;
    }
  }
}

/*
 * The number of high count values is safe to be shown directly, without any extra noise.
 * The number of low count values has to be anonymized.
 */
static CountDistinctResult count_distinct_calculate_final(DistinctTracker_hash *tracker, int aids_count)
{
  List *lc_entries = filter_lc_entries(tracker);
  list_sort(lc_entries, &compare_tracker_entries_by_value); /* Needed to ensure determinism. */

  CountDistinctResult result = {0};
  result.lc_values_count = list_length(lc_entries);
  result.hc_values_count = tracker->members - result.lc_values_count;
  result.noisy_count = result.hc_values_count;

  uint32 top_contributors_capacity = g_config.outlier_count_max + g_config.top_count_max;

  bool insufficient_data = false;
  CountResultAccumulator result_accumulator = {0};

  for (int aid_index = 0; aid_index < aids_count; aid_index++)
  {
    Contributors *top_contributors = create_contributors(top_contributors_capacity);

    uint32 lc_values_true_count = 0;
    List *per_aid_values = transpose_lc_values_per_aid(lc_entries, aid_index, &lc_values_true_count);

    list_sort(per_aid_values, &compare_per_aid_values_entries);
    distribute_lc_values(per_aid_values, lc_values_true_count);

    uint64 seed = 0;
    uint64 contributors_count = 0;
    process_lc_values_contributions(
        per_aid_values,
        &seed, &contributors_count,
        top_contributors);

    // NOTE: 0 is the unaccounted_for
    CountResult inner_count_result = aggregate_count_contributions(
        seed, lc_values_true_count, contributors_count, 0, top_contributors);

    list_free_deep(per_aid_values);
    pfree(top_contributors);

    if (inner_count_result.not_enough_aidvs)
    {
      insufficient_data = true;
      break;
    }
    accumulate_count_result(&result_accumulator, &inner_count_result);
  }

  if (!insufficient_data)
  {
    result.noisy_count += finalize_count_result(&result_accumulator);
  }

  result.noisy_count = Max(result.noisy_count, g_config.low_count_min_threshold);

  return result;
}
