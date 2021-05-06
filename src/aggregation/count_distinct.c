#include "postgres.h"
#include "fmgr.h"
#include "utils/datum.h"
#include "nodes/pg_list.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include <math.h>
#include <inttypes.h>

#include "pg_diffix/utils.h"
#include "pg_diffix/config.h"
#include "pg_diffix/aggregation/aid.h"
#include "pg_diffix/aggregation/random.h"

static int32 hash_datum(Datum value, bool typbyval, int16 typlen)
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
    data_size = datumGetSize(value, typbyval, typlen);
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
  List *aidvs; /* AID value sets for the unique value */
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
static const int AIDS_INDEX = 2;

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
get_distinct_tracker_entry(DistinctTracker_hash *tracker, Datum value, int aidvs_count)
{
  bool found = false;
  DistinctTrackerHashEntry *entry = DistinctTracker_insert(tracker, value, &found);
  if (!found)
  {
    entry->aidvs = NIL;
    entry->value = value;
    for (int i = 0; i < aidvs_count; i++)
    {
      entry->aidvs = lappend(entry->aidvs, NIL);
    }
  }
  return entry;
}

static List *add_aid_to_set(List *aidv, aid_t aid)
{
  int max_size = g_config.minimum_allowed_aid_values + LCF_RANGE + 1;
  if (list_length(aidv) == max_size) // set is full, value is not low-count
    return aidv;
  return list_append_unique_ptr(aidv, (void *)aid);
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
    Assert(PG_NARGS() > AIDS_INDEX);

    Datum value = PG_GETARG_DATUM(VALUE_INDEX);
    int aidvs_count = PG_NARGS() - AIDS_INDEX;
    DistinctTrackerHashEntry *entry = get_distinct_tracker_entry(tracker, value, aidvs_count);

    ListCell *lc;
    foreach (lc, entry->aidvs)
    {
      int aid_index = foreach_current_index(lc) + AIDS_INDEX;
      if (!PG_ARGISNULL(aid_index))
      {
        Oid aid_type = get_fn_expr_argtype(fcinfo->flinfo, aid_index);
        aid_t aid = get_aid_descriptor(aid_type).make_aid(PG_GETARG_DATUM(aid_index));
        List **aidv = (List **)&lfirst(lc); // pointer to the set of AID values
        *aidv = add_aid_to_set(*aidv, aid);
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

static CountDistinctResult count_distinct_calculate_final(DistinctTracker_hash *state);

Datum anon_count_distinct_finalfn(PG_FUNCTION_ARGS)
{
  /* We want all memory allocations to be done per aggregation node. */
  MemoryContext old_context = switch_to_aggregation_context(fcinfo);

  DistinctTracker_hash *tracker = get_distinct_tracker(fcinfo);
  CountDistinctResult result = count_distinct_calculate_final(tracker);

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

  DistinctTracker_hash *tracker = get_distinct_tracker(fcinfo);
  CountDistinctResult result = count_distinct_calculate_final(tracker);

  MemoryContextSwitchTo(old_context);

  StringInfoData string;
  initStringInfo(&string);

  appendStringInfo(&string, "hc_values=%" PRIi64 ", lc_values=%" PRIi64 ", noisy_count=%" PRIi64,
                   result.hc_values_count, result.lc_values_count, result.noisy_count);

  PG_RETURN_TEXT_P(cstring_to_text(string.data));
}

static uint64 seed_from_aidv(const List *aidv)
{
  uint64 seed = 0;
  ListCell *lc;
  foreach (lc, aidv)
  {
    aid_t aid = (aid_t)lfirst(lc);
    seed ^= aid;
  }
  return make_seed(seed);
}

static bool aid_set_is_high_count(const List *aidv)
{
  int max_size = g_config.minimum_allowed_aid_values + LCF_RANGE + 1;
  if (list_length(aidv) == max_size) // set is full, value is not low-count
    return true;
  uint64 seed = seed_from_aidv(aidv);
  int threshold = generate_lcf_threshold(&seed);
  return list_length(aidv) >= threshold;
}

static bool aid_sets_are_high_count(const List *aidvs)
{
  ListCell *lc;
  foreach (lc, aidvs)
  {
    const List *aidv = (const List *)lfirst(lc);
    if (!aid_set_is_high_count(aidv))
      return false;
  }
  return true;
}

/*
 * The number of high count values is safe to be shown directly, without any extra noise.
 * The number of low count values has to be anonymized.
 */
static CountDistinctResult count_distinct_calculate_final(DistinctTracker_hash *tracker)
{
  CountDistinctResult result = {0};

  DistinctTracker_iterator it;
  DistinctTracker_start_iterate(tracker, &it);
  DistinctTrackerHashEntry *entry = NULL;
  while ((entry = DistinctTracker_iterate(tracker, &it)) != NULL)
  {
    if (aid_sets_are_high_count(entry->aidvs))
      result.hc_values_count++;
    else
      result.lc_values_count++;
  }

  result.noisy_count = result.hc_values_count;
  return result;
}
