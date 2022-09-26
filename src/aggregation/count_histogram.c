#include "postgres.h"

#include <math.h>

#include "catalog/pg_type.h"
#include "fmgr.h"
#include "nodes/nodeFuncs.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "pg_diffix/aggregation/aid.h"
#include "pg_diffix/aggregation/aid_tracker.h"
#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/aggregation/noise.h"
#include "pg_diffix/config.h"
#include "pg_diffix/node_funcs.h"
#include "pg_diffix/query/anonymization.h"
#include "pg_diffix/utils.h"

/*-------------------------------------------------------------------------
 * diffix.count_histogram(expr, bin_size)
 *-------------------------------------------------------------------------
 */

/*
 * HashTable<Datum, int64>
 */

typedef struct DatumToInt64Entry
{
  Datum key;
  int64 value;
  char status;
} DatumToInt64Entry;

typedef struct DatumToInt64Data
{
  int16 typlen;
  bool typbyval;
} DatumToInt64Data;

#define SH_PREFIX DatumToInt64
#define SH_ELEMENT_TYPE DatumToInt64Entry
#define SH_KEY key
#define SH_KEY_TYPE Datum
#define DATA(tb) ((DatumToInt64Data *)tb->private_data)
#define SH_EQUAL(tb, a, b) datumIsEqual(a, b, DATA(tb)->typbyval, DATA(tb)->typlen)
#define SH_HASH_KEY(tb, key) (uint32) hash_datum(key, DATA(tb)->typbyval, DATA(tb)->typlen)
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

/*
 * HashTable<int64, int64>
 */

typedef struct Int64ToInt64Entry
{
  int64 key;
  int64 value;
  char status;
} Int64ToInt64Entry;

#define SH_PREFIX Int64ToInt64
#define SH_ELEMENT_TYPE Int64ToInt64Entry
#define SH_KEY key
#define SH_KEY_TYPE int64
#define SH_EQUAL(tb, a, b) (a == b)
#define SH_HASH_KEY(tb, key) (uint32) key
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

PGDLLEXPORT PG_FUNCTION_INFO_V1(count_histogram_transfn);
PGDLLEXPORT PG_FUNCTION_INFO_V1(count_histogram_finalfn);

typedef struct CountHistogramState
{
  DatumToInt64_hash *table;
  int64 bin_size;
} CountHistogramState;

const int STATE_INDEX = 0;
const int VALUE_INDEX = 1;
const int BIN_SIZE_INDEX = 2;

static CountHistogramState *count_histogram_state_new(PG_FUNCTION_ARGS)
{
  MemoryContext agg_context;
  if (!AggCheckCallContext(fcinfo, &agg_context))
    FAILWITH("count_histogram_transfn called in non-aggregate context.");

  int64 bin_size = 1;
  if (PG_NARGS() > 2)
  {
    if (PG_ARGISNULL(BIN_SIZE_INDEX))
      FAILWITH("count_histogram bin_size must not be NULL.");
    bin_size = PG_GETARG_INT64(BIN_SIZE_INDEX);
    if (bin_size < 1)
      FAILWITH("Invalid bin_size for count_histogram.");
  }

  MemoryContext old_context = MemoryContextSwitchTo(agg_context);

  DatumToInt64Data *data = palloc(sizeof(DatumToInt64Data));
  Oid type_oid = get_fn_expr_argtype(fcinfo->flinfo, VALUE_INDEX);
  get_typlenbyval(type_oid, &data->typlen, &data->typbyval);

  CountHistogramState *state = palloc(sizeof(CountHistogramState));
  state->table = DatumToInt64_create(agg_context, 4, data);
  state->bin_size = bin_size;

  MemoryContextSwitchTo(old_context);
  return state;
}

Datum count_histogram_transfn(PG_FUNCTION_ARGS)
{
  CountHistogramState *state;
  if (!PG_ARGISNULL(STATE_INDEX))
    state = (CountHistogramState *)PG_GETARG_POINTER(STATE_INDEX);
  else
    state = count_histogram_state_new(fcinfo);

  if (!PG_ARGISNULL(VALUE_INDEX))
  {
    bool found;
    DatumToInt64Entry *entry = DatumToInt64_insert(state->table, PG_GETARG_DATUM(VALUE_INDEX), &found);
    if (found)
      entry->value++;
    else
      entry->value = 1;
  }

  PG_RETURN_POINTER(state);
}

static int generalize(int value, int bin_size)
{
  return (value / bin_size) * bin_size;
}

static int int64_to_int64_entry_comparer(const ListCell *a, const ListCell *b)
{
  Int64ToInt64Entry *entry_a = (Int64ToInt64Entry *)lfirst(a);
  Int64ToInt64Entry *entry_b = (Int64ToInt64Entry *)lfirst(b);
  return entry_a->key - entry_b->key;
}

Datum count_histogram_finalfn(PG_FUNCTION_ARGS)
{
  if (PG_ARGISNULL(STATE_INDEX))
    /* If there have been no transitions, return empty array directly. */
    PG_RETURN_POINTER(construct_empty_array(INT8OID));

  CountHistogramState *state = (CountHistogramState *)PG_GETARG_POINTER(STATE_INDEX);

  /* Group entries by count. */
  Int64ToInt64_hash *histogram = Int64ToInt64_create(CurrentMemoryContext, 4, NULL);
  {
    DatumToInt64Entry *entry;
    foreach_entry(entry, state->table, DatumToInt64)
    {
      bool found;
      Int64ToInt64Entry *bin = Int64ToInt64_insert(histogram, generalize(entry->value, state->bin_size), &found);
      if (found)
        bin->value++;
      else
        bin->value = 1;
    }
  }

  /* Add bins to a flat list and sort by key. */
  List *bin_list = NIL;
  {
    Int64ToInt64Entry *entry;
    foreach_entry(entry, histogram, Int64ToInt64)
    {
      bin_list = lappend(bin_list, entry);
    }

    list_sort(bin_list, int64_to_int64_entry_comparer);
  }

  /* Prepare data for array construction. */
  int num_bins = list_length(bin_list);
  Datum *elems = palloc(2 * num_bins * sizeof(Datum));
  bool *nulls = NULL; /* A missing nulls array means there can never be nulls. */
  int dims[2] = {num_bins, 2};
  int lower_bounds[2] = {1, 1};
  for (int i = 0; i < num_bins; i++)
  {
    Int64ToInt64Entry *bin = list_nth(bin_list, i);
    elems[2 * i] = Int64GetDatum(bin->key);
    elems[2 * i + 1] = Int64GetDatum(bin->value);
  }

  /* Free temp data. */
  list_free(bin_list);
  Int64ToInt64_destroy(histogram);

  PG_RETURN_POINTER(construct_md_array(
      elems, nulls, 2, dims, lower_bounds, INT8OID, sizeof(int64), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE));
}

/*-------------------------------------------------------------------------
 * diffix.anon_count_histogram(aid_index, bin_size, aids...)
 *-------------------------------------------------------------------------
 */

const int AIDS_OFFSET = 3;

typedef struct CountTracker
{
  int64 count; /* Row count or AID count, depending on context */
  AidTrackerState aid_trackers[FLEXIBLE_ARRAY_MEMBER];
} CountTracker;

/*
 * HashTable<aid_t, AidCountTrackerEntry>
 * Holds intermediate aggregation state.
 */

typedef struct AidCountTrackerEntry
{
  aid_t key;
  CountTracker *data;
  char status;
} AidCountTrackerEntry;

#define SH_PREFIX AidCountTracker
#define SH_ELEMENT_TYPE AidCountTrackerEntry
#define SH_KEY key
#define SH_KEY_TYPE aid_t
#define SH_EQUAL(tb, a, b) (a == b)
#define SH_HASH_KEY(tb, key) (uint32) key /* AID is already a hash, take low bytes. */
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

/*
 * HashTable<int64, HistogramEntry>
 * Used for grouping by count in finalizer.
 */

typedef struct HistogramEntry
{
  int64 key;
  CountTracker *data;
  char status;
} HistogramEntry;

#define SH_PREFIX Histogram
#define SH_ELEMENT_TYPE HistogramEntry
#define SH_KEY key
#define SH_KEY_TYPE int64
#define SH_EQUAL(tb, a, b) (a == b)
#define SH_HASH_KEY(tb, key) (uint32) key
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

typedef struct AnonCountHistogramState
{
  AnonAggState base;
  AidCountTracker_hash *table;
  MapAidFunc *aid_mappers;
  int64 bin_size;
  int32 counted_aid_index; /* 0-based index of counted AID */
  int aid_trackers_count;
} AnonCountHistogramState;

static CountTracker *count_tracker_new(AnonCountHistogramState *state, MemoryContext memory_context)
{
  MemoryContext old_context = MemoryContextSwitchTo(memory_context);

  CountTracker *data = palloc(offsetof(CountTracker, aid_trackers) + state->aid_trackers_count * sizeof(AidTrackerState));
  data->count = 0;
  for (int i = 0; i < state->aid_trackers_count; i++)
  {
    AidTrackerState *aid_tracker = &data->aid_trackers[i];
    aid_tracker_init(aid_tracker, state->aid_mappers[i]);
  }

  MemoryContextSwitchTo(old_context);
  return data;
}

static void count_tracker_merge(CountTracker *dst, const CountTracker *src, int aid_trackers_count)
{
  dst->count += src->count;
  for (int i = 0; i < aid_trackers_count; i++)
    aid_tracker_merge(&dst->aid_trackers[i], &src->aid_trackers[i]);
}

static bool count_tracker_is_low_count(CountTracker *count_tracker, int aid_trackers_count)
{
  bool low_count = false;
  for (int i = 0; i < aid_trackers_count; i++)
  {
    AidTrackerState *aid_tracker = &count_tracker->aid_trackers[i];
    double threshold = generate_lcf_threshold(aid_tracker->aid_seed);
    low_count = low_count || (aid_tracker_naids(aid_tracker) < threshold);
  }
  return low_count;
}

static void count_tracker_finalize(CountTracker *count_tracker, seed_t bucket_seed, int counted_aid_index, int aid_trackers_count)
{
  AidTrackerState *aid_tracker = &count_tracker->aid_trackers[counted_aid_index];
  seed_t noise_layers[] = {bucket_seed, aid_tracker->aid_seed};
  double noise = generate_layered_noise(noise_layers, ARRAY_LENGTH(noise_layers), "count_histogram", g_config.noise_layer_sd);
  int64 noisy_count = (int64)round(aid_tracker_naids(aid_tracker) + noise);
  count_tracker->count = Max(noisy_count, g_config.low_count_min_threshold);
}

static int histogram_entry_comparer(const ListCell *a, const ListCell *b)
{
  HistogramEntry *entry_a = (HistogramEntry *)lfirst(a);
  HistogramEntry *entry_b = (HistogramEntry *)lfirst(b);
  return entry_a->key - entry_b->key;
}

/*
 * Aggregator interface
 */

static void agg_final_type(const ArgsDescriptor *args_desc, Oid *type, int32 *typmod, Oid *collid)
{
  *type = INT8ARRAYOID;
  *typmod = -1;
  *collid = 0;
}

static AnonAggState *agg_create_state(MemoryContext memory_context, ArgsDescriptor *args_desc)
{
  const int COUNTED_AID_ARG = 1;

  MemoryContext old_context = MemoryContextSwitchTo(memory_context);
  AnonCountHistogramState *state = palloc0(sizeof(AnonCountHistogramState));
  int aid_trackers_count = args_desc->num_args - AIDS_OFFSET;

  state->table = AidCountTracker_create(memory_context, 4, NULL);
  state->aid_mappers = palloc(aid_trackers_count * sizeof(MapAidFunc));
  for (int i = 0; i < aid_trackers_count; i++)
    state->aid_mappers[i] = get_aid_mapper(args_desc->args[AIDS_OFFSET + i].type_oid);
  state->counted_aid_index = unwrap_const_int32(args_desc->args[COUNTED_AID_ARG].expr, 0, aid_trackers_count - 1);
  state->bin_size = unwrap_const_int64(args_desc->args[BIN_SIZE_INDEX].expr, 1, INT64_MAX);
  state->aid_trackers_count = aid_trackers_count;

  MemoryContextSwitchTo(old_context);
  return &state->base;
}

static void agg_transition(AnonAggState *base_state, int num_args, NullableDatum *args)
{
  AnonCountHistogramState *state = (AnonCountHistogramState *)base_state;
  /* Rewriter maps the AID expression to its offset in the AID args. */
  int counted_aid_arg_index = AIDS_OFFSET + state->counted_aid_index;

  if (args[counted_aid_arg_index].isnull)
    return;

  aid_t aid = state->aid_mappers[state->counted_aid_index](args[counted_aid_arg_index].value);

  bool found;
  AidCountTrackerEntry *entry = AidCountTracker_insert(state->table, aid, &found);
  if (!found)
    entry->data = count_tracker_new(state, base_state->memory_context);

  entry->data->count++;
  for (int i = 0; i < state->aid_trackers_count; i++)
  {
    int aid_index = i + AIDS_OFFSET;
    if (!args[aid_index].isnull)
    {
      AidTrackerState *aid_tracker = &entry->data->aid_trackers[i];
      aid_t aid = aid_tracker->aid_mapper(args[aid_index].value);
      aid_tracker_update(aid_tracker, aid);
    }
  }
}

static Datum agg_finalize(AnonAggState *base_state, Bucket *bucket, BucketDescriptor *bucket_desc, bool *is_null)
{
  AnonCountHistogramState *state = (AnonCountHistogramState *)base_state;
  int counted_aid_index = state->counted_aid_index;
  int aid_trackers_count = state->aid_trackers_count;
  seed_t bucket_seed = compute_bucket_seed(bucket, bucket_desc);

  MemoryContext temp_context = AllocSetContextCreate(CurrentMemoryContext, "led_hook temporary context", ALLOCSET_DEFAULT_SIZES);
  MemoryContext old_context = MemoryContextSwitchTo(temp_context);

  Histogram_hash *histogram = Histogram_create(temp_context, 4, NULL);

  /* Group entries by count. */
  AidCountTrackerEntry *state_entry;
  foreach_entry(state_entry, state->table, AidCountTracker)
  {
    int bin_label = generalize(state_entry->data->count, state->bin_size);
    bool found;
    HistogramEntry *histogram_entry = Histogram_insert(histogram, bin_label, &found);
    if (!found)
      histogram_entry->data = count_tracker_new(state, temp_context);

    /* Add AIDs to histogram bin. We don't care about the `count` field yet. */
    count_tracker_merge(histogram_entry->data, state_entry->data, aid_trackers_count);
  }

  CountTracker *suppress_bin = count_tracker_new(state, temp_context);

  /* Add high-count bins to a flat list and sort by key. */
  List *bin_list = NIL;
  int low_count_bins = 0;
  HistogramEntry *histogram_entry;
  foreach_entry(histogram_entry, histogram, Histogram)
  {
    if (!count_tracker_is_low_count(histogram_entry->data, aid_trackers_count))
    {
      count_tracker_finalize(histogram_entry->data, bucket_seed, counted_aid_index, aid_trackers_count);
      bin_list = lappend(bin_list, histogram_entry);
    }
    else
    {
      count_tracker_merge(suppress_bin, histogram_entry->data, aid_trackers_count);
      low_count_bins++;
    }
  }

  list_sort(bin_list, histogram_entry_comparer);
  bool include_suppress_bin =
      low_count_bins >= 2 && !count_tracker_is_low_count(suppress_bin, aid_trackers_count);

  if (include_suppress_bin)
    count_tracker_finalize(suppress_bin, bucket_seed, counted_aid_index, aid_trackers_count);

  /*
   * Prepare data for array construction.
   * Keep input data in temp_context because everything will be memcopied anyway.
   */

  int suppress_bin_offset = (include_suppress_bin ? 1 : 0);
  int num_regular_bins = list_length(bin_list);
  int num_bins = suppress_bin_offset + num_regular_bins;
  Datum *elems = palloc(2 * num_bins * sizeof(Datum));
  int dims[2] = {num_bins, 2};
  int lower_bounds[2] = {1, 1};
  bool *nulls = NULL;
  if (include_suppress_bin)
  {
    nulls = palloc0(2 * num_bins * sizeof(bool));
    nulls[0] = true;                               /* Only suppress bin label is null. */
    elems[0] = Int64GetDatum(0);                   /* Suppress bin label (null). */
    elems[1] = Int64GetDatum(suppress_bin->count); /* Suppress bin value. */
  }

  for (int i = 0; i < num_regular_bins; i++)
  {
    HistogramEntry *bin = list_nth(bin_list, i);
    int dst = suppress_bin_offset + i;
    elems[2 * dst] = Int64GetDatum(bin->key);
    elems[2 * dst + 1] = Int64GetDatum(bin->data->count);
  }

  MemoryContextSwitchTo(old_context);

  ArrayType *array = construct_md_array(
      elems, nulls, 2, dims, lower_bounds, INT8OID, sizeof(int64), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);

  MemoryContextDelete(temp_context);
  PG_RETURN_POINTER(array);
}

static void agg_merge(AnonAggState *dst_base_state, const AnonAggState *src_base_state)
{
  AnonCountHistogramState *dst_state = (AnonCountHistogramState *)dst_base_state;
  const AnonCountHistogramState *src_state = (const AnonCountHistogramState *)src_base_state;

  Assert(dst_state->aid_trackers_count == src_state->aid_trackers_count);
  Assert(dst_state->counted_aid_index == src_state->counted_aid_index);

  AidCountTrackerEntry *src_entry;
  foreach_entry(src_entry, src_state->table, AidCountTracker)
  {
    bool found;
    AidCountTrackerEntry *dst_entry = AidCountTracker_insert(dst_state->table, src_entry->key, &found);
    if (!found)
      dst_entry->data = count_tracker_new(dst_state, dst_base_state->memory_context);

    count_tracker_merge(dst_entry->data, src_entry->data, dst_state->aid_trackers_count);
  }
}

static const char *agg_explain(const AnonAggState *base_state)
{
  return "diffix.anon_count_histogram";
}

const AnonAggFuncs g_count_histogram_funcs = {
    .final_type = agg_final_type,
    .create_state = agg_create_state,
    .transition = agg_transition,
    .finalize = agg_finalize,
    .merge = agg_merge,
    .explain = agg_explain,
};
