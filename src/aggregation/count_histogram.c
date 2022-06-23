#include "postgres.h"

#include "catalog/pg_type.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/lsyscache.h"

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

PG_FUNCTION_INFO_V1(count_histogram_transfn);
PG_FUNCTION_INFO_V1(count_histogram_finalfn);

typedef struct CountHistogramState
{
  DatumToInt64_hash *table;
  int64 bin_size;
} CountHistogramState;

const int STATE_INDEX = 0;
const int VALUE_INDEX = 1;
const int BIN_SIZE_INDEX = 2;

Datum count_histogram_transfn(PG_FUNCTION_ARGS)
{
  CountHistogramState *state;
  if (!PG_ARGISNULL(STATE_INDEX))
  {
    state = (CountHistogramState *)PG_GETARG_POINTER(STATE_INDEX);
  }
  else
  {
    MemoryContext agg_context;
    if (!AggCheckCallContext(fcinfo, &agg_context))
      FAILWITH("count_histogram_transfn called in non-aggregate context.");

    int bin_size = 1;
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

    state = palloc(sizeof(CountHistogramState));
    state->table = DatumToInt64_create(agg_context, 4, data);
    state->bin_size = bin_size;

    MemoryContextSwitchTo(old_context);
  }

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

static int histogram_entries_comparer(const ListCell *a, const ListCell *b)
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

  /* Do the inverse grouping by count. */
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

    list_sort(bin_list, histogram_entries_comparer);
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

  PG_RETURN_POINTER(construct_md_array(elems, nulls, 2, dims, lower_bounds, INT8OID, sizeof(int64), true, TYPALIGN_DOUBLE));
}
