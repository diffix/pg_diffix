#include "postgres.h"

#include "access/htup.h"
#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "common/sha2.h"
#include "fmgr.h"
#include "nodes/nodes.h"
#include "utils/builtins.h"
#include "utils/typcache.h"

#include "pg_diffix/utils.h"

PG_FUNCTION_INFO_V1(hash_record_transfn);
PG_FUNCTION_INFO_V1(hash_record_finalfn);

#if PG_MAJORVERSION_NUM == 13

typedef pg_sha256_ctx HashState;
#define HASH_SIZE PG_SHA256_DIGEST_LENGTH

static HashState *hash_state_new(PG_FUNCTION_ARGS)
{
  pg_sha256_ctx *hash_state = palloc(sizeof(pg_sha256_ctx));
  pg_sha256_init(hash_state);
  return hash_state;
}

static inline void hash_state_update(HashState *hash_state, const uint8 *data, size_t data_len)
{
  pg_sha256_update(hash_state, data, data_len);
}

static inline void hash_state_final(HashState *hash_state, uint8 *hash_dest)
{
  pg_sha256_final(hash_state, hash_dest);
}

#else

#include "common/cryptohash.h"

typedef pg_cryptohash_ctx HashState;
#define HASH_SIZE PG_SHA256_DIGEST_LENGTH

static void hash_state_shutdown(Datum arg)
{
  HashState *hash_state = (HashState *)DatumGetPointer(arg);
  pg_cryptohash_free(hash_state);
}

static HashState *hash_state_new(PG_FUNCTION_ARGS)
{
  pg_cryptohash_ctx *hash_state = pg_cryptohash_create(PG_SHA256);

  if (hash_state == NULL)
    FAILWITH("Failed creating hash function.");

  if (pg_cryptohash_init(hash_state) < 0)
    FAILWITH("Failed initializing hash function.");

  /* Register a shutdown callback to clean things up at end of group. */
  AggRegisterCallback(fcinfo,
                      hash_state_shutdown,
                      PointerGetDatum(hash_state));

  return hash_state;
}

static inline void hash_state_update(HashState *hash_state, const uint8 *data, size_t data_len)
{
  if (unlikely(pg_cryptohash_update(hash_state, data, data_len) < 0))
    FAILWITH("Failed updating hash function.");
}

static inline void hash_state_final(HashState *hash_state, uint8 *hash_dest)
{
  if (pg_cryptohash_final(hash_state, hash_dest, HASH_SIZE) < 0)
    FAILWITH("Failed finalizing hash function.");
}

#endif

static HashState *get_hash_state(PG_FUNCTION_ARGS)
{
  if (!PG_ARGISNULL(0))
    return (HashState *)PG_GETARG_POINTER(0);

  MemoryContext agg_context;
  if (AggCheckCallContext(fcinfo, &agg_context) != AGG_CONTEXT_AGGREGATE)
    FAILWITH("Aggregate called in non-aggregate context");

  MemoryContext old_context = MemoryContextSwitchTo(agg_context);
  HashState *hash_state = hash_state_new(fcinfo);
  MemoryContextSwitchTo(old_context);

  return hash_state;
}

Datum hash_record_transfn(PG_FUNCTION_ARGS)
{
  HashState *hash_state = get_hash_state(fcinfo);

  HeapTupleHeader tuple = PG_GETARG_HEAPTUPLEHEADER(1);
  Oid tuple_type = HeapTupleHeaderGetTypeId(tuple);
  int32 tuple_typmod = HeapTupleHeaderGetTypMod(tuple);
  TupleDesc tuple_desc = lookup_rowtype_tupdesc(tuple_type, tuple_typmod);

  HeapTupleData tuple_data;
  tuple_data.t_len = HeapTupleHeaderGetDatumLength(tuple);
  ItemPointerSetInvalid(&(tuple_data.t_self));
  tuple_data.t_tableOid = InvalidOid;
  tuple_data.t_data = tuple;

  for (int i = 0; i < tuple_desc->natts; i++)
  {
    bool is_null = false;
    Datum datum = heap_getattr(&tuple_data, i + 1, tuple_desc, &is_null);

    if (is_null)
      continue;

    if (TupleDescAttr(tuple_desc, i)->attbyval)
    {
      hash_state_update(hash_state, (const uint8 *)&datum, sizeof(Datum));
    }
    else
    {
      int16 att_len = TupleDescAttr(tuple_desc, i)->attlen;

      /* We want to make sure data is detoasted to get reliable results. */
      if (att_len == -1)
        datum = (Datum)PG_DETOAST_DATUM(datum);

      size_t data_len = datumGetSize(datum, false, att_len);
      hash_state_update(hash_state, (const uint8 *)datum, data_len);
    }
  }

  ReleaseTupleDesc(tuple_desc);
  PG_RETURN_POINTER(hash_state);
}

Datum hash_record_finalfn(PG_FUNCTION_ARGS)
{
  HashState *hash_state = get_hash_state(fcinfo);

  /* Initialize varlen datum. */
  bytea *result = palloc(VARHDRSZ + HASH_SIZE);
  SET_VARSIZE(result, HASH_SIZE);

  /* Skip 4 first bytes and store hash. */
  uint8 *hash_dest = ((uint8 *)result) + VARHDRSZ;
  hash_state_final(hash_state, hash_dest);

  PG_RETURN_BYTEA_P(result);
}
