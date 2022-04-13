#include "postgres.h"

#include "common/sha2.h"
#include "fmgr.h"
#include "utils/fmgroids.h"

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
  if (pg_cryptohash_update(hash_state, data, data_len) < 0)
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

  if (!PG_ARGISNULL(1))
  {
    char *tuple_str = OidOutputFunctionCall(F_RECORD_OUT, PG_GETARG_DATUM(1));
    hash_state_update(hash_state, (const uint8 *)tuple_str, strlen(tuple_str));
    pfree(tuple_str);
  }

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
