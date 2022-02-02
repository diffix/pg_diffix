#include "postgres.h"
#include "utils/fmgroids.h"

#include "pg_diffix/query/allowed_functions.h"
#include "pg_diffix/utils.h"
#include "pg_diffix/oid_cache.h"

// The OIDs for builtin functions which we're using to define `g_allowed_functions` come from `fmgroids.h`. That list
// misses some aliases which we define manually here:
// **NOTE** we use PostgreSQL version 13 as reference on how to call these
#define F_TEXT_SUBSTR_ALIAS 936
#define F_TEXT_SUBSTR_NO_LEN_ALIAS 937
#define F_BYTEA_SUBSTR_ALIAS 2085
#define F_BYTEA_SUBSTR_NO_LEN_ALIAS 2086

// the difference between 13 and 14 is only the names of the `#define`s which point to the same OIDs
#if PG_MAJORVERSION_NUM == 13

#define F_DROUND_INT 1342
#define F_NUMERIC_ROUND_INT 1708
#define F_NUMERIC_CEILING 2167
#define F_DCEILING 2320

static const Oid g_allowed_builtins[] = {
    // casts
    F_I2TOD, F_I2TOF, F_DTOI2, F_FTOI2, F_FTOD, F_DTOF,
    F_I2TOI4, F_I4TOI2, F_I4TOD, F_DTOI4,
    F_I4TOF, F_FTOI4, F_I8TOD, F_DTOI8, F_I8TOF, F_FTOI8,
    F_INT4_NUMERIC, F_FLOAT4_NUMERIC, F_FLOAT8_NUMERIC, F_NUMERIC_INT4, F_NUMERIC_FLOAT4, F_NUMERIC_FLOAT8,
    // substring
    F_TEXT_SUBSTR, F_TEXT_SUBSTR_NO_LEN, F_BYTEA_SUBSTR, F_BYTEA_SUBSTR_NO_LEN,
    // substr (alias for substring)
    F_TEXT_SUBSTR_ALIAS, F_TEXT_SUBSTR_NO_LEN_ALIAS, F_BYTEA_SUBSTR_ALIAS, F_BYTEA_SUBSTR_NO_LEN_ALIAS,
    // numeric generalization
    F_DROUND, F_DROUND_INT, F_NUMERIC_ROUND, F_NUMERIC_ROUND_INT,
    F_DCEIL, F_DCEILING, F_NUMERIC_CEIL, F_NUMERIC_CEILING,
    F_DFLOOR, F_NUMERIC_FLOOR,
    // width_bucket
    F_WIDTH_BUCKET_FLOAT8, F_WIDTH_BUCKET_NUMERIC};

#elif PG_MAJORVERSION_NUM >= 14
static const Oid g_allowed_builtins[] = {
    // casts
    F_FLOAT8_INT2, F_FLOAT4_INT2, F_INT2_FLOAT8, F_INT2_FLOAT4, F_FLOAT8_FLOAT4, F_FLOAT4_FLOAT8,
    F_INT4_INT2, F_INT2_INT4, F_FLOAT8_INT4, F_INT4_FLOAT8,
    F_FLOAT4_INT4, F_INT4_FLOAT4, F_FLOAT8_INT8, F_INT8_FLOAT8, F_FLOAT4_INT8, F_INT8_FLOAT4,
    F_NUMERIC_INT4, F_NUMERIC_FLOAT4, F_NUMERIC_FLOAT8, F_INT4_NUMERIC, F_FLOAT4_NUMERIC, F_FLOAT8_NUMERIC,
    // substring
    F_SUBSTR_TEXT_INT4_INT4, F_SUBSTR_TEXT_INT4, F_SUBSTRING_BYTEA_INT4_INT4, F_SUBSTRING_BYTEA_INT4,
    // substr (alias for substring)
    F_TEXT_SUBSTR_ALIAS, F_TEXT_SUBSTR_NO_LEN_ALIAS, F_BYTEA_SUBSTR_ALIAS, F_BYTEA_SUBSTR_NO_LEN_ALIAS,
    // numeric generalization
    F_DROUND, F_ROUND_NUMERIC, F_ROUND_NUMERIC_INT4, F_ROUND_FLOAT8,
    F_CEIL_FLOAT8, F_CEILING_FLOAT8, F_CEIL_NUMERIC, F_CEILING_NUMERIC,
    F_FLOOR_FLOAT8, F_FLOOR_NUMERIC,
    // width_bucket
    F_WIDTH_BUCKET_FLOAT8_FLOAT8_FLOAT8_INT4, F_WIDTH_BUCKET_NUMERIC_NUMERIC_NUMERIC_INT4};
#endif

/* These are filled at runtime. Array of const pointers to const values. */
static const Oid *const g_allowed_udfs[] = {
    &g_oid_cache.round_by_nn,
    &g_oid_cache.round_by_dd,
    &g_oid_cache.ceil_by_nn,
    &g_oid_cache.ceil_by_dd,
    &g_oid_cache.floor_by_nn,
    &g_oid_cache.floor_by_dd,
};

bool is_allowed_function(Oid funcoid)
{
  for (int i = 0; i < ARRAY_LENGTH(g_allowed_builtins); i++)
  {
    if (g_allowed_builtins[i] == funcoid)
      return true;
  }

  for (int i = 0; i < ARRAY_LENGTH(g_allowed_udfs); i++)
  {
    if (*g_allowed_udfs[i] == funcoid)
      return true;
  }

  DEBUG_LOG("Rejecting usage of function %u.", funcoid);
  return false;
}
