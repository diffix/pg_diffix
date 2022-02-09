#include "postgres.h"
#include "utils/fmgrtab.h"

#include "pg_diffix/oid_cache.h"
#include "pg_diffix/utils.h"
#include "pg_diffix/query/allowed_functions.h"

static const char *const g_allowed_builtins[] = {
    /* casts */
    "i2tod", "i2tof", "dtoi2", "ftoi2", "ftod", "dtof", "i2toi4", "i4toi2", "i4tod", "dtoi4", "i4tof", "ftoi4", "i8tod", "dtoi8", "i8tof", "ftoi8",
    "int4_numeric", "float4_numeric", "float8_numeric", "numeric_int4", "numeric_float4", "numeric_float8",
    /* substring */
    "text_substr", "text_substr_no_len", "bytea_substr", "bytea_substr_no_len",
    /* numeric generalization */
    "dround", "numeric_round", "dceil", "numeric_ceil", "dfloor", "numeric_floor",
    /* width_bucket */
    "width_bucket_float8", "width_bucket_numeric"};

/* Some allowed functions don't appear in the builtins catalog, so we must allow them manually by OID. */
#define F_NUMERIC_ROUND_INT 1708
static const Oid g_allowed_builtins_extra[] = {F_NUMERIC_ROUND_INT};

/* These are filled at runtime. Array of const pointers to const values. */
static const Oid *const g_allowed_udfs[] = {
    &g_oid_cache.round_by_nn,
    &g_oid_cache.round_by_dd,
    &g_oid_cache.ceil_by_nn,
    &g_oid_cache.ceil_by_dd,
    &g_oid_cache.floor_by_nn,
    &g_oid_cache.floor_by_dd,
};

/* Taken from fmgr.c, which has it static too. See there for original comments */
static const FmgrBuiltin *fmgr_isbuiltin(Oid id)
{
  if (id > fmgr_last_builtin_oid)
    return NULL;

  uint16 index = fmgr_builtin_oid_index[id];
  if (index == InvalidOidBuiltinMapping)
    return NULL;

  return &fmgr_builtins[index];
}

bool is_allowed_function(Oid funcoid)
{
  for (int i = 0; i < ARRAY_LENGTH(g_allowed_udfs); i++)
  {
    if (*g_allowed_udfs[i] == funcoid)
      return true;
  }

  const FmgrBuiltin *fmgr_builtin = fmgr_isbuiltin(funcoid);
  if (fmgr_builtin != NULL)
  {
    for (int i = 0; i < ARRAY_LENGTH(g_allowed_builtins); i++)
    {
      if (strcmp(g_allowed_builtins[i], fmgr_builtin->funcName) == 0)
        return true;
    }
  }

  for (int i = 0; i < ARRAY_LENGTH(g_allowed_builtins_extra); i++)
  {
    if (g_allowed_builtins_extra[i] == funcoid)
      return true;
  }

  DEBUG_LOG("Rejecting usage of function %u.", funcoid);
  return false;
}