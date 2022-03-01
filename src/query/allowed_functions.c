#include "postgres.h"

#include "utils/fmgrtab.h"

#include "pg_diffix/oid_cache.h"
#include "pg_diffix/query/allowed_functions.h"
#include "pg_diffix/utils.h"

static const char *const g_allowed_casts[] = {
    "i2tod", "i2tof", "i2toi4", "i4toi2", "i4tod", "i4tof", "i8tod", "i8tof",
    "ftod", "dtof",
    "int4_numeric", "float4_numeric", "float8_numeric",
    "numeric_float4", "numeric_float8",
    /**/
};

static const char *const g_allowed_builtins[] = {
    /* rounding casts */
    "ftoi2", "ftoi4", "ftoi8", "dtoi2", "dtoi4", "dtoi8", "numeric_int4",
    /* substring */
    "text_substr", "text_substr_no_len", "bytea_substr", "bytea_substr_no_len",
    /* numeric generalization */
    "dround", "numeric_round", "dceil", "numeric_ceil", "dfloor", "numeric_floor",
    /* width_bucket */
    "width_bucket_float8", "width_bucket_numeric",
    /**/
};

static const char *const g_substring_builtins[] = {
    "text_substr", "text_substr_no_len", "bytea_substr", "bytea_substr_no_len"
    /**/
};

static const char *const g_builtin_floor[] = {
    "dfloor", "numeric_floor"
    /**/
};

/* Some allowed functions don't appear in the builtins catalog, so we must allow them manually by OID. */
#define F_NUMERIC_ROUND_INT 1708
static const Oid g_allowed_builtins_extra[] = {F_NUMERIC_ROUND_INT};

/* Pointers to OID cache which is populated at runtime. */
static const Oid *const g_allowed_udfs[] = {
    &g_oid_cache.round_by_nn,
    &g_oid_cache.round_by_dd,
    &g_oid_cache.ceil_by_nn,
    &g_oid_cache.ceil_by_dd,
    &g_oid_cache.floor_by_nn,
    &g_oid_cache.floor_by_dd,
};

/* Taken from fmgr.c. */
static const FmgrBuiltin *fmgr_isbuiltin(Oid id)
{
  if (id > fmgr_last_builtin_oid)
    return NULL;

  uint16 index = fmgr_builtin_oid_index[id];
  if (index == InvalidOidBuiltinMapping)
    return NULL;

  return &fmgr_builtins[index];
}

static bool is_funcname_member_of(Oid funcoid, const char *const name_array[], int length)
{
  const FmgrBuiltin *fmgr_builtin = fmgr_isbuiltin(funcoid);
  if (fmgr_builtin != NULL)
  {
    for (int i = 0; i < length; i++)
    {
      if (strcmp(name_array[i], fmgr_builtin->funcName) == 0)
        return true;
    }
  }

  return false;
}

bool is_allowed_cast(Oid funcoid)
{
  return is_funcname_member_of(funcoid, g_allowed_casts, ARRAY_LENGTH(g_allowed_casts));
}

bool is_allowed_function(Oid funcoid)
{
  for (int i = 0; i < ARRAY_LENGTH(g_allowed_udfs); i++)
  {
    if (*g_allowed_udfs[i] == funcoid)
      return true;
  }

  if (is_funcname_member_of(funcoid, g_allowed_builtins, ARRAY_LENGTH(g_allowed_builtins)))
    return true;

  for (int i = 0; i < ARRAY_LENGTH(g_allowed_builtins_extra); i++)
  {
    if (g_allowed_builtins_extra[i] == funcoid)
      return true;
  }

  DEBUG_LOG("Rejecting usage of function %u.", funcoid);
  return false;
}

bool is_substring(Oid funcoid)
{
  return is_funcname_member_of(funcoid, g_substring_builtins, ARRAY_LENGTH(g_substring_builtins));
}

bool is_builtin_floor(Oid funcoid)
{
  return is_funcname_member_of(funcoid, g_builtin_floor, ARRAY_LENGTH(g_builtin_floor));
}
