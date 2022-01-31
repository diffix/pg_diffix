#include "postgres.h"
#include "utils/fmgroids.h"

#include "pg_diffix/query/allowed_functions.h"

// The OIDs for builtin functions which we're using to define `g_allowed_functions` come from `fmgroids.h`. That list
// misses some aliases which we define manually here:
#define F_TEXT_SUBSTR_ALIAS 936
#define F_TEXT_SUBSTR_NO_LEN_ALIAS 937
#define F_BYTEA_SUBSTR_ALIAS 2085
#define F_BYTEA_SUBSTR_NO_LEN_ALIAS 2086

static const Oid g_allowed_functions[] = {
    // casts
    F_I2TOD, F_I2TOF, F_DTOI2, F_FTOI2, F_FTOD, F_DTOF, F_I2TOI4, F_I4TOI2, F_I4TOD, F_DTOI4,
    F_I4TOF, F_FTOI4, F_I8TOD, F_DTOI8, F_I8TOF, F_FTOI8,
    F_INT4_NUMERIC, F_FLOAT4_NUMERIC, F_FLOAT8_NUMERIC, F_NUMERIC_INT4, F_NUMERIC_FLOAT4, F_NUMERIC_FLOAT8,
    // substring
    F_TEXT_SUBSTR, F_TEXT_SUBSTR_NO_LEN, F_BYTEA_SUBSTR, F_BYTEA_SUBSTR_NO_LEN,
    // substr (alias for substring)
    F_TEXT_SUBSTR_ALIAS, F_TEXT_SUBSTR_NO_LEN_ALIAS, F_BYTEA_SUBSTR_ALIAS, F_BYTEA_SUBSTR_NO_LEN_ALIAS,
    // width_bucket
    F_WIDTH_BUCKET_FLOAT8, F_WIDTH_BUCKET_NUMERIC};

static size_t g_allowed_functions_length = sizeof(g_allowed_functions) / sizeof(g_allowed_functions[0]);

bool is_allowed_function(Oid funcoid)
{
  for (int i = 0; i < g_allowed_functions_length; i++)
  {
    if (g_allowed_functions[i] == funcoid)
      return true;
  }
  return false;
}
