#include "postgres.h"

#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"

#include "pg_diffix/node_funcs.h"
#include "pg_diffix/utils.h"

int64 unwrap_const_int64(Expr *expr, int64 min_value, int64 max_value)
{
  if (!expr || !IsA(expr, Const))
    FAILWITH_LOCATION(exprLocation((Node *)expr), "Expected a constant integer.");

  Const *const_expr = (Const *)expr;
  if (const_expr->consttype != INT8OID || const_expr->constisnull)
    FAILWITH_LOCATION(exprLocation((Node *)expr), "Expected a constant integer.");

  int64 val = DatumGetInt64(const_expr->constvalue);
  if (val < min_value || val > max_value)
    FAILWITH_LOCATION(exprLocation((Node *)expr), "Value is outside of valid bounds.");

  return val;
}

Expr *make_const_int64(int64 value)
{
  return (Expr *)makeConst(INT8OID, -1, InvalidOid, sizeof(int64), Int64GetDatum(value), false, FLOAT8PASSBYVAL);
}
