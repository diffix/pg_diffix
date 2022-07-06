#include "postgres.h"

#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"

#include "pg_diffix/node_funcs.h"
#include "pg_diffix/query/allowed_objects.h"
#include "pg_diffix/utils.h"

bool is_datetime_to_string_cast(CoerceViaIO *expr)
{
  Node *arg = (Node *)expr->arg;
  return TypeCategory(exprType(arg)) == TYPCATEGORY_DATETIME && TypeCategory(expr->resulttype) == TYPCATEGORY_STRING;
}

Node *unwrap_cast(Node *node)
{
  if (IsA(node, FuncExpr))
  {
    FuncExpr *func_expr = (FuncExpr *)node;
    if (is_allowed_cast(func_expr->funcid))
    {
      Assert(list_length(func_expr->args) == 1); /* All allowed casts require exactly one argument. */
      return unwrap_cast(linitial(func_expr->args));
    }
  }
  else if (IsA(node, RelabelType))
  {
    RelabelType *relabel_expr = (RelabelType *)node;
    return unwrap_cast((Node *)relabel_expr->arg);
  }
  else if (IsA(node, CoerceViaIO))
  {
    /* `cast as text`; we treat it as a valid cast for datetime-like types. */
    CoerceViaIO *coerce_expr = (CoerceViaIO *)node;
    if (is_datetime_to_string_cast(coerce_expr))
      return unwrap_cast((Node *)coerce_expr->arg);
  }
  return node;
}

int64 unwrap_const_int64(Expr *expr, int64 min_value, int64 max_value)
{
  expr = (Expr *)unwrap_cast((Node *)expr);

  if (!expr || !IsA(expr, Const))
    FAILWITH_LOCATION(exprLocation((Node *)expr), "Expected a constant integer.");

  Const *const_expr = (Const *)expr;
  if (const_expr->constisnull || (const_expr->consttype != INT4OID && const_expr->consttype != INT8OID))
    FAILWITH_LOCATION(exprLocation((Node *)expr), "Expected a constant integer.");

  int64 val = const_expr->consttype == INT4OID
                  ? DatumGetInt32(const_expr->constvalue)
                  : DatumGetInt64(const_expr->constvalue);

  if (val < min_value || val > max_value)
    FAILWITH_LOCATION(exprLocation((Node *)expr), "Value is outside of valid bounds.");

  return val;
}

Expr *make_const_int64(int64 value)
{
  return (Expr *)makeConst(INT8OID, -1, InvalidOid, sizeof(int64), Int64GetDatum(value), false, FLOAT8PASSBYVAL);
}
