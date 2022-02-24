#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/tlist.h"
#include "utils/fmgrprotos.h"

#include "pg_diffix/auth.h"
#include "pg_diffix/config.h"
#include "pg_diffix/oid_cache.h"
#include "pg_diffix/query/allowed_functions.h"
#include "pg_diffix/query/regex_utils.h"
#include "pg_diffix/query/validation.h"
#include "pg_diffix/utils.h"

#define NOT_SUPPORTED(cond, feature) \
  if (cond)                          \
    FAILWITH("Feature '%s' is not currently supported.", (feature));

static void verify_query(Query *query);
static void verify_where(Query *query);
static void verify_rtable(Query *query);
static void verify_aggregators(Query *query);
static void verify_bucket_expressions(Query *query);

void verify_anonymization_requirements(Query *query)
{
  /*
   * Since we cannot easily validate cross-dependent parameters using GUC,
   * we verify those here and fail if they are misconfigured.
   */
  config_validate();
  verify_query(query);
}

void verify_anonymizing_query(Query *query)
{
  verify_bucket_expressions(query);
}

static void verify_query(Query *query)
{
  NOT_SUPPORTED(query->commandType != CMD_SELECT, "non-select query");
  NOT_SUPPORTED(query->cteList, "WITH");
  NOT_SUPPORTED(query->hasForUpdate, "FOR [KEY] UPDATE/SHARE");
  NOT_SUPPORTED(query->hasSubLinks, "SubLinks");
  NOT_SUPPORTED(query->hasTargetSRFs, "SRF functions");
  NOT_SUPPORTED(query->groupingSets, "GROUPING SETS");
  NOT_SUPPORTED(query->windowClause, "window functions");
  NOT_SUPPORTED(query->distinctClause, "DISTINCT");
  NOT_SUPPORTED(query->setOperations, "UNION/INTERSECT");

  verify_where(query);
  verify_aggregators(query);
  verify_rtable(query);
}

static void verify_where(Query *query)
{
  NOT_SUPPORTED(query->jointree->quals, "WHERE clauses in anonymizing queries");
}

static void verify_rtable(Query *query)
{
  NOT_SUPPORTED(list_length(query->rtable) > 1, "JOINs in anonymizing queries");

  ListCell *cell = NULL;
  foreach (cell, query->rtable)
  {
    RangeTblEntry *range_table = lfirst_node(RangeTblEntry, cell);
    NOT_SUPPORTED(range_table->rtekind == RTE_SUBQUERY, "Subqueries in anonymizing queries");
    NOT_SUPPORTED(range_table->rtekind == RTE_JOIN, "JOINs in anonymizing queries");

    if (range_table->rtekind != RTE_RELATION)
      FAILWITH("Unsupported FROM clause.");
  }
}

static bool verify_aggregator(Node *node, void *context)
{
  if (node == NULL)
    return false;

  if (IsA(node, Aggref))
  {
    Aggref *aggref = (Aggref *)node;
    Oid aggoid = aggref->aggfnoid;

    if (aggoid != g_oid_cache.count && aggoid != g_oid_cache.count_any)
      FAILWITH_LOCATION(aggref->location, "Unsupported aggregate in query.");

    NOT_SUPPORTED(aggref->aggfilter, "FILTER clauses in aggregate expressions");
    NOT_SUPPORTED(aggref->aggorder, "ORDER BY clauses in aggregate expressions");
  }

  return expression_tree_walker(node, verify_aggregator, context);
}

static void verify_aggregators(Query *query)
{
  query_tree_walker(query, verify_aggregator, NULL, 0);
}

static Node *unwrap_cast(Node *node)
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

  return node;
}

static void verify_bucket_expression(Node *node)
{
  if (IsA(node, FuncExpr))
  {
    FuncExpr *func_expr = (FuncExpr *)node;
    if (is_allowed_cast(func_expr->funcid))
    {
      Assert(list_length(func_expr->args) == 1); /* All allowed casts require exactly one argument. */
      return verify_bucket_expression(linitial(func_expr->args));
    }

    if (!is_allowed_function(func_expr->funcid))
      FAILWITH_LOCATION(func_expr->location, "Unsupported function used to define buckets.");

    Assert(list_length(func_expr->args) > 0); /* All allowed functions require at least one argument. */

    if (!IsA(unwrap_cast(linitial(func_expr->args)), Var))
      FAILWITH_LOCATION(func_expr->location, "Primary argument for a bucket function has to be a simple column reference.");

    for (int i = 1; i < list_length(func_expr->args); i++)
    {
      if (!IsA(unwrap_cast((Node *)list_nth(func_expr->args, i)), Const))
        FAILWITH_LOCATION(func_expr->location, "Non-primary arguments for a bucket function have to be simple constants.");
    }
  }

  if (IsA(node, OpExpr))
  {
    OpExpr *op_expr = (OpExpr *)node;
    FAILWITH_LOCATION(op_expr->location, "Use of operators to define buckets is not supported.");
  }

  if (IsA(node, Const))
  {
    Const *const_expr = (Const *)node;
    FAILWITH_LOCATION(const_expr->location, "Simple constants are not allowed as bucket expressions.");
  }
}

static void verify_substring(FuncExpr *func_expr)
{
  Node *node = unwrap_cast(list_nth(func_expr->args, 1));
  Assert(IsA(node, Const)); /* Checked by prior validations */
  Const *second_arg = (Const *)node;

  if (DatumGetUInt32(second_arg->constvalue) != 1)
    FAILWITH_LOCATION(second_arg->location, "Generalization used in the query is not allowed in untrusted access level.");
}

static void verify_rounding(FuncExpr *func_expr)
{
  Node *node = unwrap_cast(list_nth(func_expr->args, 1));
  Assert(IsA(node, Const)); /* Checked by prior validations */
  Const *second_arg = (Const *)node;

  if (!is_supported_numeric_const(second_arg))
    FAILWITH_LOCATION(second_arg->location, "Unsupported constant type used in generalization.");

  char second_arg_as_string[30];
  sprintf(second_arg_as_string, "%.15e", const_to_double(second_arg));

  if (!generalization_regex_match(second_arg_as_string))
    FAILWITH_LOCATION(second_arg->location, "Generalization used in the query is not allowed in untrusted access level.");
}

static void verify_generalization(Node *node)
{
  if (IsA(node, FuncExpr))
  {
    FuncExpr *func_expr = (FuncExpr *)node;

    if (is_substring(func_expr->funcid))
      verify_substring(func_expr);
    else if (func_expr->funcid == g_oid_cache.floor_by_nn || func_expr->funcid == g_oid_cache.floor_by_dd)
      verify_rounding(func_expr);
    else if (is_numeric_generalization(func_expr->funcid))
      ;
    else
      FAILWITH_LOCATION(func_expr->location, "Generalization used in the query is not allowed in untrusted access level.");
  }
}

/* Should be run on rewritten queries only. */
static void verify_bucket_expressions(Query *query)
{
  AccessLevel access_level = get_session_access_level();

  List *exprs_list = NIL;
  if (query->groupClause != NIL)
    /* Buckets were either explicitly defined, or implicitly defined and rewritten. */
    exprs_list = get_sortgrouplist_exprs(query->groupClause, query->targetList);
  /* Else we have the global bucket, nothing to check. */

  ListCell *cell;
  foreach (cell, exprs_list)
  {
    Node *expr = (Node *)lfirst(cell);
    verify_bucket_expression(expr);
    if (access_level == ACCESS_PUBLISH_UNTRUSTED)
      verify_generalization(expr);
  }
}

bool is_supported_numeric_const(const Const *const_expr)
{
  switch (const_expr->consttype)
  {
  case INT2OID:
  case INT4OID:
  case INT8OID:
  case FLOAT4OID:
  case FLOAT8OID:
  case NUMERICOID:
    return true;
  default:
    return false;
  }
}

double const_to_double(const Const *const_expr)
{
  switch (const_expr->consttype)
  {
  case INT2OID:
    return DatumGetInt16(const_expr->constvalue);
  case INT4OID:
    return DatumGetInt32(const_expr->constvalue);
  case INT8OID:
    return DatumGetInt64(const_expr->constvalue);
  case FLOAT4OID:
    return DatumGetFloat4(const_expr->constvalue);
  case FLOAT8OID:
    return DatumGetFloat8(const_expr->constvalue);
  case NUMERICOID:
    return DatumGetFloat8(DirectFunctionCall1(numeric_float8, const_expr->constvalue));
  default:
    Assert(false);
    return 0.0;
  }
}
