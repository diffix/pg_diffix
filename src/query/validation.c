#include "postgres.h"

#include "catalog/pg_collation.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/tlist.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"

#include "pg_diffix/auth.h"
#include "pg_diffix/config.h"
#include "pg_diffix/oid_cache.h"
#include "pg_diffix/query/allowed_functions.h"
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

    if (aggoid != g_oid_cache.count_star && aggoid != g_oid_cache.count_value)
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

/* money-style numbers, i.e. 1, 2, or 5 preceeded by or followed by zeros: ⟨... 0.1, 0.2, 0.5, 1, 2, 5, 10, ...⟩ */
static bool is_money_style(double number)
{
  char number_as_string[30];
  sprintf(number_as_string, "%.15e", number);
  text *pattern = cstring_to_text("^[125]\\.0+e[-+][0-9]+$");
  bool result = RE_compile_and_execute(pattern, number_as_string, strlen(number_as_string), REG_EXTENDED + REG_NOSUB, C_COLLATION_OID, 0, NULL) != REG_OKAY;
  pfree(pattern);
  return result;
}

/* Expects the expression being the second argument to `round_by` et al. */
static void verify_rounding_range_width(Node *range_expr)
{
  Node *range_node = unwrap_cast(range_expr);
  Assert(IsA(range_node, Const)); /* Checked by prior validations */
  Const *range_const = (Const *)range_node;

  if (!is_supported_numeric_const(range_const))
    FAILWITH_LOCATION(range_const->location, "Unsupported constant type used in generalization.");

  if (!is_money_style(const_to_double(range_const)))
    FAILWITH_LOCATION(range_const->location, "Generalization used in the query is not allowed in untrusted access level.");
}

static void verify_generalization(Node *node)
{
  if (IsA(node, FuncExpr))
  {
    FuncExpr *func_expr = (FuncExpr *)node;

    if (is_substring_builtin(func_expr->funcid))
      verify_substring(func_expr);
    else if (is_rounding_udf(func_expr->funcid))
      verify_rounding_range_width((Node *)list_nth(func_expr->args, 1));
    else if (is_rounding_builtin(func_expr->funcid))
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
