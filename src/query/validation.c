#include "postgres.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_inherits.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/tlist.h"
#include "parser/parse_coerce.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

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
static void verify_non_system_column(Var *var);
static void verify_bucket_expressions(Query *query);

void verify_utility_command(Node *utility_stmt)
{
  if (get_session_access_level() != ACCESS_DIRECT && !superuser())
  {
    switch (utility_stmt->type)
    {
    case T_DoStmt:
    case T_NotifyStmt:
    case T_ListenStmt:
    case T_UnlistenStmt:
    case T_TransactionStmt:
    case T_ExplainStmt:
    case T_VariableSetStmt:
    case T_VariableShowStmt:
    case T_DiscardStmt:
    case T_LockStmt:
    case T_CheckPointStmt:
    case T_DeclareCursorStmt:
      break;
    default:
      FAILWITH("Statement requires either SUPERUSER or direct access level.");
    }
  }
}

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

bool verify_pg_catalog_access(List *range_tables)
{
  ListCell *cell;
  foreach (cell, range_tables)
  {
    RangeTblEntry *rte = (RangeTblEntry *)lfirst(cell);
    if (rte->relid != 0)
    {
      const char *namespace_name = get_namespace_name(get_rel_namespace(rte->relid));
      if (strcmp(namespace_name, "pg_catalog") == 0)
        return false;
    }
  }
  return true;
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
  NOT_SUPPORTED(query->setOperations, "UNION/INTERSECT/EXCEPT");

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

    if (range_table->rtekind == RTE_RELATION)
    {
      NOT_SUPPORTED(has_subclass(range_table->relid) || has_superclass(range_table->relid), "Inheritance in anonymizing queries.");
    }
    else
    {
      FAILWITH("Unsupported FROM clause.");
    }
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

static bool is_datetime_to_string_cast(CoerceViaIO *expr)
{
  Node *arg = (Node *)expr->arg;
  return TypeCategory(exprType(arg)) == TYPCATEGORY_DATETIME && TypeCategory(expr->resulttype) == TYPCATEGORY_STRING;
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

static void verify_non_system_column(Var *var)
{
  if (var->varattno < 0)
    FAILWITH_LOCATION(var->location, "System columns are not allowed in bucket expressions.");
}

static void verify_bucket_expression(Node *node)
{
  if (IsA(node, FuncExpr))
  {
    FuncExpr *func_expr = (FuncExpr *)node;
    if (is_allowed_cast(func_expr->funcid))
    {
      Assert(list_length(func_expr->args) == 1); /* All allowed casts require exactly one argument. */
      verify_bucket_expression(linitial(func_expr->args));
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
  else if (IsA(node, OpExpr))
  {
    OpExpr *op_expr = (OpExpr *)node;
    FAILWITH_LOCATION(op_expr->location, "Use of operators to define buckets is not supported.");
  }
  else if (IsA(node, Const))
  {
    Const *const_expr = (Const *)node;
    FAILWITH_LOCATION(const_expr->location, "Simple constants are not allowed as bucket expressions.");
  }
  else if (IsA(node, RelabelType))
  {
    RelabelType *relabel_expr = (RelabelType *)node;
    verify_bucket_expression((Node *)relabel_expr->arg);
  }
  else if (IsA(node, CoerceViaIO))
  {
    CoerceViaIO *coerce_expr = (CoerceViaIO *)node;
    if (is_datetime_to_string_cast(coerce_expr))
      verify_bucket_expression((Node *)coerce_expr->arg);
    else
      FAILWITH_LOCATION(coerce_expr->location, "Unsupported cast destination type name.");
  }
  else if (IsA(node, Var))
  {
    verify_non_system_column((Var *)node);
  }
  else
  {
    FAILWITH("Unsupported or unrecognized query node type");
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
  text *money_pattern = cstring_to_text("^[125]\\.0+e[-+][0-9]+$");
  bool matches_money_pattern = RE_compile_and_execute(money_pattern,
                                                      number_as_string, strlen(number_as_string),
                                                      REG_EXTENDED | REG_NOSUB, C_COLLATION_OID, 0, NULL);
  pfree(money_pattern);
  return matches_money_pattern;
}

/* Expects the expression being the second argument to `round_by` et al. */
static void verify_bin_size(Node *range_expr)
{
  Node *range_node = unwrap_cast(range_expr);
  Assert(IsA(range_node, Const)); /* Checked by prior validations */
  Const *range_const = (Const *)range_node;

  if (!is_supported_numeric_type(range_const->consttype))
    FAILWITH_LOCATION(range_const->location, "Unsupported constant type used in generalization.");

  if (!is_money_style(numeric_value_to_double(range_const->consttype, range_const->constvalue)))
    FAILWITH_LOCATION(range_const->location, "Generalization used in the query is not allowed in untrusted access level.");
}

static void verify_generalization(Node *node)
{
  if (IsA(node, FuncExpr))
  {
    FuncExpr *func_expr = (FuncExpr *)node;

    if (is_substring_builtin(func_expr->funcid))
      verify_substring(func_expr);
    else if (is_implicit_range_udf(func_expr->funcid))
      verify_bin_size((Node *)list_nth(func_expr->args, 1));
    else if (is_implicit_range_builtin(func_expr->funcid))
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

bool is_supported_numeric_type(Oid type)
{
  return TypeCategory(type) == TYPCATEGORY_NUMERIC;
}

double numeric_value_to_double(Oid type, Datum value)
{
  switch (type)
  {
  case INT2OID:
    return DatumGetInt16(value);
  case INT4OID:
    return DatumGetInt32(value);
  case INT8OID:
    return DatumGetInt64(value);
  case FLOAT4OID:
    return DatumGetFloat4(value);
  case FLOAT8OID:
    return DatumGetFloat8(value);
  case NUMERICOID:
    return DatumGetFloat8(DirectFunctionCall1(numeric_float8, value));
  default:
    Assert(false);
    return 0.0;
  }
}
