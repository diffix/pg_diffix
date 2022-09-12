#include "postgres.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/tlist.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"

#include "pg_diffix/auth.h"
#include "pg_diffix/node_funcs.h"
#include "pg_diffix/oid_cache.h"
#include "pg_diffix/query/allowed_objects.h"
#include "pg_diffix/query/validation.h"
#include "pg_diffix/utils.h"

#define NOT_SUPPORTED(cond, feature)                                   \
  do                                                                   \
  {                                                                    \
    if (cond)                                                          \
      FAILWITH("Feature '%s' is not currently supported.", (feature)); \
  } while (0)

static void verify_where(Query *query);
static void verify_rtable(Query *query);
static void verify_aggregators(Query *query);
static void verify_non_system_column(Var *var);
static bool option_matches(DefElem *option, char *name, bool value);

void verify_utility_command(Node *utility_stmt)
{
  if (get_session_access_level() != ACCESS_DIRECT)
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
    case T_DeallocateStmt:
    case T_FetchStmt:
    case T_ClosePortalStmt:
    case T_PrepareStmt:
    case T_ExecuteStmt:
      break;
    default:
      FAILWITH("Statement requires direct access level.");
    }
  }
}

void verify_explain_options(ExplainStmt *explain)
{
  ListCell *cell;

  foreach (cell, explain->options)
  {
    DefElem *option = lfirst_node(DefElem, cell);
    if (option_matches(option, "costs", true))
      FAILWITH("COSTS option is not allowed for queries involving personal tables");
    if (option_matches(option, "analyze", true))
      FAILWITH("EXPLAIN ANALYZE is not allowed for queries involving personal tables");
  }
}

bool verify_pg_catalog_access(List *range_tables)
{
  ListCell *cell;
  foreach (cell, range_tables)
  {
    RangeTblEntry *rte = (RangeTblEntry *)lfirst(cell);
    if (rte->relid != 0)
      if (get_rel_namespace(rte->relid) == PG_CATALOG_NAMESPACE &&
          !is_allowed_pg_catalog_rte(rte->relid, rte->selectedCols))
        return false;
  }
  return true;
}

void verify_anonymization_requirements(Query *query)
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
      NOT_SUPPORTED(has_subclass(range_table->relid) || has_superclass(range_table->relid), "Inheritance in anonymizing queries.");
    else
      FAILWITH("Unsupported FROM clause.");
  }
}

static void verify_non_system_column(Var *var)
{
  if (var->varattno < 0)
    FAILWITH_LOCATION(var->location, "System columns are not allowed in this context.");
}

static void verify_count_histogram(Aggref *aggref, Query *query)
{
  /* Verify that counted arg is an AID. */
  Expr *counted_aid_expr = linitial_node(TargetEntry, aggref->args)->expr;
  if (!IsA(counted_aid_expr, Var))
    FAILWITH_LOCATION(exprLocation((Node *)counted_aid_expr), "count_histogram argument must be an AID column.");

  Var *counted_aid = (Var *)counted_aid_expr;
  RangeTblEntry *rte = rt_fetch(counted_aid->varno, query->rtable);
  if (!is_aid_column(rte->relid, counted_aid->varattno))
    FAILWITH_LOCATION(exprLocation((Node *)counted_aid_expr), "count_histogram argument must be an AID column.");

  if (aggref->aggfnoid == g_oid_cache.count_histogram_int8)
  {
    /* Verify bin size. */
    Expr *bin_size_expr = lsecond_node(TargetEntry, aggref->args)->expr;
    int64 bin_size = unwrap_const_int64(bin_size_expr, 1, INT64_MAX); /* Validates bounds implicitly. */

    if (get_session_access_level() == ACCESS_ANONYMIZED_UNTRUSTED &&
        !is_money_rounded(bin_size))
    {
      FAILWITH_LOCATION(
          exprLocation((Node *)bin_size_expr),
          "Used generalization expression is not allowed in untrusted access level.");
    }
  }
}

static bool is_count_histogram(Oid aggoid)
{
  return aggoid == g_oid_cache.count_histogram || aggoid == g_oid_cache.count_histogram_int8;
}

static bool verify_aggregator(Node *node, void *context)
{
  if (node == NULL)
    return false;

  if (IsA(node, Aggref))
  {
    Aggref *aggref = (Aggref *)node;
    Oid aggoid = aggref->aggfnoid;

    if (aggoid != g_oid_cache.count_star &&
        aggoid != g_oid_cache.count_value &&
        !is_sum_oid(aggoid) &&
        !is_avg_oid(aggoid) &&
        !is_count_histogram(aggoid) &&
        aggoid != g_oid_cache.count_star_noise &&
        aggoid != g_oid_cache.count_value_noise &&
        aggoid != g_oid_cache.sum_noise &&
        aggoid != g_oid_cache.avg_noise &&
        aggoid != g_oid_cache.is_suppress_bin)
      FAILWITH_LOCATION(aggref->location, "Unsupported aggregate in query.");

    if (aggoid == g_oid_cache.count_value || aggoid == g_oid_cache.count_value_noise ||
        is_sum_oid(aggoid) || aggoid == g_oid_cache.sum_noise ||
        is_avg_oid(aggoid) || aggoid == g_oid_cache.avg_noise)
    {
      TargetEntry *tle = (TargetEntry *)unwrap_cast(linitial(aggref->args));
      Node *tle_arg = unwrap_cast((Node *)tle->expr);
      if (IsA(tle_arg, Var))
        verify_non_system_column((Var *)tle_arg);
      else
        FAILWITH_LOCATION(aggref->location, "Unsupported expression as aggregate argument.");
    }

    if (aggref->aggdistinct &&
        (is_sum_oid(aggoid) || aggoid == g_oid_cache.sum_noise ||
         is_avg_oid(aggoid) || aggoid == g_oid_cache.avg_noise ||
         is_count_histogram(aggoid)))
      FAILWITH_LOCATION(aggref->location, "Unsupported distinct qualifier at aggregate argument.");

    if (is_count_histogram(aggoid))
      verify_count_histogram(aggref, (Query *)context);

    NOT_SUPPORTED(aggref->aggfilter, "FILTER clauses in aggregate expressions");
    NOT_SUPPORTED(aggref->aggorder, "ORDER BY clauses in aggregate expressions");
  }

  return expression_tree_walker(node, verify_aggregator, context);
}

static void verify_aggregators(Query *query)
{
  query_tree_walker(query, verify_aggregator, query, 0);
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
      FAILWITH_LOCATION(func_expr->location, "Unsupported function used for generalization.");

    Assert(list_length(func_expr->args) > 0); /* All allowed functions require at least one argument. */

    int primary_arg = primary_arg_index(func_expr->funcid);
    if (!IsA(unwrap_cast(list_nth(func_expr->args, primary_arg)), Var))
      FAILWITH_LOCATION(func_expr->location, "Primary argument for a generalization function has to be a simple column reference.");

    for (int i = 0; i < list_length(func_expr->args); i++)
    {
      if (i == primary_arg)
        continue;
      Node *arg = unwrap_cast((Node *)list_nth(func_expr->args, i));
      if (!is_stable_expression(arg))
        FAILWITH_LOCATION(exprLocation(arg), "Non-primary arguments for a generalization function have to be simple constants.");
    }
  }
  else if (IsA(node, OpExpr))
  {
    OpExpr *op_expr = (OpExpr *)node;
    FAILWITH_LOCATION(op_expr->location, "Use of operators for generalization is not supported.");
  }
  else if (is_stable_expression(node))
  {
    FAILWITH_LOCATION(exprLocation(node), "Simple constants are not allowed as generalization expressions.");
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
    FAILWITH("Unsupported generalization expression.");
  }
}

static void verify_substring(FuncExpr *func_expr, ParamListInfo bound_params)
{
  Node *node = unwrap_cast(list_nth(func_expr->args, 1));
  Assert(is_stable_expression(node)); /* Checked by prior validations */
  Oid type;
  Datum value;
  bool isnull;
  get_stable_expression_value(node, bound_params, &type, &value, &isnull);

  if (DatumGetUInt32(value) != 1)
    FAILWITH_LOCATION(exprLocation(node), "Used generalization expression is not allowed in untrusted access level.");
}

/* Expects the expression being the second argument to `round_by` et al. */
static void verify_bin_size(Node *range_expr, ParamListInfo bound_params)
{
  Node *range_node = unwrap_cast(range_expr);
  Assert(is_stable_expression(range_node)); /* Checked by prior validations */
  Oid type;
  Datum value;
  bool isnull;
  get_stable_expression_value(range_node, bound_params, &type, &value, &isnull);

  if (!is_supported_numeric_type(type))
    FAILWITH_LOCATION(exprLocation(range_node), "Unsupported constant type used in generalization expression.");

  if (!is_money_rounded(numeric_value_to_double(type, value)))
    FAILWITH_LOCATION(exprLocation(range_node), "Used generalization expression is not allowed in untrusted access level.");
}

static void verify_untrusted_bucket_expression(Node *node, ParamListInfo bound_params)
{
  if (IsA(node, FuncExpr))
  {
    FuncExpr *func_expr = (FuncExpr *)node;

    if (is_substring_builtin(func_expr->funcid))
      verify_substring(func_expr, bound_params);
    else if (is_implicit_range_udf_untrusted(func_expr->funcid))
      verify_bin_size(lsecond(func_expr->args), bound_params);
    else if (is_implicit_range_builtin_untrusted(func_expr->funcid))
      ;
    else
      FAILWITH_LOCATION(func_expr->location, "Used generalization expression is not allowed in untrusted access level.");
  }
}

/* Should be run on anonymizing queries only. */
void verify_bucket_expressions(Query *query, ParamListInfo bound_params)
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
    if (access_level == ACCESS_ANONYMIZED_UNTRUSTED)
      verify_untrusted_bucket_expression(expr, bound_params);
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

static bool option_matches(DefElem *option, char *name, bool value)
{
  return strcasecmp(option->defname, name) == 0 && defGetBoolean(option) == value;
}

static bool is_equality_op(Oid opno)
{
  char *opname = get_opname(opno);
  if (opname == NULL)
    return false;
  bool result = strcmp(opname, "=") == 0;
  pfree(opname);
  return result;
}

void collect_equalities_from_filters(Node *node, List **subjects, List **targets)
{
  if (node == NULL)
    return;

  if (is_andclause(node))
  {
    ListCell *cell = NULL;
    foreach (cell, ((BoolExpr *)node)->args)
      collect_equalities_from_filters(lfirst(cell), subjects, targets);
    return;
  }

  if (is_opclause(node))
  {
    OpExpr *op_expr = (OpExpr *)node;
    if (is_equality_op(op_expr->opno))
    {
      Assert(list_length(op_expr->args) == 2);
      *subjects = lappend(*subjects, linitial(op_expr->args));
      *targets = lappend(*targets, lsecond(op_expr->args));
      return;
    }
  }

  FAILWITH("Only equalities between generalization expressions and constants are allowed as pre-anonymization filters.");
}

static Var *get_bucket_expression_column_ref(Node *bucket_expression)
{
  bucket_expression = unwrap_cast(bucket_expression);
  if (IsA(bucket_expression, Var))
    return (Var *)bucket_expression;
  /* If the bucket expression is not a direct column reference, it means it is a simple function call. */
  FuncExpr *func_expr = castNode(FuncExpr, bucket_expression);

  int primary_arg = primary_arg_index(func_expr->funcid);
  return castNode(Var, unwrap_cast(list_nth(func_expr->args, primary_arg)));
}

static void verify_column_usage_in_filter(AccessLevel access_level, Node *bucket_expression, List *range_tables)
{
  Var *var_expr = get_bucket_expression_column_ref(bucket_expression);
  RangeTblEntry *rte = rt_fetch(var_expr->varno, range_tables);

  if (is_aid_column(rte->relid, var_expr->varattno))
    FAILWITH_LOCATION(var_expr->location, "AID columns can't be referenced by pre-anonymization filters.");

  if (access_level == ACCESS_ANONYMIZED_UNTRUSTED && is_not_filterable_column(rte->relid, var_expr->varattno))
    FAILWITH_LOCATION(var_expr->location,
                      "Column marked `not_filterable` can't be referenced by pre-anonymization filters in untrusted-mode.");
}

static void verify_where(Query *query)
{
  AccessLevel access_level = get_session_access_level();

  List *subjects = NIL, *targets = NIL;
  collect_equalities_from_filters(query->jointree->quals, &subjects, &targets);

  ListCell *subject_cell = NULL, *target_cell = NULL;
  forboth(subject_cell, subjects, target_cell, targets)
  {
    Node *bucket_expression = lfirst(subject_cell);
    verify_bucket_expression(bucket_expression);
    verify_column_usage_in_filter(access_level, bucket_expression, query->rtable);

    Node *target_expression = unwrap_cast(lfirst(target_cell));
    if (!is_stable_expression(target_expression))
    {
      FAILWITH_LOCATION(exprLocation(target_expression),
                        "Generalization expressions can only be matched against constants or params in pre-anonymization filters.");
    }
  }

  list_free(subjects);
  list_free(targets);
}
