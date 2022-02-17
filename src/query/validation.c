#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/tlist.h"

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
  config_check();
  verify_query(query);
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
  verify_bucket_expressions(query);
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

static bool is_expression_a(Node *node, NodeTag tag)
{
  if (node->type == tag)
    return true;

  if (IsA(node, FuncExpr))
  {
    FuncExpr *func_expr = (FuncExpr *)node;
    if (is_allowed_cast(func_expr->funcid))
    {
      Assert(list_length(func_expr->args) == 1); /* All allowed casts require exactly one argument. */
      return is_expression_a(linitial(func_expr->args), tag);
    }
  }

  return false;
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

    if (!is_expression_a(linitial(func_expr->args), T_Var))
      FAILWITH_LOCATION(func_expr->location, "Primary argument for a bucket function has to be a simple column reference.");

    for (int i = 1; i < list_length(func_expr->args); i++)
    {
      if (!is_expression_a((Node *)list_nth(func_expr->args, i), T_Const))
        FAILWITH_LOCATION(func_expr->location, "Non-primary arguments for a bucket function have to be simple constants.");
    }
  }

  if (IsA(node, OpExpr))
  {
    OpExpr *op_expr = (OpExpr *)node;
    FAILWITH_LOCATION(op_expr->location, "Unsupported operator used to define buckets.");
  }

  if (IsA(node, Const))
  {
    Const *const_expr = (Const *)node;
    FAILWITH_LOCATION(const_expr->location, "Simple constants are not allowed as bucket expressions.");
  }
}

static void verify_bucket_expressions(Query *query)
{
  List *exprs_list = NIL;
  if (query->groupClause != NIL)
    /* Buckets are explicitly defined. */
    exprs_list = get_sortgrouplist_exprs(query->groupClause, query->targetList);
  else if (!query->hasAggs)
    /* Buckets are implicitly defined. */
    exprs_list = get_tlist_exprs(query->targetList, false);
  /* Else we have the global bucket, nothing to check. */

  ListCell *cell;
  foreach (cell, exprs_list)
  {
    Node *expr = (Node *)lfirst(cell);
    verify_bucket_expression(expr);
  }
}
