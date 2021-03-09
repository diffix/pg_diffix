#include "postgres.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_oper.h"
#include "catalog/pg_type.h"
#include "catalog/pg_aggregate.h"

#include "pg_diffix/utils.h"
#include "pg_diffix/query/oid_cache.h"
#include "pg_diffix/query/rewrite.h"

#define FAILWITH(...) ereport(ERROR, (errmsg("[PG_DIFFIX] " __VA_ARGS__)))

#define AID_RELATION(context) ((RelationData *)linitial(context->relations))
#define AID_RELATION_INDEX 1

/* Mutators */
static void group_and_expand_implicit_buckets(Query *query);
static Node *aggregate_expression_mutator(Node *node, QueryContext *context);
static void add_low_count_filter(QueryContext *context);

/* Utils */
static void inject_aid_arg(Aggref *aggref, QueryContext *context);

void rewrite_query(QueryContext *context)
{
  Query *query = context->query;

  group_and_expand_implicit_buckets(query);

  query_tree_mutator(
      query,
      aggregate_expression_mutator,
      context,
      QTW_DONT_COPY_QUERY | QTW_EXAMINE_RTES_BEFORE);

  add_low_count_filter(context);

  query->hasAggs = true; /* Anonymizing queries always have at least one aggregate. */
}

/*-------------------------------------------------------------------------
 * Implicit grouping
 *-------------------------------------------------------------------------
 */

static void group_implicit_buckets(Query *query)
{
  ListCell *lc = NULL;
  foreach (lc, query->targetList)
  {
    TargetEntry *tle = lfirst_node(TargetEntry, lc);

    Oid type = exprType((const Node *)tle->expr);
    Assert(type != UNKNOWNOID);

    /* Set group index to ordinal position. */
    tle->ressortgroupref = tle->resno;

    /* Determine the eqop and optional sortop. */
    Oid sortop = 0;
    Oid eqop = 0;
    bool hashable = false;
    get_sort_group_operators(type, false, true, false, &sortop, &eqop, NULL, &hashable);

    /* Create group clause for current item. */
    SortGroupClause *groupClause = makeNode(SortGroupClause);
    groupClause->tleSortGroupRef = tle->ressortgroupref;
    groupClause->eqop = eqop;
    groupClause->sortop = sortop;
    groupClause->nulls_first = false; /* OK with or without sortop */
    groupClause->hashable = hashable;

    /* Add group clause to query. */
    query->groupClause = lappend(query->groupClause, groupClause);
  }
}

/*
 * Expand implicit buckets by adding a hidden call to `generate_series(1, diffix_count(*))` to the selection list.
 */
static void expand_implicit_buckets(Query *query)
{
  Const *const_one = makeConst(INT8OID, -1, InvalidOid, SIZEOF_LONG, Int64GetDatum(1), false, FLOAT8PASSBYVAL);

  Aggref *count_agg = makeNode(Aggref);
  count_agg->aggfnoid = g_oid_cache.count; /* Will be replaced later with anonymizing version. */
  count_agg->aggtype = INT8OID;
  count_agg->aggtranstype = InvalidOid; /* Will be set by planner. */
  count_agg->aggstar = true;
  count_agg->aggvariadic = false;
  count_agg->aggkind = AGGKIND_NORMAL;
  count_agg->aggsplit = AGGSPLIT_SIMPLE; /* Planner might change this. */
  count_agg->location = -1;              /* Unknown location. */

  FuncExpr *generate_series = makeNode(FuncExpr);
  generate_series->funcid = g_oid_cache.generate_series;
  generate_series->funcresulttype = INT8OID;
  generate_series->funcretset = true;
  generate_series->funcvariadic = false;
  generate_series->args = list_make2(const_one, count_agg);
  generate_series->location = -1;

  int target_count = list_length(query->targetList);
  TargetEntry *expand_entry = makeTargetEntry((Expr *)generate_series, target_count + 1, NULL, false);
  expand_entry->resjunk = true; /* Hide output values. */

  lappend(query->targetList, expand_entry);
  query->hasTargetSRFs = true;
}

static void group_and_expand_implicit_buckets(Query *query)
{
  /* Only simple select queries require implicit grouping. */
  if (query->hasAggs || query->groupClause != NIL)
    return;

  DEBUG_LOG("Rewriting query to group and expand implicit buckets (Query ID=%lu).", query->queryId);

  group_implicit_buckets(query);
  expand_implicit_buckets(query);
}

/*-------------------------------------------------------------------------
 * Low count filtering
 *-------------------------------------------------------------------------
 */

static void add_low_count_filter(QueryContext *context)
{
  Query *query = context->query;
  /* Global aggregates have to be excluded from low-count filtering. */
  if (query->hasAggs && query->groupClause == NIL)
    return;

  Aggref *lcf_agg = makeNode(Aggref);

  lcf_agg->aggfnoid = g_oid_cache.diffix_lcf;
  lcf_agg->aggtype = BOOLOID;
  lcf_agg->aggtranstype = InvalidOid; /* Will be set by planner. */
  lcf_agg->aggstar = false;
  lcf_agg->aggvariadic = false;
  lcf_agg->aggkind = AGGKIND_NORMAL;
  lcf_agg->aggsplit = AGGSPLIT_SIMPLE; /* Planner might change this. */
  lcf_agg->location = -1;              /* Unknown location. */

  inject_aid_arg(lcf_agg, context);

  query->havingQual = make_and_qual(query->havingQual, (Node *)lcf_agg);
  query->hasAggs = true;
}

/*-------------------------------------------------------------------------
 * Anonymizing aggregates
 *-------------------------------------------------------------------------
 */

/* Returns true if the target entry is a direct reference to the AID. */
static bool is_aid_arg(TargetEntry *arg, QueryContext *context)
{
  if (!IsA(arg->expr, Var))
    return false;

  Var *var = (Var *)arg->expr;
  /* Check if we have a variable to the same relation and same attnum as AID */
  return var->varno == AID_RELATION_INDEX && var->varattno == AID_RELATION(context)->aid_attnum;
}

static void rewrite_count(Aggref *aggref, QueryContext *context)
{
  aggref->aggfnoid = g_oid_cache.diffix_count;
  aggref->aggstar = false;
  inject_aid_arg(aggref, context);
}

static void rewrite_count_distinct(Aggref *aggref, QueryContext *context)
{
  aggref->aggfnoid = g_oid_cache.diffix_count_distinct;
  /* The UDF handles distinct counting internally */
  aggref->aggdistinct = false;
  TargetEntry *arg = linitial_node(TargetEntry, aggref->args);
  if (!is_aid_arg(arg, context))
    FAILWITH("COUNT(DISTINCT col) requires an AID column as its argument.");
}

static void rewrite_count_any(Aggref *aggref, QueryContext *context)
{
  aggref->aggfnoid = g_oid_cache.diffix_count_any;
  inject_aid_arg(aggref, context);
}

static Node *aggregate_expression_mutator(Node *node, QueryContext *context)
{
  if (node == NULL)
    return NULL;

  if (IsA(node, Query))
  {
    Query *query = (Query *)node;
    return (Node *)query_tree_mutator(
        query,
        aggregate_expression_mutator,
        context,
        0);
  }
  else if (IsA(node, Aggref))
  {
    /*
     * Copy and visit sub expressions.
     * We basically use this for copying, but we could use the visitor to process args in the future.
     */
    Aggref *aggref = (Aggref *)expression_tree_mutator(node, aggregate_expression_mutator, context);
    Oid aggfnoid = aggref->aggfnoid;

    if (aggfnoid == g_oid_cache.count)
      rewrite_count(aggref, context);
    else if (aggfnoid == g_oid_cache.count_any && aggref->aggdistinct)
      rewrite_count_distinct(aggref, context);
    else if (aggfnoid == g_oid_cache.count_any)
      rewrite_count_any(aggref, context);
    /*
    else
      FAILWITH("Unsupported aggregate in query.");
    */

    return (Node *)aggref;
  }

  return expression_tree_mutator(node, aggregate_expression_mutator, context);
}

/*-------------------------------------------------------------------------
 * Utils
 *-------------------------------------------------------------------------
 */

static void inject_aid_arg(Aggref *aggref, QueryContext *context)
{
  RelationData *relation = AID_RELATION(context);

  /* Insert AID type in front of aggargtypes */
  aggref->aggargtypes = list_insert_nth_oid(aggref->aggargtypes, 0, relation->aid_atttype);

  Expr *aid_expr = (Expr *)makeVar(
      AID_RELATION_INDEX,    /* varno */
      relation->aid_attnum,  /* varattno */
      relation->aid_atttype, /* vartype */
      relation->aid_typmod,  /* vartypmod */
      relation->aid_collid,  /* varcollid */
      0                      /* varlevelsup */
  );
  TargetEntry *aid_entry = makeTargetEntry(aid_expr, 1, NULL, false);

  /* Insert AID target entry in front of args */
  aggref->args = list_insert_nth(aggref->args, 0, aid_entry);

  /* Bump resno for all args */
  ListCell *lc;
  foreach (lc, aggref->args)
  {
    TargetEntry *tle = lfirst_node(TargetEntry, lc);
    tle->resno = foreach_current_index(lc) + 1;
  }
}
