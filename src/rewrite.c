#include "postgres.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_oper.h"
#include "catalog/pg_type.h"
#include "catalog/pg_aggregate.h"

#include "pg_diffix/config.h"
#include "pg_diffix/node_helpers.h"
#include "pg_diffix/oid_cache.h"
#include "pg_diffix/rewrite.h"
#include "pg_diffix/utils.h"

#define FAILWITH(...) ereport(ERROR, (errmsg("[PG_DIFFIX] " __VA_ARGS__)))

typedef struct MutatorContext
{
  RelationConfig *relation_config; /* Current relation in query */
  Index relation_index;            /* Index of current relation */
} MutatorContext;

/* Mutators */
static void add_implicit_grouping(Query *query);
static Node *aggregate_expression_mutator(Node *node, MutatorContext *context);
static void add_low_count_filter(Query *query);

/* Utils */
static void inject_aid_arg(Aggref *aggref, MutatorContext *context);
static MutatorContext get_mutator_context(Query *query);

void rewrite_query(Query *query)
{
  add_implicit_grouping(query);

  MutatorContext context = get_mutator_context(query);
  query_tree_mutator(
      query,
      aggregate_expression_mutator,
      &context,
      QTW_DONT_COPY_QUERY | QTW_EXAMINE_RTES_BEFORE);

  add_low_count_filter(query);
}

/*-------------------------------------------------------------------------
 * Implicit grouping
 *-------------------------------------------------------------------------
 */

static void add_implicit_grouping(Query *query)
{
  /* Only simple select queries require implicit grouping. */
  if (query->hasAggs || query->groupClause != NIL)
    return;

  DEBUG_LOG("Rewriting query to group by the selected expressions (Query ID=%lu).", query->queryId);

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

/*-------------------------------------------------------------------------
 * Low count filtering
 *-------------------------------------------------------------------------
 */

static void add_filter_to_clause(Node **clause, Node *filter)
{
  if (*clause == NULL)
    *clause = filter;
  else
    *clause = (Node *)makeBoolExpr(AND_EXPR, list_make2(*clause, filter), -1);
}

static void add_low_count_filter(Query *query)
{
  /* Global aggregates have to be excluded from low-count filtering. */
  if (query->hasAggs && query->groupClause == NIL)
    return;

  Aggref *lcf_agg = makeNode(Aggref);

  lcf_agg->aggfnoid = OidCache.diffix_lcf;
  lcf_agg->aggtype = BOOLOID;
  lcf_agg->aggtranstype = InvalidOid; /* will be set by planner */
  lcf_agg->aggstar = false;
  lcf_agg->aggvariadic = false;
  lcf_agg->aggkind = AGGKIND_NORMAL;
  lcf_agg->aggsplit = AGGSPLIT_SIMPLE; /* planner might change this */
  lcf_agg->location = -1;              /* unknown location */

  MutatorContext context = get_mutator_context(query);
  inject_aid_arg(lcf_agg, &context);

  add_filter_to_clause(&query->havingQual, (Node *)lcf_agg);
  query->hasAggs = true;
}

/*-------------------------------------------------------------------------
 * Anonymizing aggregates
 *-------------------------------------------------------------------------
 */

/* Returns true if the target entry is a direct reference to the AID. */
static bool is_aid_arg(TargetEntry *arg, MutatorContext *context)
{
  if (!IsA(arg->expr, Var))
    return false;

  Var *var = (Var *)arg->expr;
  /* Check if we have a variable to the same relation and same attnum as AID */
  return var->varno == context->relation_index && var->varattno == context->relation_config->aid_attnum;
}

static void rewrite_count(Aggref *aggref, MutatorContext *context)
{
  aggref->aggfnoid = OidCache.diffix_count;
  aggref->aggstar = false;
  inject_aid_arg(aggref, context);
}

static void rewrite_count_distinct(Aggref *aggref, MutatorContext *context)
{
  aggref->aggfnoid = OidCache.diffix_count_distinct;
  /* The UDF handles distinct counting internally */
  aggref->aggdistinct = false;
  TargetEntry *arg = linitial_node(TargetEntry, aggref->args);
  if (!is_aid_arg(arg, context))
    FAILWITH("COUNT(DISTINCT col) requires an AID column as its argument.");
}

static void rewrite_count_any(Aggref *aggref, MutatorContext *context)
{
  aggref->aggfnoid = OidCache.diffix_count_any;
  inject_aid_arg(aggref, context);
}

static Node *aggregate_expression_mutator(Node *node, MutatorContext *context)
{
  if (node == NULL)
    return NULL;

  if (IsA(node, Query))
  {
    Query *query = (Query *)node;
    MutatorContext subcontext = get_mutator_context(query);
    return (Node *)query_tree_mutator(
        query,
        aggregate_expression_mutator,
        &subcontext,
        0);
  }
  else if (IsA(node, Aggref))
  {
    /*
     * Copy and visit sub expressions.
     * We basically use this for copying, but we could use the visitor to validate aggregate args in the future.
     */
    Aggref *aggref = (Aggref *)expression_tree_mutator(node, aggregate_expression_mutator, (void *)context);
    Oid aggfnoid = aggref->aggfnoid;

    if (aggfnoid == OidCache.count)
      rewrite_count(aggref, context);
    else if (aggfnoid == OidCache.count_any && aggref->aggdistinct)
      rewrite_count_distinct(aggref, context);
    else if (aggfnoid == OidCache.count_any)
      rewrite_count_any(aggref, context);
    /*
    else
      FAILWITH("Unsupported aggregate in query.");
    */

    return (Node *)aggref;
  }

  return expression_tree_mutator(node, aggregate_expression_mutator, (void *)context);
}

/*-------------------------------------------------------------------------
 * Utils
 *-------------------------------------------------------------------------
 */

static void inject_aid_arg(Aggref *aggref, MutatorContext *context)
{
  RelationConfig *relation = context->relation_config;

  /* Insert AID type in front of aggargtypes */
  aggref->aggargtypes = list_insert_nth_oid(aggref->aggargtypes, 0, relation->aid_atttype);

  Expr *aid_expr = (Expr *)makeVar(
      context->relation_index, /* varno */
      relation->aid_attnum,    /* varattno */
      relation->aid_atttype,   /* vartype */
      relation->aid_typmod,    /* vartypmod */
      relation->aid_collid,    /* varcollid */
      0                        /* varlevelsup */
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

/*
 * Expects and returns a single sensitive relation in the query.
 * Reports an error if there are 0 or multiple relations present in the query.
 */
static RelationConfig *single_relation_config(Query *query)
{
  List *relations = gather_sensitive_relations(query, false);
  if (relations == NIL || relations->length != 1)
  {
    FAILWITH("Expected a single sensitive relation in query range.");
  }

  RelationConfig *relation = linitial(relations);
  list_free(relations);
  return relation;
}

/*
 * Builds a context to be used during query traversal.
 */
static MutatorContext get_mutator_context(Query *query)
{
  MutatorContext context = {
      .relation_config = single_relation_config(query),
      .relation_index = 1 /* We only have a single relation at this point. */
  };
  return context;
}
