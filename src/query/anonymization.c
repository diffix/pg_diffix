#include "postgres.h"

#include "catalog/pg_aggregate.h"
#include "catalog/pg_type.h"
#include "common/shortest_dec.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_oper.h"
#include "parser/parsetree.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"

#include "pg_diffix/aggregation/bucket_scan.h"
#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/oid_cache.h"
#include "pg_diffix/query/allowed_functions.h"
#include "pg_diffix/query/anonymization.h"
#include "pg_diffix/query/relation.h"
#include "pg_diffix/query/validation.h"
#include "pg_diffix/utils.h"

typedef struct AidReference
{
  const SensitiveRelation *relation; /* Source relation of AID */
  const AidColumn *aid_column;       /* Column data for AID */
  Index rte_index;                   /* RTE index in query rtable */
  AttrNumber aid_attnum;             /* AID AttrNumber in relation/subquery */
} AidReference;

typedef struct QueryContext
{
  Query *query;         /* Current query/subquery */
  List *aid_references; /* `AidReference`s in scope */
  List *child_contexts; /* `QueryContext`s of subqueries */
} QueryContext;

static void append_aid_args(Aggref *aggref, QueryContext *context);

/* Append a junk target entry at the end of query's tlist. */
static TargetEntry *add_junk_tle(Query *query, Expr *expr, char *resname)
{
  AttrNumber resno = list_length(query->targetList) + 1;
  TargetEntry *target_entry = makeTargetEntry(expr, resno, resname, true);
  query->targetList = lappend(query->targetList, target_entry);
  return target_entry;
}

/*-------------------------------------------------------------------------
 * Implicit grouping
 *-------------------------------------------------------------------------
 */

static bool is_not_const(Node *node, void *context)
{
  if (node == NULL)
    return false;

  if (IsA(node, Var))
  {
    return true;
  }

  return expression_tree_walker(node, is_not_const, context);
}

static void group_implicit_buckets(Query *query)
{
  ListCell *cell = NULL;
  foreach (cell, query->targetList)
  {
    TargetEntry *tle = lfirst_node(TargetEntry, cell);

    if (!is_not_const((Node *)tle->expr, NULL))
      continue;

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
 * Appends junk `count(*)` to target list.
 */
static void add_junk_count_star(Query *query)
{
  Aggref *count_agg = makeNode(Aggref);
  count_agg->aggfnoid = g_oid_cache.count_star; /* Will be replaced later with anonymizing version. */
  count_agg->aggtype = INT8OID;
  count_agg->aggtranstype = InvalidOid; /* Will be set by planner. */
  count_agg->aggstar = true;
  count_agg->aggvariadic = false;
  count_agg->aggkind = AGGKIND_NORMAL;
  count_agg->aggsplit = AGGSPLIT_SIMPLE; /* Planner might change this. */
  count_agg->location = -1;              /* Unknown location. */
  add_junk_tle(query, (Expr *)count_agg, "anon_count_star");
}

/*-------------------------------------------------------------------------
 * Low count filtering
 *-------------------------------------------------------------------------
 */

/*
 * Appends junk `low_count(aids...)` to target list.
 */
static void add_junk_low_count_agg(QueryContext *context)
{
  Aggref *lc_agg = makeNode(Aggref);
  lc_agg->aggfnoid = g_oid_cache.low_count;
  lc_agg->aggtype = BOOLOID;
  lc_agg->aggtranstype = InvalidOid; /* Will be set by planner. */
  lc_agg->aggstar = false;
  lc_agg->aggvariadic = false;
  lc_agg->aggkind = AGGKIND_NORMAL;
  lc_agg->aggsplit = AGGSPLIT_SIMPLE; /* Planner might change this. */
  lc_agg->location = -1;              /* Unknown location. */
  append_aid_args(lc_agg, context);
  add_junk_tle(context->query, (Expr *)lc_agg, "low_count");
}

/*-------------------------------------------------------------------------
 * Anonymizing aggregates
 *-------------------------------------------------------------------------
 */

static void rewrite_to_anon_aggregator(Aggref *aggref, QueryContext *context, Oid fnoid)
{
  aggref->aggfnoid = fnoid;
  /*
   * Technically aggref->aggtype will be different, but the translation will happen in BucketScan.
   * We do this because we want valid expression trees to go through the planner.
   */
  aggref->aggstar = false;
  aggref->aggdistinct = false;
  append_aid_args(aggref, context);
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
        QTW_DONT_COPY_QUERY);
  }
  else if (IsA(node, Aggref))
  {
    /*
     * Copy and visit sub expressions.
     * We basically use this for copying, but we could use the visitor to process args in the future.
     */
    Aggref *aggref = (Aggref *)expression_tree_mutator(node, aggregate_expression_mutator, context);
    Oid aggfnoid = aggref->aggfnoid;

    if (aggfnoid == g_oid_cache.count_star)
      rewrite_to_anon_aggregator(aggref, context, g_oid_cache.anon_count_star);
    else if (aggfnoid == g_oid_cache.count_value && aggref->aggdistinct)
      rewrite_to_anon_aggregator(aggref, context, g_oid_cache.anon_count_distinct);
    else if (aggfnoid == g_oid_cache.count_value)
      rewrite_to_anon_aggregator(aggref, context, g_oid_cache.anon_count_value);
    /*
    else
      FAILWITH("Unsupported aggregate in query.");
    */

    return (Node *)aggref;
  }

  return expression_tree_mutator(node, aggregate_expression_mutator, context);
}

/*-------------------------------------------------------------------------
 * AID Utils
 *-------------------------------------------------------------------------
 */

static Expr *make_aid_expr(AidReference *ref)
{
  return (Expr *)makeVar(
      ref->rte_index,
      ref->aid_attnum,
      ref->aid_column->atttype,
      ref->aid_column->typmod,
      ref->aid_column->collid,
      0);
}

static TargetEntry *make_aid_target(AidReference *ref, AttrNumber resno, bool resjunk)
{
  TargetEntry *te = makeTargetEntry(make_aid_expr(ref), resno, "aid", resjunk);

  te->resorigtbl = ref->relation->oid;
  te->resorigcol = ref->aid_column->attnum;

  return te;
}

static SensitiveRelation *find_relation(Oid rel_oid, List *relations)
{
  ListCell *cell;
  foreach (cell, relations)
  {
    SensitiveRelation *relation = (SensitiveRelation *)lfirst(cell);
    if (relation->oid == rel_oid)
      return relation;
  }

  return NULL;
}

/*
 * Adds references targeting AIDs of relation to `aid_references`.
 */
static void gather_relation_aids(
    const SensitiveRelation *relation,
    Index rte_index,
    RangeTblEntry *rte,
    List **aid_references)
{
  ListCell *cell;
  foreach (cell, relation->aid_columns)
  {
    AidColumn *aid_col = (AidColumn *)lfirst(cell);

    AidReference *aid_ref = palloc(sizeof(AidReference));
    aid_ref->relation = relation;
    aid_ref->aid_column = aid_col;
    aid_ref->rte_index = rte_index;
    aid_ref->aid_attnum = aid_col->attnum;

    *aid_references = lappend(*aid_references, aid_ref);

    /* Emulate what the parser does */
    rte->selectedCols = bms_add_member(
        rte->selectedCols, aid_col->attnum - FirstLowInvalidHeapAttributeNumber);
  }
}

/* Search for an existing column reference in the target list of a query. */
static AttrNumber get_var_attnum(List *target_list, Index rte_index, AttrNumber attnum)
{
  ListCell *cell = NULL;
  foreach (cell, target_list)
  {
    TargetEntry *tle = lfirst_node(TargetEntry, cell);
    if (IsA(tle->expr, Var))
    {
      Var *var_expr = (Var *)tle->expr;
      if (var_expr->varno == rte_index && var_expr->varattno == attnum)
        return tle->resno;
    }
  }
  return InvalidAttrNumber;
}

/*
 * Appends missing AID references of the subquery to its target list for use in parent query.
 * References to the exported AIDs are added to `aid_references`.
 */
static void gather_subquery_aids(
    RangeTblEntry *parent_rte,
    const QueryContext *child_context,
    Index rte_index,
    List **aid_references)
{
  Query *subquery = child_context->query;
  AttrNumber next_attnum = list_length(subquery->targetList) + 1;

  ListCell *cell;
  foreach (cell, child_context->aid_references)
  {
    AidReference *child_aid_ref = (AidReference *)lfirst(cell);

    AttrNumber attnum = get_var_attnum(subquery->targetList, child_aid_ref->rte_index, child_aid_ref->aid_attnum);
    if (attnum == InvalidAttrNumber) /* AID not referenced in subquery. */
    {
      /* Export AID from subquery. */
      attnum = next_attnum++;
      TargetEntry *aid_target = make_aid_target(child_aid_ref, attnum, true);
      subquery->targetList = lappend(subquery->targetList, aid_target);
      /* Update parent range table entry. */
      parent_rte->eref->colnames = lappend(parent_rte->eref->colnames, makeString(aid_target->resname));
    }

    /* Path to AID from parent query. */
    AidReference *parent_aid_ref = palloc(sizeof(AidReference));
    parent_aid_ref->relation = child_aid_ref->relation;
    parent_aid_ref->aid_column = child_aid_ref->aid_column;
    parent_aid_ref->rte_index = rte_index;
    parent_aid_ref->aid_attnum = attnum;

    *aid_references = lappend(*aid_references, parent_aid_ref);
  }
}

/*
 * Collects and prepares AIDs for use in the current query's scope.
 * Subqueries are rewritten to export all their AIDs.
 */
static QueryContext *build_context(Query *query, List *relations)
{
  List *aid_references = NIL;
  List *child_contexts = NIL;

  ListCell *cell;
  foreach (cell, query->rtable)
  {
    RangeTblEntry *rte = (RangeTblEntry *)lfirst(cell);
    Index rte_index = foreach_current_index(cell) + 1;

    if (rte->rtekind == RTE_RELATION)
    {
      SensitiveRelation *relation = find_relation(rte->relid, relations);
      if (relation != NULL)
        gather_relation_aids(relation, rte_index, rte, &aid_references);
    }
    else if (rte->rtekind == RTE_SUBQUERY)
    {
      QueryContext *child_context = build_context(rte->subquery, relations);
      child_contexts = lappend(child_contexts, child_context);
      gather_subquery_aids(rte, child_context, rte_index, &aid_references);
    }
  }

  QueryContext *context = palloc(sizeof(QueryContext));
  context->query = query;
  context->child_contexts = child_contexts;
  context->aid_references = aid_references;
  return context;
}

static void append_aid_args(Aggref *aggref, QueryContext *context)
{
  bool found_any = false;

  ListCell *cell;
  foreach (cell, context->aid_references)
  {
    AidReference *aid_ref = (AidReference *)lfirst(cell);
    TargetEntry *aid_entry = make_aid_target(aid_ref, list_length(aggref->args) + 1, false);

    /* Append the AID argument to function's arguments. */
    aggref->args = lappend(aggref->args, aid_entry);
    aggref->aggargtypes = lappend_oid(aggref->aggargtypes, aid_ref->aid_column->atttype);

    found_any = true;
  }

  if (!found_any)
    FAILWITH("No AID found in target relations.");
}

/*-------------------------------------------------------------------------
 * Bucket seeding
 *-------------------------------------------------------------------------
 */

#define MAX_SEED_MATERIAL_SIZE 1024 /* Fixed max size, to avoid dynamic allocation. */

static void append_seed_material(
    char *existing_material, const char *new_material, char separator)
{
  size_t existing_material_length = strlen(existing_material);
  size_t new_material_length = strlen(new_material);

  if (existing_material_length + new_material_length + 2 > MAX_SEED_MATERIAL_SIZE)
    FAILWITH_CODE(ERRCODE_NAME_TOO_LONG, "Bucket seed material too long!");

  if (existing_material_length > 0)
    existing_material[existing_material_length++] = separator;

  strcpy(existing_material + existing_material_length, new_material);
}

typedef struct CollectMaterialContext
{
  Query *query;
  char material[MAX_SEED_MATERIAL_SIZE];
} CollectMaterialContext;

static bool collect_seed_material(Node *node, CollectMaterialContext *context)
{
  if (node == NULL)
    return false;

  if (IsA(node, FuncExpr))
  {
    FuncExpr *func_expr = (FuncExpr *)node;
    if (!is_allowed_cast(func_expr->funcid))
    {
      char *func_name = get_func_name(func_expr->funcid);
      if (func_name)
      {
        /* TODO: Normalize function names. */
        append_seed_material(context->material, func_name, ',');
        pfree(func_name);
      }
    }
  }

  if (IsA(node, Var))
  {
    Var *var_expr = (Var *)node;
    RangeTblEntry *rte = rt_fetch(var_expr->varno, context->query->rtable);

    char *relation_name = get_rel_name(rte->relid);
    /* TODO: Remove this check once anonymization over non-ordinary relations is rejected. */
    if (relation_name)
    {
      append_seed_material(context->material, relation_name, ',');
      pfree(relation_name);
    }

    char *attribute_name = get_rte_attribute_name(rte, var_expr->varattno);
    append_seed_material(context->material, attribute_name, '.');
  }

  if (IsA(node, Const))
  {
    Const *const_expr = (Const *)node;

    if (!is_supported_numeric_type(const_expr->consttype))
      FAILWITH_LOCATION(const_expr->location, "Unsupported constant type used in bucket definition!");

    double const_as_double = numeric_value_to_double(const_expr->consttype, const_expr->constvalue);
    char const_as_string[DOUBLE_SHORTEST_DECIMAL_LEN];
    double_to_shortest_decimal_buf(const_as_double, const_as_string);
    append_seed_material(context->material, const_as_string, ',');
  }

  /* We ignore unknown nodes. Validation should make sure nothing unsafe reaches this stage. */
  return expression_tree_walker(node, collect_seed_material, context);
}

/*
 * Computes the SQL part of the bucket seed by combining the unique grouping expressions' seed material hashes.
 * Grouping clause (if any) must be made explicit before calling this.
 */
static seed_t prepare_bucket_seeds(Query *query)
{
  List *seed_material_hash_set = NULL;

  List *grouping_exprs = get_sortgrouplist_exprs(query->groupClause, query->targetList);
  ListCell *cell = NULL;
  foreach (cell, grouping_exprs)
  {
    Node *expr = lfirst(cell);

    /* Start from empty string and append material pieces for each non-cast expression. */
    CollectMaterialContext collect_context = {.query = query, .material = ""};
    collect_seed_material(expr, &collect_context);

    /* Keep materials with unique hashes to avoid them cancelling each other. */
    hash_t seed_material_hash = hash_string(collect_context.material);
    seed_material_hash_set = hash_set_add(seed_material_hash_set, seed_material_hash);
  }

  seed_t sql_seed = hash_set_to_seed(seed_material_hash_set);

  list_free(seed_material_hash_set);

  return sql_seed;
}

static hash_t hash_label(Oid type, Datum value, bool is_null)
{
  if (is_null)
    return hash_string("NULL");

  if (is_supported_numeric_type(type))
  {
    /* Normalize numeric values. */
    double value_as_double = numeric_value_to_double(type, value);
    char value_as_string[DOUBLE_SHORTEST_DECIMAL_LEN];
    double_to_shortest_decimal_buf(value_as_double, value_as_string);
    return hash_string(value_as_string);
  }

  /* Handle all other types by casting to text. */
  Oid type_output_funcid = InvalidOid;
  bool is_varlena = false;
  getTypeOutputInfo(type, &type_output_funcid, &is_varlena);

  char *value_as_string = OidOutputFunctionCall(type_output_funcid, value);
  hash_t hash = hash_string(value_as_string);
  pfree(value_as_string);

  return hash;
}

seed_t compute_bucket_seed(const Bucket *bucket, const BucketDescriptor *bucket_desc)
{
  List *label_hash_set = NIL;
  for (int i = 0; i < bucket_desc->num_labels; i++)
  {
    hash_t label_hash = hash_label(bucket_desc->attrs[i].final_type, bucket->values[i], bucket->is_null[i]);
    label_hash_set = hash_set_add(label_hash_set, label_hash);
  }

  seed_t bucket_seed = bucket_desc->anon_context->sql_seed ^ hash_set_to_seed(label_hash_set);

  list_free(label_hash_set);

  return bucket_seed;
}

/*-------------------------------------------------------------------------
 * Query rewriting
 *-------------------------------------------------------------------------
 */

static AnonymizationContext *make_query_anonymizing(Query *query, List *sensitive_relations)
{
  QueryContext *context = build_context(query, sensitive_relations);
  AnonymizationContext *anon_context = palloc0(sizeof(AnonymizationContext));

  bool initial_has_aggs = query->hasAggs;
  bool initial_has_group_clause = query->groupClause != NIL;
  bool initial_all_targets_constant = !is_not_const((Node *)query->targetList, NULL);

  /* Only simple select queries require implicit grouping. */
  if (!initial_has_aggs && !initial_has_group_clause)
  {
    DEBUG_LOG("Rewriting query to group and expand implicit buckets (Query ID=%lu).", query->queryId);
    group_implicit_buckets(query);
    add_junk_count_star(query);
    anon_context->expand_buckets = true;
  }

  query_tree_mutator(
      query,
      aggregate_expression_mutator,
      context,
      QTW_DONT_COPY_QUERY);

  /* Global aggregates have to be excluded from low-count filtering. */
  if (initial_has_group_clause || (!initial_has_aggs && !initial_all_targets_constant))
    add_junk_low_count_agg(context);

  query->hasAggs = true; /* Anonymizing queries always have at least one aggregate. */

  return anon_context;
}

typedef struct AggrefLink
{
  AnonymizationContext *anon_context; /* Reference to anon context */
  int orig_location;                  /* Original token location */
  Oid aggref_oid;                     /* Aggref aggfnoid, used for sanity checking */
} AggrefLink;

struct AnonQueryLinks
{
  List *aggref_links; /* List of AggrefLink */
};

const int AGGREF_LINK_OFFSET = 1000000000; /* Big number to avoid accidental overlap. */

/*
 * Data (context) used by both link and extract walkers.
 * Intentionally avoids using the word "context" twice.
 */
typedef struct AnonContextWalkerData
{
  AnonQueryLinks *links;              /* Where to store added links */
  AnonymizationContext *anon_context; /* Anon context to associate */
} AnonContextWalkerData;

static bool link_anon_context_walker(Node *node, AnonContextWalkerData *data)
{
  if (node == NULL)
    return false;

  if (IsA(node, Aggref))
  {
    Aggref *aggref = (Aggref *)node;
    if (is_anonymizing_agg(aggref->aggfnoid))
    {
      int orig_location = aggref->location;

      AggrefLink *link = palloc(sizeof(AggrefLink));
      link->anon_context = data->anon_context;
      link->orig_location = orig_location;
      link->aggref_oid = aggref->aggfnoid;

      int link_index = list_length(data->links->aggref_links);
      data->links->aggref_links = lappend(data->links->aggref_links, link);

      aggref->location = AGGREF_LINK_OFFSET + link_index; /* Attach the index to aggref. */
    }
  }

  return expression_tree_walker(node, link_anon_context_walker, data);
}

/*
 * Encodes an AnonContext reference to Aggrefs of anonymizing aggregates
 * by injecting AGGREF_LINK_OFFSET + AggrefLink's index to aggref->location.
 */
static AnonQueryLinks *link_anon_context(Query *query, AnonymizationContext *anon_context)
{
  /* Once we support subqueries, this needs to be shared across the tree. */
  AnonQueryLinks *links = palloc(sizeof(AnonQueryLinks));
  links->aggref_links = NIL;

  AnonContextWalkerData data = {links, anon_context};
  expression_tree_walker((Node *)query->targetList, link_anon_context_walker, &data);

  return links;
}

AnonQueryLinks *compile_anonymizing_query(Query *query, List *sensitive_relations)
{
  verify_anonymization_requirements(query);

  AnonymizationContext *anon_context = make_query_anonymizing(query, sensitive_relations);

  verify_anonymizing_query(query);

  anon_context->sql_seed = prepare_bucket_seeds(query);

  return link_anon_context(query, anon_context);
}

/*-------------------------------------------------------------------------
 * Plan rewriting
 *-------------------------------------------------------------------------
 */

static bool extract_anon_context_walker(Node *node, AnonContextWalkerData *data)
{
  if (node == NULL)
    return false;

  if (IsA(node, Aggref))
  {
    Aggref *aggref = (Aggref *)node;
    if (is_anonymizing_agg(aggref->aggfnoid) && aggref->location >= AGGREF_LINK_OFFSET)
    {
      int link_index = aggref->location - AGGREF_LINK_OFFSET;
      AggrefLink *link = list_nth(data->links->aggref_links, link_index);

      /* Sanity checks. */
      if (link->aggref_oid != aggref->aggfnoid)
        FAILWITH("Mismatched aggregate OIDs during plan rewrite.");
      if (data->anon_context != NULL && data->anon_context != link->anon_context)
        FAILWITH("Mismatched anonymizing subqueries in plan.");

      data->anon_context = link->anon_context;
      aggref->location = link->orig_location;
    }
  }

  return expression_tree_walker(node, extract_anon_context_walker, data);
}

/*
 * Reverse process of `link_anon_context`. Extracts and returns
 * the associated AnonymizationContext or NULL if none is found.
 */
static AnonymizationContext *extract_anon_context(Plan *plan, AnonQueryLinks *links)
{
  AnonContextWalkerData data = {links, NULL};
  expression_tree_walker((Node *)plan->targetlist, extract_anon_context_walker, &data);
  return data.anon_context;
}

static void rewrite_plan_list(List *plans, AnonQueryLinks *links)
{
  ListCell *cell;
  foreach (cell, plans)
  {
    Plan *plan = (Plan *)lfirst(cell);
    plans->elements[foreach_current_index(cell)].ptr_value = rewrite_plan(plan, links);
  }
}

Plan *rewrite_plan(Plan *plan, AnonQueryLinks *links)
{
  if (plan == NULL)
    return NULL;

  plan->lefttree = rewrite_plan(plan->lefttree, links);
  plan->righttree = rewrite_plan(plan->righttree, links);

  switch (plan->type)
  {
  case T_Append:
    rewrite_plan_list(((Append *)plan)->appendplans, links);
    break;
  case T_MergeAppend:
    rewrite_plan_list(((MergeAppend *)plan)->mergeplans, links);
    break;
  case T_SubqueryScan:
    ((SubqueryScan *)plan)->subplan = rewrite_plan(((SubqueryScan *)plan)->subplan, links);
    break;
  case T_CustomScan:
    rewrite_plan_list(((CustomScan *)plan)->custom_plans, links);
    break;
  default:
    /* Nothing to do. */
    break;
  }

  if (IsA(plan, Agg))
  {
    AnonymizationContext *anon_context = extract_anon_context(plan, links);
    if (anon_context != NULL)
      return make_bucket_scan(plan, anon_context);
  }

  return plan;
}
