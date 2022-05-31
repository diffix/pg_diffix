#include "postgres.h"

#include "catalog/pg_aggregate.h"
#include "catalog/pg_type.h"
#include "common/shortest_dec.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/tlist.h"
#include "parser/parse_oper.h"
#include "parser/parsetree.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"

#include "pg_diffix/aggregation/bucket_scan.h"
#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/oid_cache.h"
#include "pg_diffix/query/allowed_objects.h"
#include "pg_diffix/query/anonymization.h"
#include "pg_diffix/query/relation.h"
#include "pg_diffix/query/validation.h"
#include "pg_diffix/utils.h"

typedef struct AidRef
{
  const PersonalRelation *relation; /* Source relation of AID */
  const AidColumn *aid_column;      /* Column data for AID */
  Index rte_index;                  /* RTE index in query rtable */
  AttrNumber aid_attnum;            /* AID AttrNumber in relation/subquery */
} AidRef;

static void append_aid_args(Aggref *aggref, List *aid_refs);

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
static void add_junk_low_count_agg(Query *query, List *aid_refs)
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
  append_aid_args(lc_agg, aid_refs);
  add_junk_tle(query, (Expr *)lc_agg, "low_count");
}

/*-------------------------------------------------------------------------
 * Anonymizing aggregators
 *-------------------------------------------------------------------------
 */

static void rewrite_to_anon_aggregator(Aggref *aggref, List *aid_refs, Oid fnoid)
{
  aggref->aggfnoid = fnoid;
  /*
   * Technically aggref->aggtype will be different, but the translation will happen in BucketScan.
   * We do this because we want valid expression trees to go through the planner.
   */
  aggref->aggstar = false;
  aggref->aggdistinct = false;
  append_aid_args(aggref, aid_refs);
}

static Node *aggregate_expression_mutator(Node *node, List *aid_refs)
{
  if (node == NULL)
    return NULL;

  if (IsA(node, Aggref))
  {
    /*
     * Copy and visit sub expressions.
     * We basically use this for copying, but we could use the visitor to process args in the future.
     */
    Aggref *aggref = (Aggref *)expression_tree_mutator(node, aggregate_expression_mutator, aid_refs);
    Oid aggfnoid = aggref->aggfnoid;

    if (aggfnoid == g_oid_cache.count_star)
      rewrite_to_anon_aggregator(aggref, aid_refs, g_oid_cache.anon_count_star);
    else if (aggfnoid == g_oid_cache.count_value && aggref->aggdistinct)
      rewrite_to_anon_aggregator(aggref, aid_refs, g_oid_cache.anon_count_distinct);
    else if (aggfnoid == g_oid_cache.count_value)
      rewrite_to_anon_aggregator(aggref, aid_refs, g_oid_cache.anon_count_value);
    else if (is_sum_oid(aggfnoid))
      rewrite_to_anon_aggregator(aggref, aid_refs, g_oid_cache.anon_sum);
    else if (aggfnoid == g_oid_cache.count_star_noise)
      rewrite_to_anon_aggregator(aggref, aid_refs, g_oid_cache.anon_count_star_noise);
    else if (aggfnoid == g_oid_cache.count_value_noise && aggref->aggdistinct)
      rewrite_to_anon_aggregator(aggref, aid_refs, g_oid_cache.anon_count_distinct_noise);
    else if (aggfnoid == g_oid_cache.count_value_noise)
      rewrite_to_anon_aggregator(aggref, aid_refs, g_oid_cache.anon_count_value_noise);
    else if (is_sum_noise_oid(aggfnoid))
      rewrite_to_anon_aggregator(aggref, aid_refs, g_oid_cache.anon_sum_noise);
    /*
    else
      FAILWITH("Unsupported aggregate in query.");
    */

    return (Node *)aggref;
  }

  return expression_tree_mutator(node, aggregate_expression_mutator, aid_refs);
}

/*-------------------------------------------------------------------------
 * AID Utils
 *-------------------------------------------------------------------------
 */

static Expr *make_aid_expr(AidRef *aid_ref)
{
  return (Expr *)makeVar(
      aid_ref->rte_index,
      aid_ref->aid_attnum,
      aid_ref->aid_column->atttype,
      aid_ref->aid_column->typmod,
      aid_ref->aid_column->collid,
      0);
}

static TargetEntry *make_aid_target(AidRef *aid_ref, AttrNumber resno, bool resjunk)
{
  TargetEntry *te = makeTargetEntry(make_aid_expr(aid_ref), resno, "aid", resjunk);

  te->resorigtbl = aid_ref->relation->oid;
  te->resorigcol = aid_ref->aid_column->attnum;

  return te;
}

static PersonalRelation *find_relation(Oid rel_oid, List *relations)
{
  ListCell *cell;
  foreach (cell, relations)
  {
    PersonalRelation *relation = (PersonalRelation *)lfirst(cell);
    if (relation->oid == rel_oid)
      return relation;
  }

  return NULL;
}

/*
 * Adds references targeting AIDs of relation to `aid_refs`.
 */
static void gather_relation_aids(
    const PersonalRelation *relation,
    Index rte_index,
    RangeTblEntry *rte,
    List **aid_refs)
{
  ListCell *cell;
  foreach (cell, relation->aid_columns)
  {
    AidColumn *aid_col = (AidColumn *)lfirst(cell);

    AidRef *aid_ref = palloc(sizeof(AidRef));
    aid_ref->relation = relation;
    aid_ref->aid_column = aid_col;
    aid_ref->rte_index = rte_index;
    aid_ref->aid_attnum = aid_col->attnum;

    *aid_refs = lappend(*aid_refs, aid_ref);

    /* Emulate what the parser does */
    rte->selectedCols = bms_add_member(
        rte->selectedCols, aid_col->attnum - FirstLowInvalidHeapAttributeNumber);
  }
}

/* Collects and prepares AIDs for use in the current query's scope. */
static List *gather_aid_refs(Query *query, List *relations)
{
  List *aid_refs = NIL;

  ListCell *cell;
  foreach (cell, query->rtable)
  {
    RangeTblEntry *rte = (RangeTblEntry *)lfirst(cell);
    Index rte_index = foreach_current_index(cell) + 1;

    if (rte->rtekind == RTE_RELATION)
    {
      PersonalRelation *relation = find_relation(rte->relid, relations);
      if (relation != NULL)
        gather_relation_aids(relation, rte_index, rte, &aid_refs);
    }
  }

  return aid_refs;
}

static void append_aid_args(Aggref *aggref, List *aid_refs)
{
  bool found_any = false;

  ListCell *cell;
  foreach (cell, aid_refs)
  {
    AidRef *aid_ref = (AidRef *)lfirst(cell);
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

static AnonymizationContext *make_query_anonymizing(Query *query, List *personal_relations)
{
  List *aid_refs = gather_aid_refs(query, personal_relations);
  if (aid_refs == NIL)
    FAILWITH("No AID found in target relations.");

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
      aid_refs,
      QTW_DONT_COPY_QUERY);

  /* Global aggregates have to be excluded from low-count filtering. */
  if (initial_has_group_clause || (!initial_has_aggs && !initial_all_targets_constant))
    add_junk_low_count_agg(query, aid_refs);

  query->hasAggs = true; /* Anonymizing queries always have at least one aggregate. */

  /* Compute grouping columns indices now because planner may overwrite the info later. */
  anon_context->grouping_cols = extract_grouping_cols(query->groupClause, query->targetList);
  anon_context->grouping_cols_count = list_length(query->groupClause);

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

static int link_index_to_location(int link_index)
{
  return AGGREF_LINK_OFFSET + link_index;
}

static int location_to_link_index(int location)
{
  Assert(location >= AGGREF_LINK_OFFSET);
  return location - AGGREF_LINK_OFFSET;
}

/*
 * Data (context) used by both link and extract walkers.
 * Intentionally avoids using the word "context" twice.
 */
typedef struct AnonContextWalkerData
{
  AnonQueryLinks *anon_links;         /* Where to store added links */
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
      AggrefLink *link = palloc(sizeof(AggrefLink));
      link->anon_context = data->anon_context;
      link->orig_location = aggref->location;
      link->aggref_oid = aggref->aggfnoid;

      int link_index = list_length(data->anon_links->aggref_links);
      data->anon_links->aggref_links = lappend(data->anon_links->aggref_links, link);

      aggref->location = link_index_to_location(link_index); /* Attach the index to aggref. */
    }
  }

  return expression_tree_walker(node, link_anon_context_walker, data);
}

/*
 * Encodes an AnonContext reference to Aggrefs of anonymizing aggregators
 * by injecting AGGREF_LINK_OFFSET + AggrefLink's index to aggref->location.
 */
static void link_anon_context(Query *query, AnonQueryLinks *anon_links, AnonymizationContext *anon_context)
{
  AnonContextWalkerData data = {.anon_links = anon_links, .anon_context = anon_context};
  expression_tree_walker((Node *)query->targetList, link_anon_context_walker, &data);
}

/*
 * Wraps the query's HAVING with a volatile identity function.
 * This prevents the planner from pushing the qual down to WHERE clause.
 */
static void wrap_having_qual(Query *query)
{
  if (query->havingQual == NULL)
    return;

  query->havingQual = (Node *)makeFuncExpr(
      g_oid_cache.internal_qual_wrapper,
      BOOLOID,
      list_make1(query->havingQual),
      0 /* funccollid */,
      0 /* inputcollid */,
      COERCE_EXPLICIT_CALL);
}

static void compile_anonymizing_query(Query *query, List *personal_relations, AnonQueryLinks *anon_links)
{
  verify_anonymization_requirements(query);

  AnonymizationContext *anon_context = make_query_anonymizing(query, personal_relations);

  verify_bucket_expressions(query);

  anon_context->sql_seed = prepare_bucket_seeds(query);

  link_anon_context(query, anon_links, anon_context);

  wrap_having_qual(query);
}

static bool is_anonymizing_query(Query *query, List *personal_relations)
{
  ListCell *cell;
  foreach (cell, query->rtable)
  {
    RangeTblEntry *rte = (RangeTblEntry *)lfirst(cell);
    if (rte->rtekind == RTE_RELATION)
    {
      PersonalRelation *relation = find_relation(rte->relid, personal_relations);
      if (relation != NULL)
        return true;
    }
  }
  return false;
}

typedef struct QueryCompileContext
{
  List *personal_relations;
  AnonQueryLinks *anon_links;
} QueryCompileContext;

static bool compile_query_walker(Node *node, QueryCompileContext *context)
{
  if (node == NULL)
    return false;

  if (IsA(node, RangeTblEntry))
  {
    /*
     * Because we specify QTW_EXAMINE_RTES_AFTER, at this point the tree has
     * already been walked and anonymizing queries have been compiled.
     *
     * If this RTE is an anonymizing subquery, we put a security barrier to prevent the
     * planner from pulling the subquery up. We also put a dummy LIMIT clause to prevent
     * "non-leaky" quals from being pushed down and affect cross-bucket computation.
     */
    RangeTblEntry *rte = (RangeTblEntry *)node;

    if (rte->rtekind == RTE_SUBQUERY && is_anonymizing_query(rte->subquery, context->personal_relations))
    {
      rte->security_barrier = true;

      if (rte->subquery->limitCount == NULL)
        rte->subquery->limitCount = (Node *)makeConst(INT8OID, -1, InvalidOid,
                                                      sizeof(int64),
                                                      Int64GetDatum(INT64_MAX), false,
                                                      FLOAT8PASSBYVAL);
    }

    /* Prevent double walk. */
    return false;
  }

  if (IsA(node, Query))
  {
    Query *query = (Query *)node;
    if (is_anonymizing_query(query, context->personal_relations))
      compile_anonymizing_query(query, context->personal_relations, context->anon_links);
    else
      query_tree_walker(query, compile_query_walker, context, QTW_EXAMINE_RTES_AFTER);
  }

  return expression_tree_walker(node, compile_query_walker, context);
}

AnonQueryLinks *compile_query(Query *query, List *personal_relations)
{
  QueryCompileContext context = {
      .personal_relations = personal_relations,
      .anon_links = palloc0(sizeof(AnonQueryLinks)),
  };

  compile_query_walker((Node *)query, &context);

  return context.anon_links;
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
      int link_index = location_to_link_index(aggref->location);
      AggrefLink *link = list_nth(data->anon_links->aggref_links, link_index);

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

/*
 * Reverses the transformation done in `wrap_having_qual`. Pulls the expression
 * from the function's argument and converts it to implicit AND form.
 */
static void unwrap_having_qual(Plan *plan)
{
  if (plan->qual == NIL)
    return;

  Expr *func_expr = linitial(plan->qual);
  if (list_length(plan->qual) != 1 || !IsA(func_expr, FuncExpr) || ((FuncExpr *)func_expr)->funcid != g_oid_cache.internal_qual_wrapper)
    FAILWITH("Unsupported HAVING clause in anonymizing query.");

  Expr *having_qual = linitial(((FuncExpr *)func_expr)->args);

  /* Do what the planner does with top-level quals. */
  having_qual = canonicalize_qual(having_qual, false);
  plan->qual = make_ands_implicit(having_qual);
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
    {
      unwrap_having_qual(plan);
      return make_bucket_scan(plan, anon_context);
    }
  }

  if (IsA(plan, Limit) && is_bucket_scan(plan->lefttree))
  {
    Limit *limit = (Limit *)plan;
    if (IsA(limit->limitCount, Const) && DatumGetInt64(((Const *)limit->limitCount)->constvalue) == INT64_MAX)
    {
      /* Skip dummy limit parent of bucket scan. */
      return plan->lefttree;
    }
  }

  return plan;
}
