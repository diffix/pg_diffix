#include "postgres.h"

#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/tlist.h"

#include "pg_diffix/aggregation/bucket_scan.h"
#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/utils.h"

/*-------------------------------------------------------------------------
 *
 * BucketScan is a wrapper for an Agg plan. We do some rewriting to allow for cross-bucket processing,
 * which would be impossible if we streamed buckets one by one to upper nodes or the user.
 *
 * Because Agg node also does projection and filtering, we need to delay them by rewriting the plan.
 * The following changes are made to the underlying node:
 *
 *   Grouping columns:
 *
 *   Projection of grouping labels is handled by Agg's outer plan. We rewrite Agg to export
 *   them as-is at the beggining of its tlist, meaning 1..n will be the grouping labels.
 *
 *   Aggregates:
 *
 *   Aggregates can be found in tlist and qual. We need to export both in Agg's tlist because we
 *   move the actual projection and qual to BucketScan. TLEs n+1..n+m will be the aggregates.
 *   When rewriting expressions for proj/qual, we do a simple equality-based deduplication to
 *   minimize aggregates in tlist. It is not very important to be smart about optimizing at this
 *   stage because ExecInitAgg will take care of sharing aggregation state during execution.
 *   Arguments to aggregates are untouched because they do not leave the node.
 *
 *   Projection/filtering:
 *
 *   Because we need to consider aggregate merging, anonymizing aggregates are left unfinalized
 *   until after cross-bucket processing completes. Once we're ready to emit tuples, we move
 *   finalized aggregates to the scan slot in indexes 1..m. Expressions in proj/qual will have
 *   access to group labels from OUTER_VAR 1..n and aggregate values from INDEX_VAR 1..m.
 *   Expressions of the Agg node are moved to the BucketScan and rewritten to target the above vars.
 *
 *-------------------------------------------------------------------------
 */

/* Plan node */
typedef struct BucketScan
{
  CustomScan custom_scan;

  /* Custom state during planning. */
} BucketScan;

/* Executor node */
typedef struct BucketScanState
{
  CustomScanState custom_scan_state;

  /* Custom state during execution. */
  MemoryContext bucket_context; /* Buckets and aggregates are allocated in this context. */
} BucketScanState;

/* Memory context of currently executing BucketScan node. */
MemoryContext g_current_bucket_context = NULL;

/*-------------------------------------------------------------------------
 * CustomExecMethods
 *-------------------------------------------------------------------------
 */

static void bucket_begin_scan(CustomScanState *css, EState *estate, int eflags)
{
  Plan *plan = css->ss.ps.plan;
  BucketScanState *scan_state = (BucketScanState *)css;

  Assert(outerPlan(plan) != NULL);
  Assert(innerPlan(plan) == NULL);

  if (eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK))
    FAILWITH("Cannot REWIND, BACKWARD, or MARK/RESTORE a BucketScan.");

  scan_state->bucket_context = AllocSetContextCreate(estate->es_query_cxt, "BucketScanState", ALLOCSET_DEFAULT_SIZES);

  outerPlanState(scan_state) = ExecInitNode(outerPlan(plan), estate, eflags);
}

static TupleTableSlot *bucket_exec_scan(CustomScanState *css)
{
  BucketScanState *scan_state = (BucketScanState *)css;

  PlanState *outer_plan_state = outerPlanState(css);
  Assert(outer_plan_state != NULL);

  ExprState *qual = css->ss.ps.qual;
  ProjectionInfo *proj_info = css->ss.ps.ps_ProjInfo;
  ExprContext *econtext = css->ss.ps.ps_ExprContext;

  /* Caution: This needs to be restored before returning. */
  MemoryContext old_bucket_context = g_current_bucket_context;
  g_current_bucket_context = scan_state->bucket_context;

  for (;;)
  {
    TupleTableSlot *outer_slot = ExecProcNode(outer_plan_state);

    if (TupIsNull(outer_slot))
    {
      g_current_bucket_context = old_bucket_context;
      return NULL;
    }

    econtext->ecxt_outertuple = outer_slot;
    if (ExecQualAndReset(qual, econtext))
    {
      TupleTableSlot *result_slot = ExecProject(proj_info);
      g_current_bucket_context = old_bucket_context;
      return result_slot;
    }
  }
}

static void bucket_end_scan(CustomScanState *css)
{
  BucketScanState *scan_state = (BucketScanState *)css;

  MemoryContextDelete(scan_state->bucket_context);
  scan_state->bucket_context = NULL;

  /* Shut down subplans. */
  ExecEndNode(outerPlanState(css));
}

static void bucket_rescan(CustomScanState *css)
{
  FAILWITH("Rescan not supported for BucketScan.");
}

static void bucket_explain_scan(CustomScanState *node, List *ancestors, ExplainState *es)
{
}

static const CustomExecMethods BucketScanExecMethods = {
    .CustomName = "BucketScan",
    .BeginCustomScan = bucket_begin_scan,
    .ExecCustomScan = bucket_exec_scan,
    .EndCustomScan = bucket_end_scan,
    .ReScanCustomScan = bucket_rescan,
    .ExplainCustomScan = bucket_explain_scan,
};

/*-------------------------------------------------------------------------
 * CustomScanMethods
 *-------------------------------------------------------------------------
 */

static Node *create_bucket_scan_state(CustomScan *custom_scan)
{
  BucketScanState *scan_state = (BucketScanState *)newNode(
      sizeof(BucketScanState), T_CustomScanState);

  scan_state->custom_scan_state.methods = &BucketScanExecMethods;

  return (Node *)scan_state;
}

static const CustomScanMethods BucketScanScanMethods = {
    .CustomName = "BucketScan",
    .CreateCustomScanState = create_bucket_scan_state,
};

/*-------------------------------------------------------------------------
 * Planner
 *-------------------------------------------------------------------------
 */

static TargetEntry *find_var_target_entry(List *tlist, AttrNumber var_attno)
{
  ListCell *cell;
  foreach (cell, tlist)
  {
    TargetEntry *target_entry = lfirst_node(TargetEntry, cell);
    Expr *expr = target_entry->expr;
    if (IsA(expr, Var))
    {
      Var *var = (Var *)expr;
      if (var->varno == OUTER_VAR && var->varattno == var_attno)
        return target_entry;
    }
  }

  return NULL;
}

static bool gather_aggrefs_walker(Node *node, List **aggrefs)
{
  if (node == NULL)
    return false;

  if (IsA(node, Aggref))
  {
    *aggrefs = list_append_unique(*aggrefs, node); /* Uses node equals to compare. */
    return false;
  }

  return expression_tree_walker(node, gather_aggrefs_walker, aggrefs);
}

/*
 * Returns a new target list for Agg without any projections.
 * First entries are grouping labels, followed by aggregate expressions.
 */
static List *flatten_agg_tlist(Agg *agg)
{
  List *child_tlist = outerPlan(agg)->targetlist;
  List *orig_agg_tlist = agg->plan.targetlist;
  List *flat_agg_tlist = NIL;
  int num_labels = agg->numCols;

  /* Add grouping labels to target list. */
  for (int i = 0; i < num_labels; i++)
  {
    AttrNumber label_var_attno = agg->grpColIdx[i];
    TargetEntry *child_target_entry = list_nth_node(TargetEntry, child_tlist, label_var_attno - 1);

    Var *label_var = makeVarFromTargetEntry(OUTER_VAR, child_target_entry);
    Assert(label_var_attno == label_var->varattno);

    TargetEntry *label_target_entry = makeTargetEntry(
        (Expr *)label_var,
        i + 1,
        NULL,
        false);
    label_target_entry->ressortgroupref = i + 1;

    TargetEntry *orig_target_entry = find_var_target_entry(orig_agg_tlist, label_var_attno);
    if (orig_target_entry != NULL)
    {
      label_target_entry->resname = orig_target_entry->resname;
      label_target_entry->resorigtbl = orig_target_entry->resorigtbl;
      label_target_entry->resorigcol = orig_target_entry->resorigcol;
    }

    flat_agg_tlist = lappend(flat_agg_tlist, label_target_entry);
  }

  /* Add aggregates to target list. */
  List *aggrefs = NIL;
  gather_aggrefs_walker((Node *)agg->plan.targetlist, &aggrefs);
  gather_aggrefs_walker((Node *)agg->plan.qual, &aggrefs);

  int num_aggrefs = list_length(aggrefs);
  for (int i = 0; i < num_aggrefs; i++)
  {
    Expr *aggref = (Expr *)list_nth(aggrefs, i);

    TargetEntry *agg_target_entry = makeTargetEntry(
        aggref,
        num_labels + i + 1,
        NULL,
        false);

    TargetEntry *orig_target_entry = tlist_member(aggref, orig_agg_tlist);
    if (orig_target_entry != NULL)
      agg_target_entry->resname = orig_target_entry->resname;

    flat_agg_tlist = lappend(flat_agg_tlist, agg_target_entry);
  }

  return flat_agg_tlist;
}

typedef struct RewriteProjectionContext
{
  List *flat_agg_tlist;
  int num_labels;
} RewriteProjectionContext;

/*
 * Rewrites a projection to target the flattened target list.
 * These expressions are evaluated in the BucketScan, which has the Agg as its outer plan.
 * Grouping labels are rewritten to OUTER_VARs, whereas finalized aggregates are INDEX_VARs.
 */
static Node *rewrite_projection_mutator(Node *node, RewriteProjectionContext *context)
{
  if (node == NULL)
    return NULL;

  if (IsA(node, Aggref))
  {
    Aggref *aggref = (Aggref *)node;
    TargetEntry *agg_tle = tlist_member((Expr *)aggref, context->flat_agg_tlist);
    Assert(agg_tle != NULL);

    const AnonAggFuncs *agg_funcs = find_agg_funcs(aggref->aggfnoid);
    if (agg_funcs == NULL)
    {
      /* This is a non anonymizing aggregate. Will already be finalized in outer slot. */
      return (Node *)makeVarFromTargetEntry(OUTER_VAR, agg_tle);
    }

    Oid final_type;
    int32 final_typmod;
    Oid final_collid;
    agg_funcs->final_type(&final_type, &final_typmod, &final_collid);

    return (Node *)makeVar(INDEX_VAR,
                           agg_tle->resno - context->num_labels,
                           final_type,
                           final_typmod,
                           final_collid,
                           0);
  }

  if (IsA(node, Var))
  {
    Var *var = (Var *)node;
    TargetEntry *label_tle = find_var_target_entry(context->flat_agg_tlist, var->varattno);
    /* Vars can only point to grouping labels, and they should have been exported by Agg. */
    Assert(label_tle != NULL);
    return (Node *)makeVarFromTargetEntry(OUTER_VAR, label_tle);
  }

  return expression_tree_mutator(node, rewrite_projection_mutator, context);
}

static List *project_agg_tlist(List *orig_agg_tlist, RewriteProjectionContext *context)
{
  List *projected_tlist = NIL;

  ListCell *cell;
  foreach (cell, orig_agg_tlist)
  {
    TargetEntry *orig_tle = (TargetEntry *)lfirst(cell);

    TargetEntry *projected_tle = makeTargetEntry(
        (Expr *)rewrite_projection_mutator((Node *)orig_tle->expr, context),
        orig_tle->resno,
        orig_tle->resname,
        orig_tle->resjunk);

    projected_tle->resorigtbl = orig_tle->resorigtbl;
    projected_tle->resorigcol = orig_tle->resorigcol;

    projected_tlist = lappend(projected_tlist, projected_tle);
  }

  return projected_tlist;
}

static List *project_agg_qual(List *orig_agg_qual, RewriteProjectionContext *context)
{
  List *projected_qual = NIL;

  ListCell *cell;
  foreach (cell, orig_agg_qual)
  {
    Node *orig_expr = (Node *)lfirst(cell);
    Node *projected_expr = rewrite_projection_mutator(orig_expr, context);
    projected_qual = lappend(projected_qual, projected_expr);
  }

  return projected_qual;
}

Plan *make_bucket_scan(Plan *left_tree)
{
  if (!IsA(left_tree, Agg))
    FAILWITH("Outer plan of BucketScan needs to be an aggregation node.");

  Agg *agg = (Agg *)left_tree;

  /* Make plan node. */
  BucketScan *bucket_scan = (BucketScan *)newNode(sizeof(BucketScan), T_CustomScan);
  bucket_scan->custom_scan.methods = &BucketScanScanMethods;
  Plan *plan = &bucket_scan->custom_scan.scan.plan;

  /*
   * Estimate cost.
   * Buckets are iterated twice: once for hooks, then for finalizing aggregates.
   */
  Cost hooks_cost = 2 * left_tree->plan_rows * DEFAULT_CPU_TUPLE_COST;
  Cost finalization_cost = left_tree->plan_rows * DEFAULT_CPU_TUPLE_COST;
  Cost startup_cost = left_tree->total_cost + hooks_cost;
  plan->startup_cost = startup_cost;
  plan->total_cost = startup_cost + finalization_cost;
  plan->plan_rows = left_tree->plan_rows;
  plan->plan_width = left_tree->plan_width;

  /* Lift projection and qual up. */
  List *flat_agg_tlist = flatten_agg_tlist(agg);
  RewriteProjectionContext context = {flat_agg_tlist, agg->numCols};
  plan->targetlist = project_agg_tlist(agg->plan.targetlist, &context);
  plan->qual = project_agg_qual(agg->plan.qual, &context);
  outerPlan(plan) = left_tree;
  agg->plan.targetlist = flat_agg_tlist;
  agg->plan.qual = NIL;

  return (Plan *)bucket_scan;
}
