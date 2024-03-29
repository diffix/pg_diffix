#include "postgres.h"

#include "executor/tuptable.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/tlist.h"
#include "utils/datum.h"

#include "pg_diffix/aggregation/bucket_scan.h"
#include "pg_diffix/aggregation/common.h"
#include "pg_diffix/aggregation/led.h"
#include "pg_diffix/aggregation/star_bucket.h"
#include "pg_diffix/config.h"
#include "pg_diffix/oid_cache.h"
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
 *   minimize aggregates in tlist. During execution, anonymizing aggregators will reuse state
 *   if conditions in `can_share_agg_state` are met. Arguments to aggregates are untouched
 *   because they do not leave the node.
 *
 *   Projection/filtering:
 *
 *   Because we need to consider aggregate merging, anonymizing aggregator states are left
 *   unfinalized until after cross-bucket processing completes. Once we're ready to emit tuples,
 *   we move labels and finalized aggregates to the scan slot. Expressions of the Agg node are
 *   moved to the BucketScan and label/aggregate references are rewritten to INDEX_VARs.
 *
 *-------------------------------------------------------------------------
 */

typedef struct BucketScanData
{
  ExtensibleNode extensible;
  AnonymizationContext anon_context; /* Anonymization config for plan node */
  int num_labels;                    /* Number of grouping labels */
  int num_aggs;                      /* Number of aggregates in child Agg */
  int low_count_index;               /* Index of low count aggregate */
  int count_star_index;              /* Index of anonymizing count(*) aggregate */
} BucketScanData;

#define BUCKET_SCAN_DATA_NAME CppAsString(BucketScanData)

/* Plan node */
typedef CustomScan BucketScan;

static inline BucketScanData *get_plan_data(BucketScan *plan)
{
  Assert(list_length(plan->custom_private) == 1);
  return linitial(plan->custom_private);
}

/* Executor node */
typedef struct BucketScanState
{
  CustomScanState css;
  MemoryContext bucket_context;  /* Buckets and aggregates are allocated in this context */
  BucketDescriptor *bucket_desc; /* Bucket metadata */
  List *buckets;                 /* List of buckets gathered from child plan */
  int64 repeat_previous_bucket;  /* If greater than zero, previous bucket will be emitted again */
  int next_bucket_index;         /* Next bucket to emit, starting from 0 if there is a star bucket, from 1 otherwise */
  bool input_done;               /* Is the list of buckets populated? */
} BucketScanState;

static inline bool has_star_bucket(BucketScanState *bucket_state)
{
  Assert(bucket_state->buckets != NIL);
  return linitial(bucket_state->buckets) != NULL;
}

/* State of currently executing bucket scan. */
static BucketScanState *g_current_bucket_scan = NULL;

MemoryContext get_current_bucket_context(void);
bool aggref_shares_state(Aggref *aggref);

/* Used by common.c to locate the bucket memory context. */
MemoryContext get_current_bucket_context(void)
{
  return g_current_bucket_scan != NULL
             ? g_current_bucket_scan->bucket_context
             : NULL;
}

/* Used by common.c to check if an agg has redirected state. */
bool aggref_shares_state(Aggref *aggref)
{
  if (g_current_bucket_scan == NULL)
    return false;

  BucketDescriptor *bucket_desc = g_current_bucket_scan->bucket_desc;
  int num_atts = bucket_num_atts(bucket_desc);
  for (int i = bucket_desc->num_labels; i < num_atts; i++)
  {
    BucketAttribute *att = &bucket_desc->attrs[i];
    /* We use reference comparison to pinpoint exact position of aggregate. */
    if (att->agg.aggref == aggref)
      return i != att->agg.redirect_to;
  }

  /* This should not happen, but we can't guarantee that the Aggref was not copied somewhere. */
  return false;
}

/*-------------------------------------------------------------------------
 * CustomExecMethods
 *-------------------------------------------------------------------------
 */

/*
 * Returns true if aggregates can share the same agg state.
 * This is possible when args, initial state, transition, and merge functions are identical.
 */
static bool can_share_agg_state(BucketAttribute *agg1, BucketAttribute *agg2)
{
  const AnonAggFuncs *funcs1 = agg1->agg.funcs;
  const AnonAggFuncs *funcs2 = agg2->agg.funcs;

  return funcs1->create_state == funcs2->create_state &&
         funcs1->transition == funcs2->transition &&
         funcs1->merge == funcs2->merge &&
         equal(agg1->agg.aggref->args, agg2->agg.aggref->args);
}

/*
 * Populates `bucket_desc` field with type metadata.
 */
static void init_bucket_descriptor(BucketScanState *bucket_state)
{
  BucketScan *plan = (BucketScan *)bucket_state->css.ss.ps.plan;
  BucketScanData *plan_data = get_plan_data(plan);

  int num_atts = plan_data->num_labels + plan_data->num_aggs;

  BucketDescriptor *bucket_desc = palloc0(sizeof(BucketDescriptor) + num_atts * sizeof(BucketAttribute));
  bucket_desc->bucket_context = bucket_state->bucket_context;
  bucket_desc->anon_context = &plan_data->anon_context;
  bucket_desc->low_count_index = plan_data->low_count_index;
  bucket_desc->num_labels = plan_data->num_labels;
  bucket_desc->num_aggs = plan_data->num_aggs;

  List *outer_tlist = outerPlan(plan)->targetlist;
  TupleDesc outer_tupdesc = outerPlanState(bucket_state)->ps_ResultTupleDesc;

  for (int i = 0; i < num_atts; i++)
  {
    TargetEntry *tle = list_nth_node(TargetEntry, outer_tlist, i);

    BucketAttribute *att = &bucket_desc->attrs[i];
    att->typ_len = outer_tupdesc->attrs[i].attlen;
    att->typ_byval = outer_tupdesc->attrs[i].attbyval;
    att->resname = tle->resname;

    const AnonAggFuncs *agg_funcs = NULL;
    if (i >= plan_data->num_labels)
    {
      Aggref *aggref = castNode(Aggref, tle->expr);
      agg_funcs = find_agg_funcs(aggref->aggfnoid);
      att->agg.aggref = aggref;
      att->agg.funcs = agg_funcs;
      att->agg.args_desc = build_args_desc(aggref);
      att->agg.redirect_to = i; /* Pointing to itself means state is not shared. */
      att->tag = agg_funcs != NULL ? BUCKET_ANON_AGG : BUCKET_REGULAR_AGG;
    }

    if (agg_funcs != NULL)
    {
      /* For anonymizing aggregators we describe finalized type. */
      agg_funcs->final_type(att->agg.args_desc, &att->final_type, &att->final_typmod, &att->final_collid);

      /* Look back to check if there is a compatible agg which we can share state with. */
      for (int j = plan_data->num_labels; j < i; j++)
      {
        BucketAttribute *other_att = &bucket_desc->attrs[j];
        if (other_att->agg.funcs == NULL)
          continue;

        if (can_share_agg_state(att, other_att))
        {
          Assert(i != plan_data->low_count_index); /* low_count is always unique. */
          att->agg.redirect_to = j;
          break;
        }
      }
    }
    else
    {
      /* Describe label or regular aggregate. */
      Node *expr = (Node *)tle->expr;
      att->final_type = exprType(expr);
      att->final_typmod = exprTypmod(expr);
      att->final_collid = exprCollation(expr);
    }
  }

  bucket_state->bucket_desc = bucket_desc;
}

static void bucket_begin_scan(CustomScanState *css, EState *estate, int eflags)
{
  BucketScanState *bucket_state = (BucketScanState *)css;
  BucketScan *plan = (BucketScan *)css->ss.ps.plan;

  Assert(outerPlan(plan) != NULL);
  Assert(innerPlan(plan) == NULL);

  if (eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK))
    FAILWITH("Cannot BACKWARD or MARK/RESTORE a BucketScan.");

  bucket_state->bucket_context = AllocSetContextCreate(estate->es_query_cxt, "BucketScan context", ALLOCSET_DEFAULT_SIZES);
  bucket_state->buckets = NIL;
  bucket_state->repeat_previous_bucket = 0;
  bucket_state->next_bucket_index = 1;
  bucket_state->input_done = false;

  /* Initialize child plan. */
  outerPlanState(bucket_state) = ExecInitNode(outerPlan(plan), estate, eflags);

  /* Requires an initialized outerPlanState. */
  init_bucket_descriptor(bucket_state);
  css->ss.ps.ps_ExprContext->ecxt_scantuple = css->ss.ss_ScanTupleSlot;
}

static void fill_bucket_list(BucketScanState *bucket_state)
{
  BucketScanState *old_bucket_scan = g_current_bucket_scan;

  ExprContext *econtext = bucket_state->css.ss.ps.ps_ExprContext;
  MemoryContext per_tuple_memory = econtext->ecxt_per_tuple_memory;
  PlanState *outer_plan_state = outerPlanState(bucket_state);

  MemoryContext bucket_context = bucket_state->bucket_context;
  BucketDescriptor *bucket_desc = bucket_state->bucket_desc;
  int num_atts = bucket_num_atts(bucket_desc);
  int low_count_index = bucket_desc->low_count_index;

  MemoryContext old_context = MemoryContextSwitchTo(bucket_context);
  List *buckets = list_make1(NULL); /* First item is reserved for star bucket. */
  MemoryContextSwitchTo(old_context);

  for (;;)
  {
    CHECK_FOR_INTERRUPTS();

    g_current_bucket_scan = bucket_state;
    TupleTableSlot *outer_slot = ExecProcNode(outer_plan_state);

    if (TupIsNull(outer_slot))
      break; /* EOF */

    /* Make sure data is safe for copying. */
    ExecMaterializeSlot(outer_slot);
    slot_getallattrs(outer_slot);

    /* Buckets are allocated in longer lived memory. */
    old_context = MemoryContextSwitchTo(bucket_context);
    Bucket *bucket = palloc0(sizeof(Bucket));
    bucket->values = palloc0(num_atts * sizeof(Datum));
    bucket->is_null = palloc0(num_atts * sizeof(bool));

    for (int i = 0; i < num_atts; i++)
    {
      if (outer_slot->tts_isnull[i])
        bucket->is_null[i] = true;
      else
        bucket->values[i] = datumCopy(outer_slot->tts_values[i],
                                      bucket_desc->attrs[i].typ_byval,
                                      bucket_desc->attrs[i].typ_len);
    }

    buckets = lappend(buckets, bucket);

    /*
     * If the aggregate is missing, we consider buckets high-count.
     * This can happen with global aggregation or non-anonymizing queries.
     */
    if (low_count_index != -1)
    {
      /* Switch to tuple memory to evaluate low count. */
      MemoryContextSwitchTo(per_tuple_memory);
      bucket->low_count = eval_low_count(bucket, bucket_desc);
      MemoryContextReset(per_tuple_memory);
    }

    MemoryContextSwitchTo(old_context);
  }

  bucket_state->buckets = buckets;
  bucket_state->input_done = true;

  /* Restore previous bucket scan context. */
  g_current_bucket_scan = old_bucket_scan;
}

static void run_hooks(BucketScanState *bucket_state)
{
  BucketDescriptor *bucket_desc = bucket_state->bucket_desc;
  bool has_low_count_agg = bucket_desc->low_count_index != -1;
  if (!has_low_count_agg)
    return;

  led_hook(bucket_state->buckets, bucket_desc);

  Bucket *star_bucket = NULL;
  if (g_config.compute_suppress_bin)
    star_bucket = star_bucket_hook(bucket_state->buckets, bucket_desc);

  if (star_bucket != NULL)
  {
    list_head(bucket_state->buckets)->ptr_value = star_bucket;
    bucket_state->next_bucket_index = 0; /* Include star bucket in output. */
  }
}

/*
 * Moves bucket data to scan slot.
 * Aggregates are finalized in per tuple memory context.
 */
static void finalize_bucket(Bucket *bucket, BucketDescriptor *bucket_desc, ExprContext *econtext)
{
  MemoryContext old_context = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

  TupleTableSlot *scan_slot = econtext->ecxt_scantuple;
  Datum *values = scan_slot->tts_values;
  bool *is_null = scan_slot->tts_isnull;

  int num_atts = bucket_num_atts(bucket_desc);
  for (int i = 0; i < num_atts; i++)
  {
    BucketAttribute *att = &bucket_desc->attrs[i];
    if (att->tag == BUCKET_ANON_AGG)
    {
      int state_source_index = att->agg.redirect_to; /* If shared, points to some other non-NULL state. */
      AnonAggState *agg_state = (AnonAggState *)DatumGetPointer(bucket->values[state_source_index]);
      Assert(agg_state != NULL);
      is_null[i] = false;
      values[i] = att->agg.funcs->finalize(agg_state, bucket, bucket_desc, &is_null[i]);
    }
    else
    {
      values[i] = bucket->values[i];
      is_null[i] = bucket->is_null[i];
    }
  }

  MemoryContextSwitchTo(old_context);

  /* Mark slot as ready. */
  scan_slot->tts_flags &= ~TTS_FLAG_EMPTY;
  scan_slot->tts_nvalid = num_atts;
}

static int64 scan_slot_get_int64(ExprContext *econtext, int index)
{
  TupleTableSlot *scan_slot = econtext->ecxt_scantuple;
  if (scan_slot->tts_isnull[index])
    return 0;
  else
    return DatumGetInt64(scan_slot->tts_values[index]);
}

static TupleTableSlot *bucket_exec_scan(CustomScanState *css)
{
  BucketScanState *bucket_state = (BucketScanState *)css;

  if (!bucket_state->input_done)
  {
    fill_bucket_list(bucket_state);
    run_hooks(bucket_state);
  }

  /*
   * If we have queries where labels are followed by aggregates in matching order,
   * for example `SELECT city, count(*) FROM customers GROUP BY city`,
   * then CustomScan drops the redundant projection info and we have to return
   * scan tuples directly.
   */
  ProjectionInfo *proj_info = css->ss.ps.ps_ProjInfo;

  /* Expand previously emitted bucket. */
  if (bucket_state->repeat_previous_bucket > 0)
  {
    CHECK_FOR_INTERRUPTS();
    bucket_state->repeat_previous_bucket--;
    return proj_info
               ? bucket_state->css.ss.ps.ps_ResultTupleSlot
               : bucket_state->css.ss.ss_ScanTupleSlot;
  }

  BucketScan *plan = (BucketScan *)bucket_state->css.ss.ps.plan;
  BucketScanData *plan_data = get_plan_data(plan);
  BucketDescriptor *bucket_desc = bucket_state->bucket_desc;
  ExprContext *econtext = css->ss.ps.ps_ExprContext;
  ExprState *qual = css->ss.ps.qual;

  List *buckets = bucket_state->buckets;
  int num_buckets = list_length(buckets);

  for (;;)
  {
    CHECK_FOR_INTERRUPTS();

    if (bucket_state->next_bucket_index >= num_buckets)
      return NULL; /* EOF */

    Bucket *bucket = list_nth(buckets, bucket_state->next_bucket_index++);
    if (bucket->low_count || bucket->merged)
      continue; /* We can skip bucket without further evaluation. */

    ResetExprContext(econtext);
    finalize_bucket(bucket, bucket_desc, econtext);

    /* We do not reset after qual because some values in scan tuple are owned by econtext. */
    if (ExecQual(qual, econtext))
    {
      if (plan_data->anon_context.expand_buckets)
      {
        int64 bucket_repeat_count = scan_slot_get_int64(econtext, plan_data->count_star_index);
        if (bucket_repeat_count > 0)
          /* Repeat bucket for n-1 times after current one. */
          bucket_state->repeat_previous_bucket = bucket_repeat_count - 1;
        else
          /* Zero occurrences, skip bucket. */
          continue;
      }

      return proj_info ? ExecProject(proj_info) : econtext->ecxt_scantuple;
    }
  }
}

static void bucket_end_scan(CustomScanState *css)
{
  BucketScanState *bucket_state = (BucketScanState *)css;

  MemoryContextDelete(bucket_state->bucket_context);
  bucket_state->bucket_context = NULL;

  /* Shut down subplans. */
  ExecEndNode(outerPlanState(css));
}

static void bucket_rescan(CustomScanState *css)
{
  BucketScanState *bucket_state = (BucketScanState *)css;
  PlanState *outer_plan = outerPlanState(css);

  /* Buckets not materialized yet, nothing to do. */
  if (!bucket_state->input_done)
    return;

  if (outer_plan->chgParam != NULL)
  {
    /* We are forced to re-scan input. */
    MemoryContextReset(bucket_state->bucket_context); /* Frees all existing buckets. */
    bucket_state->buckets = NIL;
    bucket_state->next_bucket_index = 1;
    bucket_state->repeat_previous_bucket = 0;
    bucket_state->input_done = false;
    /* Child plan will be re-scanned by first ExecProcNode, so no need to do it here. */
  }
  else
  {
    /* Re-scan existing buckets. */
    bucket_state->next_bucket_index = has_star_bucket(bucket_state) ? 0 : 1;
    bucket_state->repeat_previous_bucket = 0;
  }
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
  BucketScanState *bucket_state = (BucketScanState *)newNode(sizeof(BucketScanState), T_CustomScanState);
  bucket_state->css.methods = &BucketScanExecMethods;
  return (Node *)bucket_state;
}

static const CustomScanMethods BucketScanMethods = {
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
static List *flatten_agg_tlist(Agg *agg, AttrNumber *grouping_cols, int num_labels)
{
  List *child_tlist = outerPlan(agg)->targetlist;
  List *orig_agg_tlist = agg->plan.targetlist;
  List *flat_agg_tlist = NIL;

  /* Add grouping labels to target list. */
  for (int i = 0; i < num_labels; i++)
  {
    TargetEntry *label_tle = list_nth_node(TargetEntry, orig_agg_tlist, grouping_cols[i] - 1);
    Assert(label_tle->resno == grouping_cols[i]);

    if (!IsA(label_tle->expr, Var))
      FAILWITH("Unexpected grouping expression in plan.");

    AttrNumber label_var_attno = ((Var *)label_tle->expr)->varattno;
    TargetEntry *child_target_entry = list_nth_node(TargetEntry, child_tlist, label_var_attno - 1);

    Var *label_var = makeVarFromTargetEntry(OUTER_VAR, child_target_entry);
    Assert(label_var_attno == label_var->varattno);

    TargetEntry *label_target_entry = makeTargetEntry((Expr *)label_var, i + 1, NULL, false);
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
    Aggref *aggref = list_nth_node(Aggref, aggrefs, i);
    TargetEntry *agg_target_entry = makeTargetEntry((Expr *)aggref, num_labels + i + 1, NULL, false);
    TargetEntry *orig_target_entry = tlist_member((Expr *)aggref, orig_agg_tlist);

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
 * These expressions are evaluated against the BucketScan's scan
 * slot where aggregates are finalized.
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
      /* Already finalized, only redirect to scan tuple. */
      return (Node *)makeVarFromTargetEntry(INDEX_VAR, agg_tle);
    }

    Oid final_type;
    int32 final_typmod;
    Oid final_collid;

    ArgsDescriptor *args_desc = build_args_desc(aggref);
    agg_funcs->final_type(args_desc, &final_type, &final_typmod, &final_collid);
    pfree(args_desc);

    return (Node *)makeVar(INDEX_VAR, agg_tle->resno, final_type, final_typmod, final_collid, 0);
  }

  if (IsA(node, Var))
  {
    Var *var = (Var *)node;
    TargetEntry *label_tle = find_var_target_entry(context->flat_agg_tlist, var->varattno);
    /* Vars can only point to grouping labels, and they should have been exported by Agg. */
    if (label_tle == NULL)
      FAILWITH("Expression does not point to a grouping label.");

    return (Node *)makeVarFromTargetEntry(INDEX_VAR, label_tle);
  }

  return expression_tree_mutator(node, rewrite_projection_mutator, context);
}

static List *project_agg_tlist(List *orig_agg_tlist, RewriteProjectionContext *context)
{
  List *projected_tlist = NIL;

  ListCell *cell;
  foreach (cell, orig_agg_tlist)
  {
    TargetEntry *orig_tle = lfirst_node(TargetEntry, cell);

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

static int find_agg_index(List *tlist, Oid fnoid)
{
  ListCell *cell;
  foreach (cell, tlist)
  {
    TargetEntry *tle = lfirst_node(TargetEntry, cell);
    Expr *expr = tle->expr;
    if (IsA(expr, Aggref) && ((Aggref *)expr)->aggfnoid == fnoid)
      return foreach_current_index(cell);
  }

  return -1;
}

/*
 * Builds a tlist describing the scan slot. Attributes match with the child tlist,
 * except for anonymized aggregates, which are finalized at this stage.
 * Anonymized aggregates of Agg are updated to have AnonAggState return type.
 */
static List *make_scan_tlist(List *flat_agg_tlist, int num_labels, int num_aggs)
{
  int num_atts = num_labels + num_aggs;
  List *scan_tlist = NIL;

  for (int i = 0; i < num_atts; i++)
  {
    TargetEntry *agg_tle = list_nth_node(TargetEntry, flat_agg_tlist, i);
    Var *var = makeVarFromTargetEntry(OUTER_VAR, agg_tle);

    if (i >= num_labels)
    {
      Aggref *aggref = castNode(Aggref, agg_tle->expr);
      const AnonAggFuncs *agg_funcs = find_agg_funcs(aggref->aggfnoid);
      if (agg_funcs != NULL)
      {
        /* In index slot's entry we store final type. */
        ArgsDescriptor *args_desc = build_args_desc(aggref);
        agg_funcs->final_type(args_desc, &var->vartype, &var->vartypmod, &var->varcollid);
        pfree(args_desc);

        /* In Agg's entry we hold the intermediate AnonAggState. */
        aggref->aggtype = g_oid_cache.anon_agg_state;
        aggref->aggcollid = 0;
      }
    }

    TargetEntry *scan_tle = makeTargetEntry((Expr *)var, i + 1, agg_tle->resname, false);
    scan_tlist = lappend(scan_tlist, scan_tle);
  }

  return scan_tlist;
}

Plan *make_bucket_scan(Plan *left_tree, AnonymizationContext *anon_context)
{
  if (!IsA(left_tree, Agg))
    FAILWITH("Outer plan of BucketScan needs to be an aggregation node.");

  /* Make plan node. */
  BucketScan *bucket_scan = (BucketScan *)newNode(sizeof(BucketScan), T_CustomScan);
  Plan *plan = &bucket_scan->scan.plan;
  bucket_scan->methods = &BucketScanMethods;
  bucket_scan->flags = 0; /* No support for BACKWARD or MARK/RESTORE. */

  /* Attach data to plan. */
  BucketScanData *plan_data = (BucketScanData *)newNode(sizeof(BucketScanData), T_ExtensibleNode);
  bucket_scan->custom_private = list_make1(plan_data);
  plan_data->extensible.extnodename = BUCKET_SCAN_DATA_NAME;
  plan_data->anon_context = *anon_context; /* Copy by value to avoid managing another custom node. */
  int num_labels = plan_data->num_labels = anon_context->grouping_cols_count;

  /* Lift projection and qual up. */
  Agg *agg = (Agg *)left_tree;
  List *flat_agg_tlist = flatten_agg_tlist(agg, anon_context->grouping_cols, anon_context->grouping_cols_count);
  RewriteProjectionContext context = {flat_agg_tlist, num_labels};
  plan->targetlist = project_agg_tlist(agg->plan.targetlist, &context);
  plan->qual = project_agg_qual(agg->plan.qual, &context);
  outerPlan(plan) = left_tree;
  agg->plan.targetlist = flat_agg_tlist;
  agg->plan.qual = NIL;

  /* Rest of data after flattening agg tlist. */
  int num_aggs = plan_data->num_aggs = list_length(flat_agg_tlist) - num_labels;
  plan_data->low_count_index = find_agg_index(flat_agg_tlist, g_oid_cache.low_count);
  plan_data->count_star_index = find_agg_index(flat_agg_tlist, g_oid_cache.anon_count_star);
  bucket_scan->custom_scan_tlist = make_scan_tlist(flat_agg_tlist, num_labels, num_aggs);

  if (anon_context->expand_buckets && plan_data->count_star_index == -1)
    FAILWITH("Cannot expand buckets with no anonymized COUNT(*) in scope.");

  /* Estimate cost. */
  double rows = left_tree->plan_rows;
  Cost gather_cost = rows * cpu_tuple_cost;
  Cost led_cost = 0;
  Cost star_bucket_cost = 0;
  Cost finalization_cost = rows * cpu_tuple_cost;

  if (plan_data->low_count_index != -1)
  {
    if (num_labels > 2)
    {
      Cost led_table_cost = num_labels * rows * cpu_tuple_cost;
      Cost led_loop_cost = rows * cpu_tuple_cost;
      led_cost = led_table_cost + led_loop_cost;
    }

    if (g_config.compute_suppress_bin)
      star_bucket_cost = rows * cpu_tuple_cost;
  }

  plan->startup_cost = left_tree->total_cost + gather_cost + led_cost + star_bucket_cost;
  plan->total_cost = plan->startup_cost + finalization_cost;
  plan->plan_rows = left_tree->plan_rows;
  plan->plan_width = left_tree->plan_width;

  return (Plan *)bucket_scan;
}

bool is_bucket_scan(Plan *plan)
{
  if (IsA(plan, CustomScan))
    return ((CustomScan *)plan)->methods == &BucketScanMethods;
  else
    return false;
}

/*-------------------------------------------------------------------------
 * Custom nodes
 *-------------------------------------------------------------------------
 */

/* Subset of macros from outfuncs.c & copyfuncs.c. */

#define WRITE_INT_FIELD(fldname) \
  appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

#define WRITE_NODE_FIELD(fldname)                              \
  (appendStringInfoString(str, " :" CppAsString(fldname) " "), \
   outNode(str, node->fldname))

#define WRITE_BOOL_FIELD(fldname) \
  appendStringInfo(str, " :" CppAsString(fldname) " %s", booltostr(node->fldname))

#define WRITE_SEED_FIELD(fldname) \
  appendStringInfo(str, " :" CppAsString(fldname) " %" INT64_MODIFIER "x", node->fldname)

#define WRITE_ATTRNUMBER_ARRAY(fldname, len)                    \
  do                                                            \
  {                                                             \
    appendStringInfoString(str, " :" CppAsString(fldname) " "); \
    for (int i = 0; i < len; i++)                               \
      appendStringInfo(str, " %d", node->fldname[i]);           \
  } while (0)

#define booltostr(x) ((x) ? "true" : "false")

#define COPY_SCALAR_FIELD(fldname) \
  (dst->fldname = src->fldname)

#define COPY_NODE_FIELD(fldname) \
  (dst->fldname = copyObjectImpl(src->fldname))

#define COPY_POINTER_FIELD(fldname, sz)          \
  do                                             \
  {                                              \
    Size _size = (sz);                           \
    if (_size > 0)                               \
    {                                            \
      dst->fldname = palloc(_size);              \
      memcpy(dst->fldname, src->fldname, _size); \
    }                                            \
  } while (0)

static void bucket_scan_data_copy(ExtensibleNode *dst_node, const ExtensibleNode *src_node)
{
  BucketScanData *dst = (BucketScanData *)dst_node;
  const BucketScanData *src = (const BucketScanData *)src_node;

  COPY_SCALAR_FIELD(num_labels);
  COPY_SCALAR_FIELD(num_aggs);
  COPY_SCALAR_FIELD(low_count_index);
  COPY_SCALAR_FIELD(count_star_index);

  int grouping_cols_size = sizeof(src->anon_context.grouping_cols[0]) * src->anon_context.grouping_cols_count;
  COPY_POINTER_FIELD(anon_context.grouping_cols, grouping_cols_size);
  COPY_SCALAR_FIELD(anon_context.grouping_cols_count);
  COPY_SCALAR_FIELD(anon_context.sql_seed);
  COPY_SCALAR_FIELD(anon_context.expand_buckets);
}

static bool bucket_scan_data_equal(const ExtensibleNode *a, const ExtensibleNode *b)
{
  FAILWITH("Node function not supported.");
  return false;
}

static void bucket_scan_data_out(struct StringInfoData *str, const ExtensibleNode *raw_node)
{
  BucketScanData *node = (BucketScanData *)raw_node;

  WRITE_INT_FIELD(num_labels);
  WRITE_INT_FIELD(num_aggs);
  WRITE_INT_FIELD(low_count_index);
  WRITE_INT_FIELD(count_star_index);

  WRITE_SEED_FIELD(anon_context.sql_seed);
  WRITE_ATTRNUMBER_ARRAY(anon_context.grouping_cols, node->anon_context.grouping_cols_count);
  WRITE_BOOL_FIELD(anon_context.expand_buckets);
}

static void bucket_scan_data_read(ExtensibleNode *node)
{
  FAILWITH("Node function not supported.");
}

static const ExtensibleNodeMethods g_bucket_scan_data_methods = {
    .extnodename = BUCKET_SCAN_DATA_NAME,
    .node_size = sizeof(BucketScanData),
    .nodeCopy = bucket_scan_data_copy,
    .nodeEqual = bucket_scan_data_equal,
    .nodeOut = bucket_scan_data_out,
    .nodeRead = bucket_scan_data_read,
};

void register_bucket_scan_nodes(void)
{
  RegisterExtensibleNodeMethods(&g_bucket_scan_data_methods);
}
