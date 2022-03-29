#include "postgres.h"

#include "nodes/makefuncs.h"

#include "pg_diffix/query/node_functions.h"

static void mutate_plan_list(List *plans, Plan *(*mutator)(), void *context)
{
  ListCell *cell;
  foreach (cell, plans)
  {
    Plan *plan = (Plan *)lfirst(cell);
    plans->elements[foreach_current_index(cell)].ptr_value = mutator(plan, context);
  }
}

static bool walk_plan_list(List *plans, bool (*walker)(), void *context)
{
  ListCell *cell;
  foreach (cell, plans)
  {
    Plan *plan = (Plan *)lfirst(cell);
    if (walker(plan, context))
      return true;
  }
  return false;
}

bool walk_plan(Plan *plan, bool (*walker)(), void *context)
{
  if (plan == NULL)
    return false;

  if (walker(plan->lefttree, context))
    return true;
  if (walker(plan->righttree, context))
    return true;

  switch (plan->type)
  {
  case T_Append:
    if (walk_plan_list(((Append *)plan)->appendplans, walker, context))
      return true;
    break;
  case T_MergeAppend:
    if (walk_plan_list(((MergeAppend *)plan)->mergeplans, walker, context))
      return true;
    break;
  case T_SubqueryScan:
    if (walker(((SubqueryScan *)plan)->subplan, context))
      return true;
    break;
  case T_CustomScan:
    if (walk_plan_list(((CustomScan *)plan)->custom_plans, walker, context))
      return true;
    break;
  default:
    /* Nothing to do. */
    break;
  }

  return false;
}

Plan *mutate_plan(Plan *plan, Plan *(*mutator)(), void *context)
{
  if (plan == NULL)
    return NULL;

  plan->lefttree = mutator(plan->lefttree, context);
  plan->righttree = mutator(plan->righttree, context);

  switch (plan->type)
  {
  case T_Append:
    mutate_plan_list(((Append *)plan)->appendplans, mutator, context);
    break;
  case T_MergeAppend:
    mutate_plan_list(((MergeAppend *)plan)->mergeplans, mutator, context);
    break;
  case T_SubqueryScan:
    ((SubqueryScan *)plan)->subplan = mutator(((SubqueryScan *)plan)->subplan, context);
    break;
  case T_CustomScan:
    mutate_plan_list(((CustomScan *)plan)->custom_plans, mutator, context);
    break;
  default:
    /* Nothing to do. */
    break;
  }

  return plan;
}