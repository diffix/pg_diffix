#include "postgres.h"
#include "nodes/nodeFuncs.h"

#include "pg_diffix/config.h"
#include "pg_diffix/node_helpers.h"

bool is_sensitive_query(Query *query)
{
  List *sensitive_relations = gather_sensitive_relations(query, true);
  if (sensitive_relations != NIL)
  {
    list_free(sensitive_relations);
    return true;
  }
  else
  {
    /* No sensitive relations. */
    return false;
  }
}

typedef struct GatherSensitiveRelationsContext
{
  List *sensitive_relations;
  int flags;
} GatherSensitiveRelationsContext;

static bool gather_sensitive_relations_walker(Node *node, GatherSensitiveRelationsContext *context);

List *gather_sensitive_relations(Query *query, bool include_subqueries)
{
  GatherSensitiveRelationsContext context = {
      .relations = NIL,
      .flags = include_subqueries
                   ? QTW_EXAMINE_RTES_BEFORE
                   : QTW_EXAMINE_RTES_BEFORE | QTW_IGNORE_RT_SUBQUERIES};

  gather_sensitive_relations_walker((Node *)query, &context);

  return context.sensitive_relations;
}

/*
 * Walks query RTEs and gathers sensitive relations in context.
 */
static bool gather_sensitive_relations_walker(Node *node, GatherSensitiveRelationsContext *context)
{
  if (node == NULL)
  {
    return false;
  }

  if (IsA(node, Query))
  {
    Query *query = (Query *)node;
    return range_table_walker(
        query->rtable,
        gather_sensitive_relations_walker,
        (void *)context,
        context->flags);
  }
  else if (IsA(node, RangeTblEntry))
  {
    RangeTblEntry *rte = (RangeTblEntry *)node;
    SensitiveRelationConfig *relation = get_relation_config(rte->relid);
    if (relation != NULL)
    {
      context->sensitive_relations = list_append_unique_ptr(context->sensitive_relations, relation);
    }
  }

  return false;
}
