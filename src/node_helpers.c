#include "postgres.h"
#include "nodes/nodeFuncs.h"

#include "pg_diffix/config.h"
#include "pg_diffix/node_helpers.h"

typedef struct GatherSensitiveRelationsContext
{
  List *relations;
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

  return context.relations;
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
    RelationConfig *relation = get_relation_config(rte->relid);
    if (relation != NULL)
    {
      context->relations = list_append_unique_ptr(context->relations, relation);
    }
  }

  return false;
}
