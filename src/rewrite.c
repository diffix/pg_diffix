#include "postgres.h"
#include "nodes/nodeFuncs.h"

#include "pg_diffix/config.h"
#include "pg_diffix/node_helpers.h"
#include "pg_diffix/oid_cache.h"
#include "pg_diffix/rewrite.h"

#define FAILWITH(...) ereport(ERROR, (errmsg("[PG_DIFFIX] " __VA_ARGS__)))

typedef struct MutatorContext
{
  RelationConfig *relation_config; /* Current relation in query */
} MutatorContext;

/* Mutators */
static Node *aggregate_expression_mutator(Node *node, MutatorContext *context);

/* Utils */
static MutatorContext get_mutator_context(Query *query);
static RelationConfig *single_relation_config(Query *query);

void rewrite_query(Query *query)
{
  MutatorContext context = get_mutator_context(query);
  query_tree_mutator(
      query,
      aggregate_expression_mutator,
      &context,
      QTW_DONT_COPY_QUERY | QTW_EXAMINE_RTES_BEFORE);
}

/*
 * Rewrites standard aggregates to their anonymizing version.
 */
static Node *aggregate_expression_mutator(Node *node, MutatorContext *context)
{
  if (node == NULL)
  {
    return NULL;
  }

  if (IsA(node, Query))
  {
    Query *query = (Query *)node;
    MutatorContext subcontext = get_mutator_context(query);
    return query_tree_mutator(
        query,
        aggregate_expression_mutator,
        &subcontext,
        0);
  }

  if (IsA(node, Aggref))
  {
    /*
     * Copy and visit sub expressions.
     * We basically use this for copying, but we'll need to validate aggregate args in the future.
     */
    Aggref *aggref = (Aggref *)expression_tree_mutator(node, aggregate_expression_mutator, (void *)context);

    /* Todo: insert AID arg, change aggfnoid to diffix_*. */

    return (Node *)aggref;
  }

  return expression_tree_mutator(node, aggregate_expression_mutator, (void *)context);
}

/*
 * Builds a context to be used during query traversal.
 */
static MutatorContext get_mutator_context(Query *query)
{
  MutatorContext context;
  context.relation_config = single_relation_config(query);
  return context;
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
