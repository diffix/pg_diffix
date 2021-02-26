#include "postgres.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_oper.h"
#include "catalog/pg_type.h"

#include "pg_diffix/config.h"
#include "pg_diffix/node_helpers.h"
#include "pg_diffix/oid_cache.h"
#include "pg_diffix/rewrite.h"
#include "pg_diffix/utils.h"

#define FAILWITH(...) ereport(ERROR, (errmsg("[PG_DIFFIX] " __VA_ARGS__)))

typedef struct MutatorContext
{
  RelationConfig *relation_config; /* Current relation in query */
} MutatorContext;

/* Mutators */
static void add_implicit_grouping(Query *query);

/* Utils */
static MutatorContext get_mutator_context(Query *query);
static RelationConfig *single_relation_config(Query *query);

void rewrite_query(Query *query)
{
  add_implicit_grouping(query);
}

static void add_implicit_grouping(Query *query)
{
  /* Only simple select queries require implicit grouping. */
  if (query->hasAggs || query->groupClause == NIL)
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
