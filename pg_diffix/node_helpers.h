/*
 * Various helper functions for processing query trees.
 */

#ifndef PG_DIFFIX_NODE_HELPERS_H
#define PG_DIFFIX_NODE_HELPERS_H

#include "nodes/parsenodes.h"

/*
 * Returns a list of sensitive relations in range of the query and optionally subqueries.
 */
extern List *gather_sensitive_relations(Query *query, bool include_subqueries);

#endif /* PG_DIFFIX_NODE_HELPERS_H */
