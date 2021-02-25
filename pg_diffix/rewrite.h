#ifndef PG_DIFFIX_REWRITE_H
#define PG_DIFFIX_REWRITE_H

#include "nodes/parsenodes.h"

/*
 * Rewrites a regular query to an anonymizing query.
 */
void rewrite_query(Query *query);

#endif /* PG_DIFFIX_REWRITE_H */
