#ifndef PG_DIFFIX_REWRITE_H
#define PG_DIFFIX_REWRITE_H

#include "pg_diffix/query/context.h"

/*
 * Rewrites a regular query to an anonymizing query.
 */
void rewrite_query(QueryContext *context);

#endif /* PG_DIFFIX_REWRITE_H */
