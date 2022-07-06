#ifndef PG_DIFFIX_NODE_FUNCS_H
#define PG_DIFFIX_NODE_FUNCS_H

#include "nodes/primnodes.h"

extern int64 unwrap_const_int64(Expr *expr, int64 min_value, int64 max_value);

extern Expr *make_const_int64(int64 value);

#endif /* PG_DIFFIX_NODE_FUNCS_H */
