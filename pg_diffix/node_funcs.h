#ifndef PG_DIFFIX_NODE_FUNCS_H
#define PG_DIFFIX_NODE_FUNCS_H

#include "nodes/params.h"
#include "nodes/primnodes.h"

extern bool is_datetime_to_string_cast(CoerceViaIO *expr);

/*
 * Returns the first non-cast node.
 */
extern Node *unwrap_cast(Node *node);

extern int32 unwrap_const_int32(Expr *expr, int32 min_value, int32 max_value);

extern int64 unwrap_const_int64(Expr *expr, int64 min_value, int64 max_value);

extern Expr *make_const_int32(int32 value);

extern Expr *make_const_int64(int64 value);

/*
 * Returns `true` if the node represents a constant from pg_diffix perspective, i.e. `1` or `$1`.
 */
extern bool is_stable_expression(Node *node);

/*
 * Fills `type`, `value` and `isnull` with what the stable expression `node` holds.
 * `bound_params` must be provided, since `node` might be a `Param` node.
 */
extern void get_stable_expression_value(Node *node, ParamListInfo bound_params, Oid *type, Datum *value, bool *isnull);

#endif /* PG_DIFFIX_NODE_FUNCS_H */
