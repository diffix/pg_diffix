#ifndef PG_DIFFIX_NODE_FUNCS_H
#define PG_DIFFIX_NODE_FUNCS_H

#include "nodes/params.h"
#include "nodes/primnodes.h"

/*
 * Returns `true` if the node represents a constant from pg_diffix perspective, i.e. `1` or `$1`.
 */
extern bool is_simple_constant(Node *node);

extern void get_simple_constant_typed_value(Node *node, ParamListInfo bound_params, Oid *type, Datum *value, bool *isnull);

#endif /* PG_DIFFIX_NODE_FUNCS_H */
