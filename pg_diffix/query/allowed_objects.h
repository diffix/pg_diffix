#ifndef PG_DIFFIX_ALLOWED_OBJECTS_H
#define PG_DIFFIX_ALLOWED_OBJECTS_H

#include "nodes/bitmapset.h"

/*
 * Returns whether the OID points to a function (or operator) allowed in defining buckets.
 */
extern bool is_allowed_function(Oid funcoid);

/*
 * Returns whether the OID points to a cast allowed in defining buckets.
 */
extern bool is_allowed_cast(Oid funcoid);

/*
 * Returns whether the OID points to a UDF being a implicit_range function, e.g. `ceil_by(x, 2.0)`,
 * allowed when called in `anonymized_untrusted` access level.
 */
extern bool is_implicit_range_udf_untrusted(Oid funcoid);

/*
 * Returns whether the OID points to a built-in substring function.
 */
extern bool is_substring_builtin(Oid funcoid);

/*
 * Returns whether the OID points to a built-in implicit_range function, e.g. `ceil(x)`,
 * allowed when called in `anonymized_untrusted` access level.
 */
extern bool is_implicit_range_builtin_untrusted(Oid funcoid);

/*
 * Returns whether selecting `selected_cols` from a relation in the `pg_catalog` is allowed.
 */
extern bool is_allowed_pg_catalog_rte(Oid relation_oid, const Bitmapset *selected_cols);

#endif /* PG_DIFFIX_ALLOWED_OBJECTS_H */
