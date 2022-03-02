#ifndef PG_DIFFIX_ALLOWED_FUNCTIONS_H
#define PG_DIFFIX_ALLOWED_FUNCTIONS_H

/*
 * Returns whether the OID points to a function (or operator) allowed in defining buckets.
 */
extern bool is_allowed_function(Oid funcoid);

/*
 * Returns whether the OID points to a cast allowed in defining buckets.
 */
extern bool is_allowed_cast(Oid funcoid);

/*
 * Returns whether the OID points to a UDF being a rounding function, e.g. `ceil_by(x, 2.0)`.
 */
extern bool is_rounding_udf(Oid funcoid);

/*
 * Returns whether the OID points to a built-in substring function.
 */
extern bool is_substring_builtin(Oid funcoid);

/*
 * Returns whether the OID points to a built-in rounding function, e.g. `ceil(x)`.
 */
extern bool is_rounding_builtin(Oid funcoid);

#endif /* PG_DIFFIX_ALLOWED_FUNCTIONS_H */
