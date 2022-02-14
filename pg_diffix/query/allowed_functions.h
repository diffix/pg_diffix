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

#endif /* PG_DIFFIX_ALLOWED_FUNCTIONS_H */
