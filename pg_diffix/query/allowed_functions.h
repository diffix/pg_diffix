#ifndef PG_DIFFIX_ALLOWED_FUNCTIONS_H
#define PG_DIFFIX_ALLOWED_FUNCTIONS_H

#include "lib/integerset.h"

/*
 * Returns whether the OID points to a function (or operator) allowed in defining buckets
 */
extern bool is_allowed_function(Oid funcoid);

#endif /* PG_DIFFIX_ALLOWED_FUNCTIONS_H */
