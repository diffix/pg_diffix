#ifndef PG_DIFFIX_ALLOWED_FUNCTIONS_H
#define PG_DIFFIX_ALLOWED_FUNCTIONS_H

#include "lib/integerset.h"

/*
 * Returns whether the OID points to a function (or operator) allowed in defining buckets
 */
extern bool is_allowed_function(Oid funcoid);

/*
 * Populates the allowed set. Does nothing if already loaded.
 * We don't call this at activation time because the UDFs may not be registered yet.
 */
extern void allowed_functions_init(void);

/*
 * Frees the allowed set for a clean plugin reload.
 */
extern void allowed_functions_cleanup(void);

#endif /* PG_DIFFIX_ALLOWED_FUNCTIONS_H */
