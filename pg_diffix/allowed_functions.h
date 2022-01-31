#ifndef PG_DIFFIX_ALLOWED_FUNCTIONS_H
#define PG_DIFFIX_ALLOWED_FUNCTIONS_H

#include "postgres.h"
#include "lib/integerset.h"

/*
 * Global instance of a set of OIDs which are allowed.
 */
extern IntegerSet *g_allowed_functions;

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
