#ifndef PG_DIFFIX_VALIDATION_H
#define PG_DIFFIX_VALIDATION_H

#include "nodes/parsenodes.h"

/*
 * Verifies that a query matches current anonymization restrictions and limitations.
 * If requirements are not met, an error is reported and execution is halted.
 *
 * Some part of verification is up to `verify_rewritten_query`.
 */
extern void verify_anonymization_requirements(Query *query);

/*
 * Similar to `verify_anonymization_requirements`, operates on a rewritten query.
 */
extern void verify_rewritten_query(Query *query);

#endif /* PG_DIFFIX_VALIDATION_H */
