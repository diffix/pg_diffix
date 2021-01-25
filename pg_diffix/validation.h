#ifndef PG_DIFFIX_VALIDATION_H
#define PG_DIFFIX_VALIDATION_H

#include "nodes/parsenodes.h"

/*
 * Returns true if query range contains any sensitive relation.
 * See config.h for relation configuration.
 */
extern bool requires_anonymization(Query *query);

/*
 * Verifies that a query matches current anonymization restrictions and limitations.
 * If requirements are not met, an error is reported and execution is halted.
 */
extern void verify_anonymization_requirements(Query *query);

#endif /* PG_DIFFIX_VALIDATION_H */
