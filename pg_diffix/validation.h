#ifndef PG_DIFFIX_VALIDATION_H
#define PG_DIFFIX_VALIDATION_H

#include "nodes/parsenodes.h"

extern bool requires_anonymization(Query *query);

extern void verify_anonymization_requirements(Query *query);

#endif /* PG_DIFFIX_VALIDATION_H */
