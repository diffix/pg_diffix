#ifndef PG_DIFFIX_VALIDATION_H
#define PG_DIFFIX_VALIDATION_H

#include "nodes/parsenodes.h"

/*
 * Verifies that the utility statement is one of the allowed ones for restricted access users.
 * If requirements are not met, an error is reported and execution is halted.
 */
extern void verify_utility_command(Node *utility_stmt);

/*
 * Verifies that a query matches current anonymization restrictions and limitations.
 * If requirements are not met, an error is reported and execution is halted.
 *
 * Some part of verification is up to `verify_anonymizing_query`.
 */
extern void verify_anonymization_requirements(Query *query);

/*
 * Similar to `verify_anonymization_requirements`, operates on an anonymizing query.
 */
extern void verify_anonymizing_query(Query *query);

/*
 * Returns `true` if the given list of `RangeTblEntry` from `ExecutorCheckPerms` does not access `pg_catalog`
 * relations.
 */
extern bool verify_no_pg_catalog_access(List *rangeTabls);

/*
 * Returns `true` if the given type represents a supported numeric type.
 */
extern bool is_supported_numeric_type(Oid type);

/*
 * Returns the numeric value as a `double`.
 */
extern double numeric_value_to_double(Oid type, Datum value);

#endif /* PG_DIFFIX_VALIDATION_H */
