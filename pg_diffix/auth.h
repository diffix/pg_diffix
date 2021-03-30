#ifndef PG_DIFFIX_AUTH_H
#define PG_DIFFIX_AUTH_H

#include "config.h"

#include "access/attnum.h"

extern void auth_init(void);

/*
 * Returns access level for current session user.
 */
extern AccessLevel get_access_level(void);

/*
 * Returns true if the relation has been labeled as sensitive, false otherwise.
 * If the relation is unlabeled, the schema label is checked.
 * If the schema is unlabeled, the database label is checked.
 */
extern bool is_sensitive_relation(Oid relation_oid, Oid namespace_oid);

/*
 * Returns true if the column has been labeled as an AID, false otherwise.
 */
extern bool is_aid_column(Oid relation_oid, AttrNumber attnum);

#endif /* PG_DIFFIX_AUTH_H */
